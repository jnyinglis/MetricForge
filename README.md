# MetricForge Semantic Engine

MetricForge is a grain-agnostic semantic metrics engine written in TypeScript. It lets you prototype and validate semantic models entirely in-memory, keeping the focus on metric definitions and grain logic instead of warehouse plumbing.

## Sweet spot & intended use cases

- **Prototype a semantic layer quickly** when you want to express facts, dimensions, and metrics without provisioning database infrastructure.
- **Experiment with grain logic and time-aware transforms** in a deterministic, small-data sandbox before porting definitions to production systems.
- **Teach or debug semantic modeling concepts**—joins, filters, base-fact choice, and metric dependency graphs—using a transparent runtime you can step through.

## How it works

- **Metric functions, not grain-bound measures**: metric definitions are evaluator functions that receive grouped rows and the current group key; grains are provided by the query, not hard-wired into the metric. 
- **Explicit base fact selection with graceful dimension fallback**: the runtime builds a fact-backed frame when metrics declare `baseFact`, and falls back to a single dimension relation when no facts are needed.
- **Rowset transforms for time intelligence**: metrics can swap the rowset they aggregate over (e.g., aligning to last year) while still reporting at the current grain.
- **Attribute-first filtering**: filter helpers normalize expressions and prune them to available attributes so `where` clauses stay composable across facts and dimensions.

## Quick example

Define a minimal semantic model and run a query at the store grain:

```ts
import {
  aggregateMetric,
  runSemanticQuery,
  QuerySpec,
  SemanticModel,
} from "./src/semanticEngine";

const model: SemanticModel = {
  facts: {
    fact_orders: { name: "fact_orders" },
    fact_returns: { name: "fact_returns" },
  },
  dimensions: { dim_store: { name: "dim_store" } },
  attributes: {
    storeId: { name: "storeId", relation: "fact_orders", column: "storeId" },
    storeName: { name: "storeName", relation: "dim_store", column: "storeName" },
    amount: { name: "amount", relation: "fact_orders", column: "amount" },
    refund: { name: "refund", relation: "fact_returns", column: "refund" },
  },
  joins: [
    { fact: "fact_orders", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
    { fact: "fact_returns", dimension: "dim_store", factKey: "storeId", dimensionKey: "id" },
  ],
  metrics: {
    totalSales: aggregateMetric("totalSales", "fact_orders", "amount", "sum"),
    totalRefunds: aggregateMetric("totalRefunds", "fact_returns", "refund", "sum"),
  },
};

const spec: QuerySpec = {
  dimensions: ["storeId", "storeName"],
  metrics: ["totalSales", "totalRefunds"],
};

const rows = runSemanticQuery({ db, model }, spec);
console.log(rows);
```

The engine builds a fact-backed frame for the primary fact, evaluates each metric within the grouped rowsets, and merges results across additional facts or dimension-scoped metrics before applying optional `having` filters.

## Repository layout

| Path | Description |
| --- | --- |
| [`src/semanticEngine.ts`](src/semanticEngine.ts) | Core engine with filter helpers, metric definitions, rowset transforms, and `runSemanticQuery`. |
| [`src/semanticEngineDemo.ts`](src/semanticEngineDemo.ts) | Minimal demo wiring a store dataset, two fact tables, and aggregate metrics. |
| [`src/linq.js`](src/linq.js) / [`src/linq.d.ts`](src/linq.d.ts) | Bundled LINQ runtime plus TypeScript declarations used by the engine and tests. |
| [`src/operators.md`](src/operators.md) | Operator reference for custom LINQ-powered transforms. |
| [`test/semanticEngine.test.ts`](test/semanticEngine.test.ts) | Mocha + Chai coverage for base fact selection, filters, and rowset transforms. |
| [`docs/`](docs) | Design notes, DSL references, and playground guides. |
| [`playground/`](playground) | React + Vite playground for editing schemas, metrics, and queries in the browser. |

## Getting started

1. **Install dependencies**:
   ```bash
   npm install
   ```
2. **Run automated tests** to exercise the engine end-to-end:
   ```bash
   npm test
   ```
3. **Try the CLI demo** to see a multi-fact query at work:
   ```bash
   npx ts-node src/semanticEngineDemo.ts
   ```
4. **Explore the DSL demo** for parsing metric and query definitions from text:
   ```bash
   npx ts-node src/dslDemo.ts
   ```

## Playground overview

The `playground/` package is a React + Vite single-page app that ships Monaco-based editors for schema, metric, and query DSL definitions. It runs entirely in the browser so you can:

- Load JSON datasets, define facts/dimensions/attributes, and wire joins.
- Author metrics and queries with syntax highlighting, autocomplete, and inline parser feedback.
- Execute queries client-side to visualize results, inspect parsed ASTs, and export/import workspaces.

From `playground/`, use `npm install` followed by `npm run dev` to launch the local experience. See [`docs/playground-user-manual.md`](docs/playground-user-manual.md) and [`docs/web-playground.md`](docs/web-playground.md) for full UX and product details.
