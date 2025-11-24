# Semantic DSL Grammar (EBNF/PEG)

This document describes the full grammar accepted by the semantic engine DSL parser (`src/dsl.ts`) using an EBNF/PEG-inspired notation. Whitespace is skipped before every token, and keywords require a non-alphanumeric boundary via the lexer helpers, so spacing is flexible unless otherwise noted.

## Lexical rules

```
Identifier      ::= /[A-Za-z_][A-Za-z0-9_]*/
NumberLiteral   ::= /\d+(?:\.\d+)?/
BindingLiteral  ::= ':' Identifier
Comparator      ::= '>=' | '<=' | '>' | '<' | '==' | '!='
```

## File structure

```
DslFile         ::= { MetricDecl | QueryDecl }
MetricDecl      ::= 'metric' Identifier 'on' Identifier [ 'where' BoolExpr ] '=' Expr
QueryDecl       ::= 'query' Identifier '{' QueryLine+ '}'
```

A DSL file is a sequence of metric declarations and query blocks. Parsing stops when neither construct matches.

### Metric-level filters

Metric declarations may include an inline `where` clause that restricts the fact rows contributing to the metric before any aggregation or downstream composition. The `BoolExpr` grammar used here matches the query-level `where` grammar below and therefore supports numeric literals and binding placeholders.

```
MetricDecl      ::= 'metric' Identifier 'on' Identifier [ 'where' BoolExpr ] '=' Expr
```

## Metric expressions

Operator precedence follows the parser combinators in `dsl.ts`: parentheses > function calls/atoms > multiplication/division > addition/subtraction.

```
Expr            ::= Additive
Additive        ::= Multiplicative (('+' | '-') Multiplicative)*
Multiplicative  ::= Primary (('*' | '/') Primary)*
Primary         ::= '(' Expr ')' | FunctionCall | NumberLiteral | Identifier
```

### Function calls

```
FunctionCall    ::= Identifier '(' ( LastYearArgs | '*' | [ArgList] ) ')'
LastYearArgs    ::= Identifier [ ',' ] 'by' Identifier
ArgList         ::= Expr (',' Expr)*
```

* `last_year(metric by anchor)` is special-cased: the first identifier is treated as a metric reference, followed by an optional comma and a dimension anchor identifier.
* Aggregate helpers (`sum`, `avg`, `min`, `max`, `count`) and other functions accept either `*` or an explicit argument list; validation later enforces the expected argument shapes.
* Bare identifiers in expressions initially parse as attribute references and are rewritten to metric references when they match declared metric names.

## Filters (`where`)

```
BoolExpr        ::= BoolOr
BoolOr          ::= BoolAnd ('or' BoolAnd)*
BoolAnd         ::= BoolTerm ('and' BoolTerm)*
BoolTerm        ::= '(' BoolExpr ')' | FilterExpr
FilterExpr      ::= Identifier Comparator (NumberLiteral | BindingLiteral)
```

`where` clauses combine attribute comparisons with `and`/`or` plus parenthesized grouping. The comparison operators map directly to filter nodes (`gt`, `gte`, `lt`, `lte`, `eq`, or disjunction for `!=`).

## Having clauses

```
HavingExpr      ::= HavingOr
HavingOr        ::= HavingAnd ('or' HavingAnd)*
HavingAnd       ::= HavingTerm ('and' HavingTerm)*
HavingTerm      ::= Identifier Comparator (NumberLiteral | BindingLiteral)
```

`having` evaluates against metric outputs. Each identifier is treated as a metric name, and comparisons support the same operator set as `where`. Logical composition mirrors `where` with `and`/`or` plus optional parentheses.

## Query blocks

```
QueryLine       ::= DimensionsLine | MetricsLine | WhereLine | HavingLine
DimensionsLine  ::= 'dimensions' ':' IdentList
MetricsLine     ::= 'metrics' ':' IdentList
WhereLine       ::= 'where' ':' BoolExpr
HavingLine      ::= 'having' ':' HavingExpr
IdentList       ::= Identifier (',' Identifier)*
```

A `query` block may omit `dimensions` or `metrics` (e.g., metric-less or dimension-less queries are valid), while `where` and `having` remain optional. Lines repeat until the closing `}` is encountered.

## Whitespace and delimiters

All tokens automatically consume leading whitespace through `skipWs`, and tokens such as commas and braces can be separated by arbitrary spaces or newlines. Keywords use boundary-aware regexes (e.g., `dimensions(?![A-Za-z0-9_])`), so identifiers cannot start with a keyword but may contain them internally (e.g., `dimension_total`). Binding placeholders retain their leading colon and are substituted at execution time using the `bindings` map provided to `runQuery`/`runSemanticQuery`.

## End-to-end example (with parameter bindings)

The snippet below demonstrates the full DSL surface area, including metrics, queries, filters, and binding substitution. At runtime, bindings are supplied as `runQuery("sales_by_region", { year: 2025, minMargin: 0.12 })`.

```
metric gross_sales on fact_sales = sum(sales_amount)
metric net_sales   on fact_sales = sum(sales_amount) - sum(discount)
metric promo_sales on fact_sales where discount > :minDiscount = sum(sales_amount)
metric margin_pct  on fact_sales = net_sales / gross_sales

query sales_by_region {
  dimensions: region, product_category
  metrics:    gross_sales, net_sales, promo_sales, margin_pct
  where:      year == :year and region == :region
  having:     margin_pct >= :minMargin
}
```

* Metric declarations show arithmetic expressions and aggregation helpers.
* `promo_sales` illustrates a metric-level filter that applies before aggregation and supports bindings.
* The query block mixes dimensions, multiple metrics, `where` filters, and `having` clauses.
* `:year`, `:region`, `:minDiscount`, and `:minMargin` are binding placeholders resolved from the caller-supplied bindings object.

### API usage matching the DSL example

The same DSL can be executed through the SemanticEngine API. Load the schema and DSL text, fetch the parsed query, and pass the
binding values through `runSemanticQuery`:

```ts
import { runSemanticQuery, SemanticEngine } from "../src/semanticEngine";

// Schema must align with the DSL above (e.g., `fact_sales`, `region`, etc.).
const engine = SemanticEngine.fromSchema(schema, db).useDslFile(dsl);
const spec = engine.getQuery("sales_by_region");

const rows = runSemanticQuery(
  { db, model: engine.getModel() },
  spec,
  {
    bindings: {
      year: 2025,
      region: "west",
      minDiscount: 5,
      minMargin: 0.12,
    },
  }
);

console.log(rows);
```

This flow reuses the parsed DSL definitions, substitutes bindings, and runs the query against the in-memory database.

### Fluent API equivalent

You can create the same metrics and query without the DSL by chaining the SemanticEngine’s fluent helpers. Metrics are built
with the `Expr` constructors and standard helpers like `aggregateMetric`, filters use `f`, and `runQuery` accepts the same
binding shape:

```ts
import {
  aggregateMetric,
  buildMetricFromExpr,
  Expr,
  f,
  SemanticEngine,
} from "../src/semanticEngine";

const engine = SemanticEngine.fromSchema(schema, db)
  .registerMetric(aggregateMetric("gross_sales", "fact_sales", "sales_amount"))
  .registerMetric(
    buildMetricFromExpr({
      name: "net_sales",
      baseFact: "fact_sales",
      expr: Expr.sub(Expr.sum("sales_amount"), Expr.sum("discount")),
    })
  )
  .registerMetric(
    buildMetricFromExpr({
      name: "promo_sales",
      baseFact: "fact_sales",
      expr: Expr.sum("sales_amount"),
    })
  )
  .registerMetric(
    buildMetricFromExpr({
      name: "margin_pct",
      baseFact: "fact_sales",
      expr: Expr.div(Expr.metric("net_sales"), Expr.metric("gross_sales")),
    })
  );

engine.registerQuery("sales_by_region", {
  dimensions: ["region", "product_category"],
  metrics: ["gross_sales", "net_sales", "promo_sales", "margin_pct"],
  where: f.and(f.eq("year", ":year"), f.eq("region", ":region")),
  having: f.gte("margin_pct", ":minMargin"),
});

const rows = engine.runQuery("sales_by_region", {
  year: 2025,
  region: "west",
  minDiscount: 5,
  minMargin: 0.12,
});

console.log(rows);
```

This programmatic setup mirrors the DSL example, including binding substitution and the same dimensions, metrics, filters, and
having clause.
