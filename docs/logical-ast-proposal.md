# Logical AST (IR) Proposal

> **Status**: Draft v2
> **Author**: Claude
> **Date**: 2025-11-23
> **Revision**: v2 - Incorporates architectural feedback

## Overview

This document proposes the addition of a **Logical AST** (also known as an Intermediate Representation or IR) to the MetricForge Semantic Engine. The Logical AST sits between the existing Syntax AST (`MetricExpr`) and the compiled evaluation functions, providing a normalized, type-aware representation of metric computations.

## Motivation

### Current Pipeline

```
DSL Text → Parser → Syntax AST (MetricExpr) → compileMetricExpr() → MetricEval Functions → Execution
```

The current architecture compiles directly from the syntax AST to executable functions. While this works well for simple cases, it has limitations:

1. **No Type Information**: The syntax AST doesn't track data types, making it difficult to catch type errors early
2. **Unresolved References**: Attribute names remain as strings until runtime
3. **No Query Plan**: There's no intermediate representation of the full query structure
4. **Limited Optimization**: No place to apply transformations like filter pushdown
5. **Debugging Difficulty**: Hard to explain what a query will do before running it

### Proposed Pipeline with Logical IR

```
DSL Text → Parser → Syntax AST (MetricExpr)
                         ↓
              ┌──────────┴──────────┐
              │                     │
        LogicalExpr           LogicalPlan
     (scalar expressions)    (query structure)
              │                     │
              └──────────┬──────────┘
                         ↓
                 Optimization Passes (Future)
                         ↓
                 Compiled MetricEval Functions
                         ↓
                      Execution
```

## Design Goals

1. **Semantic Clarity**: Represent *what* to compute, not *how* it was written
2. **Type Safety**: Track data types through all operations
3. **Resolved References**: Map logical names to physical table.column
4. **Clean Layer Separation**: Expressions for values, Plan for rowset operations
5. **Explicit Plan Nodes**: IDs and structure for visualization/EXPLAIN
6. **Extensibility**: Easy to add new node types and optimization passes
7. **Backward Compatibility**: Existing DSL and APIs continue to work

## Architecture: Two Distinct Layers

This proposal defines **two separate IR layers** with clear responsibilities:

| Layer | Responsibility | Contains |
|-------|---------------|----------|
| **LogicalExpr** | Scalar value computation | Constants, attribute refs, metric refs, arithmetic, scalar functions |
| **LogicalPlan** | Rowset operations | Scans, joins, filters, aggregations, windows, transforms |

**Key principle**: LogicalExpr nodes compute values; LogicalPlan nodes transform row sets. Window functions and transforms are rowset operations and belong in the plan layer, not the expression layer.

---

## Part 1: Type System

### Data Types

```typescript
type DataType =
  | { kind: "number"; precision?: "integer" | "decimal" }
  | { kind: "string"; maxLength?: number }
  | { kind: "boolean" }
  | { kind: "date" }
  | { kind: "datetime" }
  | { kind: "null" }
  | { kind: "unknown" };
```

### Type Inference Rules

| Expression | Result Type |
|------------|-------------|
| Numeric literal | `number` |
| String literal | `string` |
| Boolean literal | `boolean` |
| `sum`, `avg`, `min`, `max` | `number` |
| `count` | `number (integer)` |
| Arithmetic (`+`, `-`, `*`, `/`) | `number` |
| Comparison (`>`, `<`, `=`, etc.) | `boolean` |
| Logical (`and`, `or`, `not`) | `boolean` |
| Attribute reference | Inferred from schema |

---

## Part 2: Logical Expression IR

The **LogicalExpr** layer handles **scalar value computation only**. It does not include rowset operations like windows or transforms.

### Node Type Summary

| Syntax AST (Current) | Logical IR (Proposed) | Key Difference |
|---------------------|----------------------|----------------|
| `Literal` | `LogicalConstant` | Adds `dataType` |
| `AttrRef` | `LogicalAttributeRef` | Resolves to physical table.column |
| `MetricRef` | `LogicalMetricRef` | Slim: only name + type (deps in plan) |
| `Call` (aggregates) | `LogicalAggregate` | Typed aggregate with source |
| `Call` (scalars) | `LogicalScalarFunction` | Type-checked scalar functions |
| `BinaryOp` | `LogicalScalarOp` | Adds `resultType` |
| `Window` | **Moved to Plan** | Rowset operation |
| `Transform` | **Moved to Plan** | Rowset operation |
| — | `LogicalComparison` | Boolean: comparison ops |
| — | `LogicalLogicalOp` | Boolean: and/or/not |
| — | `LogicalConditional` | NEW: if/case expressions |
| — | `LogicalCoalesce` | NEW: null handling |
| — | `LogicalInList` | Boolean: IN operator |
| — | `LogicalBetween` | Boolean: BETWEEN operator |
| — | `LogicalIsNull` | Boolean: NULL check |

### Type Definition

```typescript
// ═══════════════════════════════════════════════════════════════════
// LOGICAL EXPRESSION IR - Scalar values only
// ═══════════════════════════════════════════════════════════════════

type LogicalExpr =
  // Value expressions
  | LogicalConstant
  | LogicalAttributeRef
  | LogicalMetricRef
  | LogicalAggregate
  | LogicalScalarOp
  | LogicalScalarFunction
  | LogicalConditional
  | LogicalCoalesce
  // Boolean expressions (predicates are just boolean-typed expressions)
  | LogicalComparison
  | LogicalLogicalOp
  | LogicalInList
  | LogicalBetween
  | LogicalIsNull;

// Type guard for predicates (boolean expressions)
type LogicalPredicate = LogicalExpr & { resultType: { kind: "boolean" } };
```

### Node Definitions

#### LogicalConstant

Replaces `Literal` with type information.

```typescript
interface LogicalConstant {
  kind: "Constant";
  value: number | string | boolean | null;
  dataType: DataType;
}
```

#### LogicalAttributeRef

Replaces `AttrRef` with resolved physical location.

```typescript
interface LogicalAttributeRef {
  kind: "AttributeRef";
  attributeId: string;         // Semantic model attribute ID
  logicalName: string;         // Original name from DSL
  physicalTable: string;       // Resolved table name
  physicalColumn: string;      // Resolved column name
  dataType: DataType;
  sourceKind: "fact" | "dimension";
}
```

**Example transformation:**
```
Syntax:   { kind: "AttrRef", name: "storeName" }

Logical:  {
  kind: "AttributeRef",
  attributeId: "storeName",
  logicalName: "storeName",
  physicalTable: "dim_store",
  physicalColumn: "name",
  dataType: { kind: "string" },
  sourceKind: "dimension"
}
```

#### LogicalMetricRef

Replaces `MetricRef`. **Kept slim** - dependency/cost info belongs in the plan layer.

```typescript
interface LogicalMetricRef {
  kind: "MetricRef";
  metricName: string;
  baseFact: string | null;
  resultType: DataType;
}
```

> **Design note**: Dependencies, required attributes, and evaluation cost are properties of the metric *within a query plan*, not of individual reference nodes. This avoids duplication since `LogicalMetricPlan` already tracks this information.

**Example transformation:**
```
Syntax:   { kind: "MetricRef", name: "total_sales" }

Logical:  {
  kind: "MetricRef",
  metricName: "total_sales",
  baseFact: "fact_orders",
  resultType: { kind: "number" }
}
```

#### LogicalAggregate

Replaces `Call` nodes for aggregate functions.

```typescript
interface LogicalAggregate {
  kind: "Aggregate";
  op: "sum" | "avg" | "count" | "min" | "max" | "count_distinct";
  input: LogicalExpr;          // What to aggregate (usually an AttributeRef)
  distinct: boolean;
  filter?: LogicalExpr;        // Optional aggregate filter (WHERE in aggregate)
  sourceTable: string;         // Which fact table
  resultType: DataType;
}
```

#### LogicalScalarOp

Replaces `BinaryOp` with result type information.

```typescript
interface LogicalScalarOp {
  kind: "ScalarOp";
  op: "+" | "-" | "*" | "/" | "%" | "^";
  left: LogicalExpr;
  right: LogicalExpr;
  resultType: DataType;
}
```

#### LogicalScalarFunction

For non-aggregate function calls.

```typescript
interface LogicalScalarFunction {
  kind: "ScalarFunction";
  fn: string;
  args: LogicalExpr[];
  resultType: DataType;
}
```

#### LogicalConditional

Enables conditional logic in metrics.

```typescript
interface LogicalConditional {
  kind: "Conditional";
  condition: LogicalExpr;      // Must be boolean-typed
  thenExpr: LogicalExpr;
  elseExpr: LogicalExpr;
  resultType: DataType;
}
```

#### LogicalCoalesce

Handles null values explicitly.

```typescript
interface LogicalCoalesce {
  kind: "Coalesce";
  exprs: LogicalExpr[];
  resultType: DataType;
}
```

### Boolean Expression Nodes (Predicates)

Predicates are simply **boolean-typed LogicalExpr nodes**. This unifies the type system rather than maintaining a parallel hierarchy.

#### LogicalComparison

```typescript
interface LogicalComparison {
  kind: "Comparison";
  left: LogicalExpr;
  op: "=" | "!=" | "<" | "<=" | ">" | ">=";
  right: LogicalExpr;
  resultType: { kind: "boolean" };
}
```

#### LogicalLogicalOp

```typescript
interface LogicalLogicalOp {
  kind: "LogicalOp";
  op: "and" | "or" | "not";
  operands: LogicalExpr[];     // 2 for and/or, 1 for not
  resultType: { kind: "boolean" };
}
```

#### LogicalInList

```typescript
interface LogicalInList {
  kind: "InList";
  expr: LogicalExpr;
  values: LogicalConstant[];
  negated: boolean;
  resultType: { kind: "boolean" };
}
```

#### LogicalBetween

```typescript
interface LogicalBetween {
  kind: "Between";
  expr: LogicalExpr;
  low: LogicalExpr;
  high: LogicalExpr;
  resultType: { kind: "boolean" };
}
```

#### LogicalIsNull

```typescript
interface LogicalIsNull {
  kind: "IsNull";
  expr: LogicalExpr;
  negated: boolean;            // IS NULL vs IS NOT NULL
  resultType: { kind: "boolean" };
}
```

---

## Part 3: Logical Plan IR

The **LogicalPlan** layer represents **rowset operations** as a DAG of plan nodes. Each node has an ID for visualization and tracing.

### Plan Node Base Type

```typescript
type PlanNodeId = string;

interface BasePlanNode {
  id: PlanNodeId;
  annotations?: Record<string, unknown>;  // For cost estimates, row counts, etc.
}
```

### Plan Node Types

```typescript
type LogicalPlanNode =
  | FactScanNode
  | DimensionScanNode
  | JoinNode
  | FilterNode
  | AggregateNode
  | WindowNode
  | TransformNode
  | ProjectNode;
```

#### FactScanNode

Scans a fact table.

```typescript
interface FactScanNode extends BasePlanNode {
  kind: "FactScan";
  tableName: string;
  requiredColumns: LogicalAttributeRef[];
  inlineFilters: LogicalExpr[];  // Filters pushable to scan
}
```

#### DimensionScanNode

Scans a dimension table.

```typescript
interface DimensionScanNode extends BasePlanNode {
  kind: "DimensionScan";
  tableName: string;
  requiredColumns: LogicalAttributeRef[];
  inlineFilters: LogicalExpr[];
}
```

#### JoinNode

Joins two plan nodes.

```typescript
interface JoinNode extends BasePlanNode {
  kind: "Join";
  joinType: "inner" | "left" | "right" | "full";
  leftInputId: PlanNodeId;
  rightInputId: PlanNodeId;
  joinKeys: Array<{
    leftAttr: LogicalAttributeRef;
    rightAttr: LogicalAttributeRef;
  }>;
  cardinality: "1:1" | "1:N" | "N:1" | "N:M";
}
```

> **Design note**: `joinKeys` is an array to support composite keys. Using `PlanNodeId` instead of table names allows for self-joins and role-playing dimensions.

#### FilterNode

Applies a filter predicate.

```typescript
interface FilterNode extends BasePlanNode {
  kind: "Filter";
  inputId: PlanNodeId;
  predicate: LogicalExpr;      // Boolean-typed expression
}
```

#### AggregateNode

Groups and aggregates rows.

```typescript
interface AggregateNode extends BasePlanNode {
  kind: "Aggregate";
  inputId: PlanNodeId;
  groupBy: LogicalAttributeRef[];
  aggregates: Array<{
    outputName: string;
    expr: LogicalAggregate;
  }>;
}
```

#### WindowNode

Applies window functions. **This is where windows live** - in the plan, not in expressions.

```typescript
interface WindowNode extends BasePlanNode {
  kind: "Window";
  inputId: PlanNodeId;
  partitionBy: LogicalAttributeRef[];
  orderBy: Array<{
    attr: LogicalAttributeRef;
    direction: "asc" | "desc";
  }>;
  frame: WindowFrameSpec;
  windowFunctions: Array<{
    outputName: string;
    aggregate: AggregationOperator;
    input: LogicalExpr;
  }>;
}
```

#### TransformNode

Applies a rowset or table transform. Treated like a specialized join to a transform table.

```typescript
interface TransformNode extends BasePlanNode {
  kind: "Transform";
  inputId: PlanNodeId;
  transformKind: "rowset" | "table";
  transformId: string;
  transformDef: RowsetTransformDefinition | TableTransformDefinition;
  inputAttr: LogicalAttributeRef;
  outputAttr: LogicalAttributeRef;
}
```

#### ProjectNode

Selects and computes output columns.

```typescript
interface ProjectNode extends BasePlanNode {
  kind: "Project";
  inputId: PlanNodeId;
  outputs: Array<{
    name: string;
    expr: LogicalExpr;
  }>;
}
```

---

## Part 4: Logical Query Plan

The **LogicalQueryPlan** ties everything together: the plan DAG, metrics, and grain.

### Structure

```typescript
interface LogicalQueryPlan {
  // Plan DAG
  rootNodeId: PlanNodeId;
  nodes: Map<PlanNodeId, LogicalPlanNode>;

  // Output structure
  outputGrain: ResolvedGrain;
  outputMetrics: LogicalMetricPlan[];

  // Metric evaluation
  metricEvalOrder: string[];   // Topological order

  // Optional annotations
  estimatedRowCount?: number;
  estimatedCost?: number;
}
```

### Supporting Types

#### ResolvedGrain

```typescript
interface ResolvedGrain {
  dimensions: LogicalAttributeRef[];
  grainId: string;             // Canonical ID derived from sorted dimension IDs
}

// grainId is computed, not arbitrary:
function computeGrainId(dimensions: LogicalAttributeRef[]): string {
  return dimensions
    .map(d => d.attributeId)
    .sort()
    .join(",");
}
```

#### LogicalMetricPlan

Contains all plan-level metric information including dependencies and evaluation order.

```typescript
interface LogicalMetricPlan {
  name: string;
  expr: LogicalExpr;
  baseFact: string | null;

  // Dependencies (computed from expr analysis)
  dependencies: string[];                    // Other metrics this depends on
  requiredAttrs: LogicalAttributeRef[];      // Attributes needed (resolved, not strings)

  // Execution
  executionPhase: number;                    // 0 = base aggregates, 1 = derived, etc.
  estimatedCost?: number;                    // Relative cost for ordering
}
```

> **Design note**: `requiredAttrs` uses `LogicalAttributeRef[]` instead of `string[]` for type safety and to preserve resolved table/column information.

---

## Part 5: Transformation Functions

### Syntax to Logical Expression

```typescript
/**
 * Transform a syntax AST (MetricExpr) into a Logical Expression.
 * Resolves attribute references and infers types.
 *
 * Note: Window and Transform nodes in the syntax AST are handled
 * separately during plan building, not expression transformation.
 */
function syntaxToLogical(
  expr: MetricExpr,
  model: SemanticModel,
  baseFact: string | null
): LogicalExpr;
```

### Query Spec to Logical Plan

```typescript
/**
 * Build a complete logical query plan from a query specification.
 * This includes:
 * - Building the plan DAG (scans, joins, filters, aggregates)
 * - Transforming metric expressions
 * - Computing metric dependencies and evaluation order
 * - Resolving grain
 */
function buildLogicalPlan(
  query: QuerySpecV2,
  model: SemanticModel
): LogicalQueryPlan;
```

### Plan Visualization

```typescript
/**
 * Generate a human-readable EXPLAIN output for a logical plan.
 */
function explainPlan(plan: LogicalQueryPlan): string;
```

### Compilation from Logical IR

```typescript
/**
 * Compile a logical plan into executable form.
 * Returns a function that can be invoked with data.
 */
function compileLogicalPlan(plan: LogicalQueryPlan): CompiledQuery;
```

---

## Implementation Phases

### Phase 1: Core Logical Expression Types

**Scope:**
- Define `LogicalExpr` node types in new file `src/logicalAst.ts`
- Define `DataType` type system
- Implement `syntaxToLogical()` transformer for core nodes:
  - `Literal` → `LogicalConstant`
  - `AttrRef` → `LogicalAttributeRef`
  - `MetricRef` → `LogicalMetricRef`
  - `BinaryOp` → `LogicalScalarOp`
  - `Call` (aggregates) → `LogicalAggregate`
- **Defer**: Window, Transform, Conditional, Coalesce (Phase 2)

**Deliverables:**
- `src/logicalAst.ts` - Type definitions
- `src/syntaxToLogical.ts` - Transformation function
- Unit tests for transformation

### Phase 2: Plan Node Infrastructure

**Scope:**
- Define `BasePlanNode` and plan node types
- Implement `FactScanNode`, `DimensionScanNode`, `JoinNode`, `FilterNode`, `AggregateNode`
- Build basic plan DAG construction

**Deliverables:**
- Plan node type definitions
- Plan builder skeleton
- Plan traversal utilities

### Phase 3: Window and Transform as Plan Nodes

**Scope:**
- Implement `WindowNode` and `TransformNode`
- Handle Window/Transform in syntax AST by emitting plan nodes
- Wire window evaluation through the plan

**Deliverables:**
- Window plan node implementation
- Transform plan node implementation
- Integration tests

### Phase 4: Full Query Plan Builder

**Scope:**
- Implement `buildLogicalPlan()` end-to-end
- Join inference from required attributes
- Filter classification (pre-aggregate vs post-aggregate)
- Grain resolution
- Metric dependency DAG with topological sort

**Deliverables:**
- Complete plan builder
- Dependency graph builder
- Cycle detection with clear error messages

### Phase 5: EXPLAIN and Integration

**Scope:**
- Implement `explainPlan()` for visualization
- Update `runSemanticQuery()` to build logical plan first
- Update `compileMetricExpr()` to work from `LogicalExpr`
- Maintain backward compatibility

**Deliverables:**
- EXPLAIN command support
- Updated compiler
- Migration documentation

---

## Benefits Summary

| Benefit | Description |
|---------|-------------|
| **Clean Architecture** | Expressions compute values; Plan handles rowset operations |
| **Better Error Messages** | Report errors with resolved names, types, and source locations |
| **Type Safety** | Catch type mismatches before execution |
| **Visualizable Plans** | Plan node IDs enable tree/DAG visualization |
| **Query Optimization** | Enable filter pushdown, join reordering (future) |
| **SQL Generation** | Logical plan maps naturally to SQL (future) |
| **Caching** | Cache at logical plan level |
| **Debugging** | `EXPLAIN` command to show logical plan before execution |

---

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| **Slim LogicalMetricRef** | Dependencies belong in LogicalMetricPlan, not every reference node |
| **Window/Transform in Plan** | They're rowset operations, not scalar expressions |
| **Predicates as boolean LogicalExpr** | Avoids parallel type hierarchies |
| **Plan nodes with IDs** | Enables DAG structure, visualization, future rewrites |
| **joinKeys as array** | Supports composite keys |
| **Source IDs not table names** | Supports self-joins, role-playing dimensions |
| **requiredAttrs as LogicalAttributeRef[]** | Type safety, preserves resolved info |
| **grainId computed from dimensions** | Canonical, stable, not arbitrary strings |

---

## Appendix: Example Transformation

### DSL Input

```
metric total_sales on fact_orders = sum(amount)
metric order_count on fact_orders = count(orderId)
metric avg_ticket on fact_orders = total_sales / order_count

query weekly_summary {
  dimensions: storeName, salesWeek
  metrics: total_sales, avg_ticket
  where: salesWeek >= 202401
}
```

### Syntax AST (Current)

```typescript
// avg_ticket metric
{
  kind: "BinaryOp",
  op: "/",
  left: { kind: "MetricRef", name: "total_sales" },
  right: { kind: "MetricRef", name: "order_count" }
}
```

### Logical Expression (Proposed)

```typescript
// avg_ticket metric - note the slim MetricRef
{
  kind: "ScalarOp",
  op: "/",
  left: {
    kind: "MetricRef",
    metricName: "total_sales",
    baseFact: "fact_orders",
    resultType: { kind: "number" }
  },
  right: {
    kind: "MetricRef",
    metricName: "order_count",
    baseFact: "fact_orders",
    resultType: { kind: "number" }
  },
  resultType: { kind: "number", precision: "decimal" }
}
```

### Logical Query Plan (Proposed)

```typescript
{
  rootNodeId: "agg_1",

  nodes: new Map([
    ["fact_scan_1", {
      id: "fact_scan_1",
      kind: "FactScan",
      tableName: "fact_orders",
      requiredColumns: [
        { attributeId: "amount", physicalTable: "fact_orders", physicalColumn: "amount", ... },
        { attributeId: "orderId", physicalTable: "fact_orders", physicalColumn: "order_id", ... },
      ],
      inlineFilters: []
    }],

    ["dim_scan_store", {
      id: "dim_scan_store",
      kind: "DimensionScan",
      tableName: "dim_store",
      requiredColumns: [
        { attributeId: "storeName", physicalTable: "dim_store", physicalColumn: "name", ... }
      ],
      inlineFilters: []
    }],

    ["dim_scan_date", {
      id: "dim_scan_date",
      kind: "DimensionScan",
      tableName: "dim_date",
      requiredColumns: [
        { attributeId: "salesWeek", physicalTable: "dim_date", physicalColumn: "week_id", ... }
      ],
      inlineFilters: [{
        kind: "Comparison",
        left: { kind: "AttributeRef", attributeId: "salesWeek", ... },
        op: ">=",
        right: { kind: "Constant", value: 202401, dataType: { kind: "number" } },
        resultType: { kind: "boolean" }
      }]
    }],

    ["join_store", {
      id: "join_store",
      kind: "Join",
      joinType: "inner",
      leftInputId: "fact_scan_1",
      rightInputId: "dim_scan_store",
      joinKeys: [{
        leftAttr: { attributeId: "storeKey", ... },
        rightAttr: { attributeId: "storeKey", ... }
      }],
      cardinality: "N:1"
    }],

    ["join_date", {
      id: "join_date",
      kind: "Join",
      joinType: "inner",
      leftInputId: "join_store",
      rightInputId: "dim_scan_date",
      joinKeys: [{
        leftAttr: { attributeId: "dateKey", ... },
        rightAttr: { attributeId: "dateKey", ... }
      }],
      cardinality: "N:1"
    }],

    ["agg_1", {
      id: "agg_1",
      kind: "Aggregate",
      inputId: "join_date",
      groupBy: [
        { attributeId: "storeName", ... },
        { attributeId: "salesWeek", ... }
      ],
      aggregates: [
        { outputName: "total_sales", expr: { kind: "Aggregate", op: "sum", ... } },
        { outputName: "order_count", expr: { kind: "Aggregate", op: "count", ... } }
      ]
    }]
  ]),

  outputGrain: {
    dimensions: [
      { attributeId: "storeName", logicalName: "storeName", physicalTable: "dim_store", ... },
      { attributeId: "salesWeek", logicalName: "salesWeek", physicalTable: "dim_date", ... }
    ],
    grainId: "salesWeek,storeName"  // Sorted alphabetically
  },

  outputMetrics: [
    {
      name: "total_sales",
      expr: { kind: "Aggregate", op: "sum", ... },
      baseFact: "fact_orders",
      dependencies: [],
      requiredAttrs: [{ attributeId: "amount", ... }],
      executionPhase: 0
    },
    {
      name: "order_count",
      expr: { kind: "Aggregate", op: "count", ... },
      baseFact: "fact_orders",
      dependencies: [],
      requiredAttrs: [{ attributeId: "orderId", ... }],
      executionPhase: 0
    },
    {
      name: "avg_ticket",
      expr: { kind: "ScalarOp", op: "/", ... },
      baseFact: "fact_orders",
      dependencies: ["total_sales", "order_count"],
      requiredAttrs: [],  // Derived metric, no direct attrs
      executionPhase: 1
    }
  ],

  metricEvalOrder: ["total_sales", "order_count", "avg_ticket"]
}
```

### EXPLAIN Output (Example)

```
EXPLAIN weekly_summary:

Plan DAG:
  [agg_1] Aggregate
    groupBy: storeName, salesWeek
    aggregates: total_sales=sum(amount), order_count=count(orderId)
    ↳ [join_date] Join (inner, N:1)
        on: dateKey = dateKey
        ↳ [join_store] Join (inner, N:1)
            on: storeKey = storeKey
            ↳ [fact_scan_1] FactScan fact_orders
                columns: amount, orderId, storeKey, dateKey
            ↳ [dim_scan_store] DimensionScan dim_store
                columns: storeKey, name
        ↳ [dim_scan_date] DimensionScan dim_date
            columns: dateKey, week_id
            filter: salesWeek >= 202401

Output Grain: salesWeek, storeName

Metrics (evaluation order):
  Phase 0: total_sales, order_count
  Phase 1: avg_ticket = total_sales / order_count
```
