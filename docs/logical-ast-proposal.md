# Logical AST (IR) Proposal

> **Status**: Draft Proposal
> **Author**: Claude
> **Date**: 2025-11-23

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
                    Logical AST/IR (NEW)
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
4. **Dependency Tracking**: Explicit metric dependency graph
5. **Extensibility**: Easy to add new node types and optimization passes
6. **Backward Compatibility**: Existing DSL and APIs continue to work

---

## Proposed Type System

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
| `sum`, `avg`, `min`, `max` | `number` |
| `count` | `number (integer)` |
| Arithmetic (`+`, `-`, `*`, `/`) | `number` |
| Comparison (`>`, `<`, `=`) | `boolean` |
| Attribute reference | Inferred from schema |

---

## Logical Expression IR

The Logical Expression IR replaces/transforms the existing `MetricExpr` syntax AST into a semantically-rich representation.

### Node Type Summary

| Syntax AST (Current) | Logical IR (Proposed) | Key Difference |
|---------------------|----------------------|----------------|
| `Literal` | `LogicalConstant` | Adds `dataType` |
| `AttrRef` | `LogicalAttributeRef` | Resolves to physical table.column |
| `MetricRef` | `LogicalMetricRef` | Adds dependency metadata |
| `Call` (aggregates) | `LogicalAggregate` | Separates aggregate from scalar |
| `Call` (scalars) | `LogicalScalarFunction` | Type-checked scalar functions |
| `BinaryOp` | `LogicalScalarOp` | Adds `resultType` |
| `Window` | `LogicalWindow` | Resolved partition/order attributes |
| `Transform` | `LogicalTransform` | Validated transform reference |
| — | `LogicalConditional` | NEW: if/case expressions |
| — | `LogicalCoalesce` | NEW: null handling |

### Node Definitions

```typescript
// ═══════════════════════════════════════════════════════════════════
// LOGICAL EXPRESSION IR
// ═══════════════════════════════════════════════════════════════════

type LogicalExpr =
  | LogicalConstant
  | LogicalAttributeRef
  | LogicalMetricRef
  | LogicalAggregate
  | LogicalScalarOp
  | LogicalScalarFunction
  | LogicalWindow
  | LogicalTransform
  | LogicalConditional
  | LogicalCoalesce;
```

#### LogicalConstant

Replaces `Literal` with type information.

```typescript
interface LogicalConstant {
  kind: "Constant";
  value: number | string | boolean | null;
  dataType: DataType;
}
```

**Example transformation:**
```
Syntax:   { kind: "Literal", value: 100 }
Logical:  { kind: "Constant", value: 100, dataType: { kind: "number" } }
```

#### LogicalAttributeRef

Replaces `AttrRef` with resolved physical location.

```typescript
interface LogicalAttributeRef {
  kind: "AttributeRef";
  logicalName: string;        // Original name from DSL
  physicalTable: string;      // Resolved table name
  physicalColumn: string;     // Resolved column name
  dataType: DataType;
  sourceKind: "fact" | "dimension";
}
```

**Example transformation:**
```
Syntax:   { kind: "AttrRef", name: "storeName" }

Logical:  {
  kind: "AttributeRef",
  logicalName: "storeName",
  physicalTable: "dim_store",
  physicalColumn: "name",
  dataType: { kind: "string" },
  sourceKind: "dimension"
}
```

#### LogicalMetricRef

Replaces `MetricRef` with dependency metadata for evaluation ordering.

```typescript
interface LogicalMetricRef {
  kind: "MetricRef";
  metricName: string;
  baseFact: string | null;
  dependencies: string[];     // Transitive metric dependencies
  requiredAttrs: string[];    // Attributes needed for evaluation
  evaluationCost: number;     // Relative cost for ordering
}
```

**Example transformation:**
```
Syntax:   { kind: "MetricRef", name: "avg_ticket" }

Logical:  {
  kind: "MetricRef",
  metricName: "avg_ticket",
  baseFact: "fact_orders",
  dependencies: ["total_sales", "order_count"],
  requiredAttrs: ["amount", "orderId"],
  evaluationCost: 2
}
```

#### LogicalAggregate

Replaces `Call` nodes for aggregate functions with explicit source tracking.

```typescript
interface LogicalAggregate {
  kind: "Aggregate";
  op: "sum" | "avg" | "count" | "min" | "max" | "count_distinct";
  input: LogicalExpr;         // What to aggregate
  distinct: boolean;
  filter?: LogicalPredicate;  // Optional aggregate filter (future)
  sourceTable: string;        // Which fact table
  resultType: DataType;
}
```

**Example transformation:**
```
Syntax:   { kind: "Call", fn: "sum", args: [{ kind: "AttrRef", name: "amount" }] }

Logical:  {
  kind: "Aggregate",
  op: "sum",
  input: { kind: "AttributeRef", logicalName: "amount", ... },
  distinct: false,
  sourceTable: "fact_orders",
  resultType: { kind: "number" }
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

**Example transformation:**
```
Syntax:   { kind: "BinaryOp", op: "/", left: ..., right: ... }

Logical:  {
  kind: "ScalarOp",
  op: "/",
  left: { kind: "MetricRef", metricName: "total_sales", ... },
  right: { kind: "MetricRef", metricName: "order_count", ... },
  resultType: { kind: "number", precision: "decimal" }
}
```

#### LogicalScalarFunction

For non-aggregate function calls (future scalar functions).

```typescript
interface LogicalScalarFunction {
  kind: "ScalarFunction";
  fn: string;
  args: LogicalExpr[];
  resultType: DataType;
}
```

#### LogicalWindow

Replaces `Window` with fully resolved partition and order specifications.

```typescript
interface LogicalWindow {
  kind: "Window";
  input: LogicalExpr;
  partitionBy: LogicalAttributeRef[];
  orderBy: Array<{
    attr: LogicalAttributeRef;
    direction: "asc" | "desc";
  }>;
  frame: WindowFrameSpec;
  aggregate: AggregationOperator;
  resultType: DataType;
}
```

#### LogicalTransform

Represents rowset or table transforms with validated references.

```typescript
interface LogicalTransform {
  kind: "Transform";
  transformKind: "rowset" | "table";
  transformId: string;
  transformDef: RowsetTransformDefinition | TableTransformDefinition;
  input: LogicalExpr;
  inputAttr?: LogicalAttributeRef;
  outputAttr?: LogicalAttributeRef;
  resultType: DataType;
}
```

#### LogicalConditional (New)

Enables conditional logic in metrics.

```typescript
interface LogicalConditional {
  kind: "Conditional";
  condition: LogicalPredicate;
  thenExpr: LogicalExpr;
  elseExpr: LogicalExpr;
  resultType: DataType;
}
```

#### LogicalCoalesce (New)

Handles null values explicitly.

```typescript
interface LogicalCoalesce {
  kind: "Coalesce";
  exprs: LogicalExpr[];
  resultType: DataType;
}
```

---

## Logical Query Plan IR

The Logical Query Plan represents the full structure of a query, including data sources, joins, and metric evaluation order.

### Structure

```typescript
interface LogicalQueryPlan {
  // Output structure
  outputGrain: ResolvedGrain;
  outputMetrics: LogicalMetricPlan[];

  // Data sources
  dataSources: LogicalDataSource[];
  joins: LogicalJoin[];

  // Filtering
  preAggregateFilters: LogicalPredicate[];   // Push down to scan
  postAggregateFilters: LogicalPredicate[];  // Having clause

  // Execution hints
  metricEvalOrder: string[];  // Topological order
  estimatedCardinality?: number;
}
```

### Supporting Types

```typescript
interface ResolvedGrain {
  dimensions: LogicalAttributeRef[];
  grainKey: string;  // Canonical grain identifier (e.g., "store,week")
}

interface LogicalDataSource {
  kind: "fact" | "dimension";
  tableName: string;
  requiredColumns: string[];
  filters: LogicalPredicate[];  // Filters applicable to this source
}

interface LogicalJoin {
  left: string;   // table name
  right: string;  // table name
  joinType: "inner" | "left";
  joinKey: { leftCol: string; rightCol: string };
  cardinality: "1:1" | "1:N" | "N:1";
}

interface LogicalMetricPlan {
  name: string;
  expr: LogicalExpr;          // Transformed expression
  baseFact: string | null;
  dependencies: string[];     // Other metrics this depends on
  requiredAttrs: string[];    // Attributes needed
  executionPhase: number;     // 0 = base aggregates, 1 = derived, etc.
}
```

### Predicate Types

```typescript
type LogicalPredicate =
  | LogicalComparison
  | LogicalInList
  | LogicalBetween
  | LogicalIsNull
  | LogicalAnd
  | LogicalOr
  | LogicalNot;

interface LogicalComparison {
  kind: "Comparison";
  left: LogicalExpr;
  op: "=" | "!=" | "<" | "<=" | ">" | ">=";
  right: LogicalExpr;
}

interface LogicalInList {
  kind: "InList";
  expr: LogicalExpr;
  values: LogicalConstant[];
  negated: boolean;
}

interface LogicalBetween {
  kind: "Between";
  expr: LogicalExpr;
  low: LogicalExpr;
  high: LogicalExpr;
}

interface LogicalIsNull {
  kind: "IsNull";
  expr: LogicalExpr;
  negated: boolean;
}

interface LogicalAnd {
  kind: "And";
  operands: LogicalPredicate[];
}

interface LogicalOr {
  kind: "Or";
  operands: LogicalPredicate[];
}

interface LogicalNot {
  kind: "Not";
  operand: LogicalPredicate;
}
```

---

## Transformation Functions

### Syntax to Logical Expression

```typescript
/**
 * Transform a syntax AST (MetricExpr) into a Logical Expression.
 * Resolves attribute references and infers types.
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
 */
function buildLogicalPlan(
  query: QuerySpecV2,
  model: SemanticModel
): LogicalQueryPlan;
```

### Compilation from Logical IR

```typescript
/**
 * Compile a logical expression into an executable MetricEval function.
 */
function compileLogicalExpr(expr: LogicalExpr): MetricEvalV2;
```

---

## Implementation Phases

### Phase 1: Core Logical Expression Types

**Scope:**
- Define `LogicalExpr` node types in new file `src/logicalAst.ts`
- Define `DataType` type system
- Implement `syntaxToLogical()` transformer for basic nodes:
  - `Literal` → `LogicalConstant`
  - `AttrRef` → `LogicalAttributeRef`
  - `MetricRef` → `LogicalMetricRef`
  - `BinaryOp` → `LogicalScalarOp`
  - `Call` → `LogicalAggregate`

**Deliverables:**
- `src/logicalAst.ts` - Type definitions
- `src/syntaxToLogical.ts` - Transformation function
- Unit tests for transformation

### Phase 2: Name Resolution & Type Inference

**Scope:**
- Implement `resolveAttributeRef()` to map logical names → physical table.column
- Implement `inferType()` for type propagation through expressions
- Add validation for type compatibility in operations

**Deliverables:**
- Type inference functions
- Validation error types with source location
- Integration tests

### Phase 3: Logical Query Plan

**Scope:**
- Define `LogicalQueryPlan` structure
- Implement `buildLogicalPlan()` that takes QuerySpec + SemanticModel → LogicalQueryPlan
- Add join inference based on required attributes
- Implement filter classification (pre-aggregate vs post-aggregate)

**Deliverables:**
- Query plan builder
- Join inference logic
- Plan visualization (EXPLAIN output)

### Phase 4: Metric Dependency DAG

**Scope:**
- Build explicit dependency graph as part of LogicalQueryPlan
- Topological sort for evaluation order
- Detect and report cycles with better error messages
- Add execution phase assignment

**Deliverables:**
- Dependency graph builder
- Topological sort implementation
- Enhanced cycle detection errors

### Phase 5: Integration & Migration

**Scope:**
- Update `compileMetricExpr()` to work from `LogicalExpr`
- Update `runSemanticQuery()` to build logical plan first
- Add debug/explain mode to visualize logical plan
- Maintain backward compatibility

**Deliverables:**
- Updated compiler
- EXPLAIN command support
- Migration documentation

---

## Benefits Summary

| Benefit | Description |
|---------|-------------|
| **Better Error Messages** | Report errors with resolved names, types, and source locations |
| **Type Safety** | Catch type mismatches before execution |
| **Query Optimization** | Enable filter pushdown, join reordering (future) |
| **SQL Generation** | Logical plan maps naturally to SQL (future) |
| **Caching** | Cache at logical plan level, not just results |
| **Debugging** | `EXPLAIN` command to show logical plan before execution |
| **Extensibility** | Add new node types without changing parser |
| **Dependency Clarity** | Explicit metric dependency graph with evaluation order |

---

## Open Questions

1. **Scope of Phase 1**: Should we implement all LogicalExpr types at once, or start with a subset (Constant, AttributeRef, Aggregate, ScalarOp)?

2. **Type System Depth**: How detailed should `DataType` be initially? Start simple (number/string/boolean) or include precision from the start?

3. **Backward Compatibility**: Should `compileMetricExpr()` continue to accept raw `MetricExpr`, or require `LogicalExpr` after migration?

4. **Transform Representation**: How should `Transform` (rowset/table transforms) be represented in the logical IR? Should transform definitions be inlined or referenced?

5. **Filter IR**: Should we create a new `LogicalPredicate` type or enhance the existing `FilterNode`?

6. **Window Functions**: Should window function evaluation strategy be encoded in the logical plan, or remain a runtime decision?

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
// avg_ticket metric
{
  kind: "ScalarOp",
  op: "/",
  left: {
    kind: "MetricRef",
    metricName: "total_sales",
    baseFact: "fact_orders",
    dependencies: [],
    requiredAttrs: ["amount"],
    evaluationCost: 1
  },
  right: {
    kind: "MetricRef",
    metricName: "order_count",
    baseFact: "fact_orders",
    dependencies: [],
    requiredAttrs: ["orderId"],
    evaluationCost: 1
  },
  resultType: { kind: "number", precision: "decimal" }
}
```

### Logical Query Plan (Proposed)

```typescript
{
  outputGrain: {
    dimensions: [
      { kind: "AttributeRef", logicalName: "storeName", physicalTable: "dim_store", physicalColumn: "name", ... },
      { kind: "AttributeRef", logicalName: "salesWeek", physicalTable: "dim_date", physicalColumn: "week_id", ... }
    ],
    grainKey: "storeName,salesWeek"
  },

  outputMetrics: [
    {
      name: "total_sales",
      expr: { kind: "Aggregate", op: "sum", ... },
      baseFact: "fact_orders",
      dependencies: [],
      executionPhase: 0
    },
    {
      name: "avg_ticket",
      expr: { kind: "ScalarOp", op: "/", ... },
      baseFact: "fact_orders",
      dependencies: ["total_sales", "order_count"],
      executionPhase: 1
    }
  ],

  dataSources: [
    { kind: "fact", tableName: "fact_orders", requiredColumns: ["amount", "orderId", "storeKey", "dateKey"], filters: [] },
    { kind: "dimension", tableName: "dim_store", requiredColumns: ["storeKey", "name"], filters: [] },
    { kind: "dimension", tableName: "dim_date", requiredColumns: ["dateKey", "week_id"], filters: [{ kind: "Comparison", ... }] }
  ],

  joins: [
    { left: "fact_orders", right: "dim_store", joinType: "inner", joinKey: { leftCol: "storeKey", rightCol: "storeKey" }, cardinality: "N:1" },
    { left: "fact_orders", right: "dim_date", joinType: "inner", joinKey: { leftCol: "dateKey", rightCol: "dateKey" }, cardinality: "N:1" }
  ],

  preAggregateFilters: [
    { kind: "Comparison", left: { kind: "AttributeRef", logicalName: "salesWeek", ... }, op: ">=", right: { kind: "Constant", value: 202401, ... } }
  ],

  postAggregateFilters: [],

  metricEvalOrder: ["total_sales", "order_count", "avg_ticket"]
}
```
