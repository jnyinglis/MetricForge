/**
 * Logical AST (IR) for MetricForge Semantic Engine
 *
 * This module defines the Logical Intermediate Representation that sits between
 * the Syntax AST (MetricExpr) and compiled evaluation functions. It provides:
 * - Type-aware expression nodes with resolved references
 * - Clear separation between scalar expressions and rowset operations
 * - Foundation for query optimization and EXPLAIN functionality
 *
 * @see docs/logical-ast-proposal.md for full design documentation
 */

// Re-export types we depend on from semanticEngine
import type {
  MetricExpr,
  SemanticModel,
  LogicalAttribute,
  AggregationOperator as SyntaxAggregationOperator,
  WindowFrameSpec,
  RowsetTransformDefinition,
  TableTransformDefinition,
} from "./semanticEngine";

// ---------------------------------------------------------------------------
// DATA TYPE SYSTEM
// ---------------------------------------------------------------------------

/**
 * Data types for the logical IR.
 * Used for type inference, validation, and future SQL generation.
 */
export type DataType =
  | { kind: "number"; precision?: "integer" | "decimal" }
  | { kind: "string"; maxLength?: number }
  | { kind: "boolean" }
  | { kind: "date" }
  | { kind: "datetime" }
  | { kind: "null" }
  | { kind: "unknown" };

/**
 * Common data type constants for convenience.
 */
export const DataTypes = {
  number: { kind: "number" } as DataType,
  integer: { kind: "number", precision: "integer" } as DataType,
  decimal: { kind: "number", precision: "decimal" } as DataType,
  string: { kind: "string" } as DataType,
  boolean: { kind: "boolean" } as DataType,
  date: { kind: "date" } as DataType,
  datetime: { kind: "datetime" } as DataType,
  null: { kind: "null" } as DataType,
  unknown: { kind: "unknown" } as DataType,
} as const;

// ---------------------------------------------------------------------------
// LOGICAL EXPRESSION IR - Scalar values only
// ---------------------------------------------------------------------------

/**
 * Aggregation operators supported in LogicalAggregate and WindowNode.
 * Mirrors the syntax AST but explicitly typed.
 */
export type AggregationOperator =
  | "sum"
  | "avg"
  | "count"
  | "min"
  | "max"
  | "count_distinct";

/**
 * Union of all logical expression node types.
 * LogicalExpr handles scalar value computation only.
 * Rowset operations (windows, transforms) are in the plan layer.
 */
export type LogicalExpr =
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

/**
 * Type alias for predicates (boolean expressions).
 * This documents intent; use isLogicalPredicate() for runtime checking.
 */
export type LogicalPredicate = LogicalExpr & {
  resultType: { kind: "boolean" };
};

/**
 * Type guard for checking if an expression is a predicate (boolean-typed).
 */
export function isLogicalPredicate(
  expr: LogicalExpr
): expr is LogicalPredicate {
  return "resultType" in expr && expr.resultType?.kind === "boolean";
}

// ---------------------------------------------------------------------------
// VALUE EXPRESSION NODES
// ---------------------------------------------------------------------------

/**
 * Constant value with type information.
 * Replaces syntax AST's Literal node.
 */
export interface LogicalConstant {
  kind: "Constant";
  value: number | string | boolean | null;
  dataType: DataType;
}

/**
 * Resolved attribute reference.
 * Replaces syntax AST's AttrRef with physical table/column resolution.
 */
export interface LogicalAttributeRef {
  kind: "AttributeRef";
  /** Semantic model attribute ID */
  attributeId: string;
  /** Original name from DSL (may differ from attributeId in aliases) */
  logicalName: string;
  /** Resolved physical table name */
  physicalTable: string;
  /** Resolved physical column name */
  physicalColumn: string;
  /** Inferred data type */
  dataType: DataType;
  /** Whether this comes from a fact or dimension table */
  sourceKind: "fact" | "dimension";
}

/**
 * Metric reference (slim version).
 * Dependencies and cost info are in LogicalMetricPlan, not here.
 */
export interface LogicalMetricRef {
  kind: "MetricRef";
  /** Name of the referenced metric */
  metricName: string;
  /** Base fact table for this metric (null for fact-agnostic) */
  baseFact: string | null;
  /** Result type of the metric */
  resultType: DataType;
}

/**
 * Aggregate operation.
 * Note: sourceTable is intentionally omitted - the plan provides data lineage.
 */
export interface LogicalAggregate {
  kind: "Aggregate";
  /** Aggregation operator */
  op: AggregationOperator;
  /** Expression to aggregate (usually an AttributeRef) */
  input: LogicalExpr;
  /** Whether to use DISTINCT */
  distinct: boolean;
  /** Optional filter applied before aggregation */
  filter?: LogicalPredicate;
  /** Result type (always number for aggregates) */
  resultType: DataType;
}

/**
 * Scalar arithmetic operation.
 * Replaces syntax AST's BinaryOp with result type.
 */
export interface LogicalScalarOp {
  kind: "ScalarOp";
  op: "+" | "-" | "*" | "/" | "%" | "^";
  left: LogicalExpr;
  right: LogicalExpr;
  resultType: DataType;
}

/**
 * Scalar function call (non-aggregate functions).
 */
export interface LogicalScalarFunction {
  kind: "ScalarFunction";
  /** Function name */
  fn: string;
  /** Function arguments */
  args: LogicalExpr[];
  /** Result type */
  resultType: DataType;
}

/**
 * Conditional expression (IF/CASE).
 */
export interface LogicalConditional {
  kind: "Conditional";
  /** Condition (must be boolean-typed) */
  condition: LogicalExpr;
  /** Value if condition is true */
  thenExpr: LogicalExpr;
  /** Value if condition is false */
  elseExpr: LogicalExpr;
  /** Result type (unified from then/else branches) */
  resultType: DataType;
}

/**
 * Coalesce expression for null handling.
 */
export interface LogicalCoalesce {
  kind: "Coalesce";
  /** Expressions to try in order (first non-null wins) */
  exprs: LogicalExpr[];
  /** Result type */
  resultType: DataType;
}

// ---------------------------------------------------------------------------
// BOOLEAN EXPRESSION NODES (Predicates)
// ---------------------------------------------------------------------------

/**
 * Comparison operation.
 */
export interface LogicalComparison {
  kind: "Comparison";
  left: LogicalExpr;
  op: "=" | "!=" | "<" | "<=" | ">" | ">=";
  right: LogicalExpr;
  resultType: { kind: "boolean" };
}

/**
 * Logical operation (AND, OR, NOT).
 *
 * Enforcement: Validators must check operand counts:
 * - "not": exactly 1 operand
 * - "and" / "or": 2 or more operands
 */
export interface LogicalLogicalOp {
  kind: "LogicalOp";
  op: "and" | "or" | "not";
  /** Operands (must be boolean-typed) */
  operands: LogicalPredicate[];
  resultType: { kind: "boolean" };
}

/**
 * IN list expression.
 */
export interface LogicalInList {
  kind: "InList";
  expr: LogicalExpr;
  values: LogicalConstant[];
  /** True for NOT IN */
  negated: boolean;
  resultType: { kind: "boolean" };
}

/**
 * BETWEEN expression.
 */
export interface LogicalBetween {
  kind: "Between";
  expr: LogicalExpr;
  low: LogicalExpr;
  high: LogicalExpr;
  resultType: { kind: "boolean" };
}

/**
 * IS NULL / IS NOT NULL expression.
 */
export interface LogicalIsNull {
  kind: "IsNull";
  expr: LogicalExpr;
  /** True for IS NOT NULL */
  negated: boolean;
  resultType: { kind: "boolean" };
}

// ---------------------------------------------------------------------------
// LOGICAL PLAN NODE TYPES (Phase 2+)
// ---------------------------------------------------------------------------
// These are defined here for completeness but implementation is deferred.

export type PlanNodeId = string;

export interface BasePlanNode {
  id: PlanNodeId;
  annotations?: Record<string, unknown>;
}

// Plan node types will be fully implemented in Phase 2
export type LogicalPlanNode =
  | FactScanNode
  | DimensionScanNode
  | JoinNode
  | FilterNode
  | AggregateNode
  | WindowNode
  | TransformNode
  | ProjectNode;

export interface FactScanNode extends BasePlanNode {
  kind: "FactScan";
  tableName: string;
  requiredColumns: LogicalAttributeRef[];
  inlineFilters: LogicalPredicate[];
}

export interface DimensionScanNode extends BasePlanNode {
  kind: "DimensionScan";
  tableName: string;
  requiredColumns: LogicalAttributeRef[];
  inlineFilters: LogicalPredicate[];
}

export interface JoinNode extends BasePlanNode {
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

export interface FilterNode extends BasePlanNode {
  kind: "Filter";
  inputId: PlanNodeId;
  predicate: LogicalPredicate;
}

export interface AggregateNode extends BasePlanNode {
  kind: "Aggregate";
  inputId: PlanNodeId;
  groupBy: LogicalAttributeRef[];
  aggregates: Array<{
    outputName: string;
    expr: LogicalAggregate;
  }>;
}

export interface WindowNode extends BasePlanNode {
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
    op: AggregationOperator;
    input: LogicalExpr;
  }>;
}

export interface TransformNode extends BasePlanNode {
  kind: "Transform";
  inputId: PlanNodeId;
  transformKind: "rowset" | "table";
  transformId: string;
  transformDef?: RowsetTransformDefinition | TableTransformDefinition;
  inputAttr: LogicalAttributeRef;
  outputAttr: LogicalAttributeRef;
}

export interface ProjectNode extends BasePlanNode {
  kind: "Project";
  inputId: PlanNodeId;
  outputs: Array<{
    name: string;
    expr: LogicalExpr;
  }>;
}

// ---------------------------------------------------------------------------
// LOGICAL QUERY PLAN (Phase 4+)
// ---------------------------------------------------------------------------

export interface ResolvedGrain {
  dimensions: LogicalAttributeRef[];
  /** Canonical ID derived from sorted dimension IDs */
  grainId: string;
}

/**
 * Compute a canonical grain ID from dimensions.
 */
export function computeGrainId(dimensions: LogicalAttributeRef[]): string {
  return dimensions
    .map((d) => d.attributeId)
    .sort()
    .join(",");
}

export interface LogicalMetricPlan {
  name: string;
  expr: LogicalExpr;
  baseFact: string | null;
  /** Other metrics this depends on */
  dependencies: string[];
  /** Attributes needed (resolved) */
  requiredAttrs: LogicalAttributeRef[];
  /** 0 = base aggregates, 1 = derived, etc. */
  executionPhase: number;
  /** Relative cost for ordering */
  estimatedCost?: number;
}

export interface LogicalQueryPlan {
  /** Root of the plan DAG */
  rootNodeId: PlanNodeId;
  /** All plan nodes by ID */
  nodes: Map<PlanNodeId, LogicalPlanNode>;
  /** Output grain (dimensions) */
  outputGrain: ResolvedGrain;
  /** Metrics to compute */
  outputMetrics: LogicalMetricPlan[];
  /** Topological evaluation order */
  metricEvalOrder: string[];
  /** Optional: estimated output row count */
  estimatedRowCount?: number;
  /** Optional: estimated total cost */
  estimatedCost?: number;
}

// ---------------------------------------------------------------------------
// EXPRESSION BUILDER HELPERS
// ---------------------------------------------------------------------------

/**
 * Helper functions for constructing LogicalExpr nodes.
 * Similar to the Expr helpers in semanticEngine.ts.
 */
export const LogicalExprBuilder = {
  constant(value: number | string | boolean | null, dataType?: DataType): LogicalConstant {
    const inferredType: DataType =
      dataType ??
      (typeof value === "number"
        ? DataTypes.number
        : typeof value === "string"
          ? DataTypes.string
          : typeof value === "boolean"
            ? DataTypes.boolean
            : DataTypes.null);
    return { kind: "Constant", value, dataType: inferredType };
  },

  attributeRef(
    attributeId: string,
    physicalTable: string,
    physicalColumn: string,
    dataType: DataType,
    sourceKind: "fact" | "dimension"
  ): LogicalAttributeRef {
    return {
      kind: "AttributeRef",
      attributeId,
      logicalName: attributeId,
      physicalTable,
      physicalColumn,
      dataType,
      sourceKind,
    };
  },

  metricRef(
    metricName: string,
    baseFact: string | null,
    resultType: DataType = DataTypes.number
  ): LogicalMetricRef {
    return { kind: "MetricRef", metricName, baseFact, resultType };
  },

  aggregate(
    op: AggregationOperator,
    input: LogicalExpr,
    distinct = false
  ): LogicalAggregate {
    return {
      kind: "Aggregate",
      op,
      input,
      distinct,
      resultType: DataTypes.number,
    };
  },

  scalarOp(
    op: "+" | "-" | "*" | "/" | "%" | "^",
    left: LogicalExpr,
    right: LogicalExpr,
    resultType: DataType = DataTypes.number
  ): LogicalScalarOp {
    return { kind: "ScalarOp", op, left, right, resultType };
  },

  add(left: LogicalExpr, right: LogicalExpr): LogicalScalarOp {
    return this.scalarOp("+", left, right);
  },

  sub(left: LogicalExpr, right: LogicalExpr): LogicalScalarOp {
    return this.scalarOp("-", left, right);
  },

  mul(left: LogicalExpr, right: LogicalExpr): LogicalScalarOp {
    return this.scalarOp("*", left, right);
  },

  div(left: LogicalExpr, right: LogicalExpr): LogicalScalarOp {
    return this.scalarOp("/", left, right, DataTypes.decimal);
  },

  comparison(
    left: LogicalExpr,
    op: "=" | "!=" | "<" | "<=" | ">" | ">=",
    right: LogicalExpr
  ): LogicalComparison {
    return { kind: "Comparison", left, op, right, resultType: { kind: "boolean" } };
  },

  and(...operands: LogicalPredicate[]): LogicalLogicalOp {
    if (operands.length < 2) {
      throw new Error("AND requires at least 2 operands");
    }
    return { kind: "LogicalOp", op: "and", operands, resultType: { kind: "boolean" } };
  },

  or(...operands: LogicalPredicate[]): LogicalLogicalOp {
    if (operands.length < 2) {
      throw new Error("OR requires at least 2 operands");
    }
    return { kind: "LogicalOp", op: "or", operands, resultType: { kind: "boolean" } };
  },

  not(operand: LogicalPredicate): LogicalLogicalOp {
    return { kind: "LogicalOp", op: "not", operands: [operand], resultType: { kind: "boolean" } };
  },
};
