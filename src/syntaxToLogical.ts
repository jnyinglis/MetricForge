/**
 * Syntax AST to Logical IR Transformation
 *
 * Transforms MetricExpr (syntax AST) nodes into LogicalExpr (logical IR) nodes.
 * This includes:
 * - Resolving attribute references to physical table.column
 * - Inferring data types
 * - Validating metric references
 *
 * Note: Window and Transform nodes are handled separately during plan building
 * (Phase 3+), not in this expression-level transformation.
 *
 * @see docs/logical-ast-proposal.md
 */

import type { MetricExpr, SemanticModel, LogicalAttribute } from "./semanticEngine";
import {
  LogicalExpr,
  LogicalConstant,
  LogicalAttributeRef,
  LogicalMetricRef,
  LogicalAggregate,
  LogicalScalarOp,
  AggregationOperator,
  DataType,
  DataTypes,
  isLogicalPredicate,
} from "./logicalAst";

// ---------------------------------------------------------------------------
// TRANSFORMATION ERRORS
// ---------------------------------------------------------------------------

export class TransformationError extends Error {
  constructor(
    message: string,
    public readonly node: MetricExpr,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = "TransformationError";
  }
}

// ---------------------------------------------------------------------------
// ATTRIBUTE RESOLUTION
// ---------------------------------------------------------------------------

/**
 * Result of resolving an attribute name against the semantic model.
 */
export interface ResolvedAttribute {
  attributeId: string;
  physicalTable: string;
  physicalColumn: string;
  sourceKind: "fact" | "dimension";
  dataType: DataType;
}

/**
 * Resolve an attribute name to its physical location using the semantic model.
 */
export function resolveAttribute(
  attrName: string,
  model: SemanticModel,
  baseFact: string | null
): ResolvedAttribute {
  const attr = model.attributes[attrName];
  if (!attr) {
    throw new TransformationError(
      `Unknown attribute: "${attrName}"`,
      { kind: "AttrRef", name: attrName }
    );
  }

  const physicalTable = attr.table;
  const physicalColumn = attr.column ?? attrName;

  // Determine if this is a fact or dimension attribute
  const sourceKind = determineSourceKind(physicalTable, model);

  // Infer data type (default to number for now; could be extended with schema metadata)
  const dataType = inferAttributeType(attrName, model);

  return {
    attributeId: attrName,
    physicalTable,
    physicalColumn,
    sourceKind,
    dataType,
  };
}

/**
 * Determine if a table is a fact or dimension.
 */
function determineSourceKind(
  tableName: string,
  model: SemanticModel
): "fact" | "dimension" {
  // Check if it's a fact table
  for (const [factId, factDef] of Object.entries(model.facts)) {
    const factTable = factDef.table ?? factId;
    if (factTable === tableName) {
      return "fact";
    }
  }

  // Check if it's a dimension table
  for (const [dimId, dimDef] of Object.entries(model.dimensions)) {
    const dimTable = dimDef.table ?? dimId;
    if (dimTable === tableName) {
      return "dimension";
    }
  }

  // Default to dimension if unknown (safer for joins)
  return "dimension";
}

/**
 * Infer the data type of an attribute.
 * For now, defaults to number. Could be extended with schema metadata.
 */
function inferAttributeType(attrName: string, model: SemanticModel): DataType {
  // Future: look up type from schema metadata
  // For now, assume number for most attributes
  return DataTypes.unknown;
}

// ---------------------------------------------------------------------------
// AGGREGATE FUNCTION MAPPING
// ---------------------------------------------------------------------------

const AGGREGATE_FUNCTIONS: Set<string> = new Set([
  "sum",
  "avg",
  "count",
  "min",
  "max",
  "count_distinct",
]);

function isAggregateFunction(fn: string): fn is AggregationOperator {
  return AGGREGATE_FUNCTIONS.has(fn);
}

// ---------------------------------------------------------------------------
// MAIN TRANSFORMATION
// ---------------------------------------------------------------------------

/**
 * Transform options for customizing the transformation behavior.
 */
export interface TransformOptions {
  /**
   * If true, throw on Window/Transform nodes.
   * If false, return a placeholder (for partial transformation).
   */
  strictMode?: boolean;
}

/**
 * Context for the transformation, containing resolved information.
 */
export interface TransformContext {
  model: SemanticModel;
  baseFact: string | null;
  options: TransformOptions;
}

/**
 * Transform a syntax AST (MetricExpr) into a Logical Expression.
 * Resolves attribute references and infers types.
 *
 * Note: Window and Transform nodes in the syntax AST should be handled
 * separately during plan building, not expression transformation.
 *
 * @param expr - The syntax AST expression to transform
 * @param model - The semantic model for attribute resolution
 * @param baseFact - The base fact table for this metric (for attribute resolution)
 * @param options - Optional transformation options
 * @returns The transformed logical expression
 * @throws TransformationError if the expression cannot be transformed
 */
export function syntaxToLogical(
  expr: MetricExpr,
  model: SemanticModel,
  baseFact: string | null,
  options: TransformOptions = {}
): LogicalExpr {
  const ctx: TransformContext = {
    model,
    baseFact,
    options: { strictMode: true, ...options },
  };

  return transformExpr(expr, ctx);
}

/**
 * Internal recursive transformation function.
 */
function transformExpr(expr: MetricExpr, ctx: TransformContext): LogicalExpr {
  switch (expr.kind) {
    case "Literal":
      return transformLiteral(expr, ctx);

    case "AttrRef":
      return transformAttrRef(expr, ctx);

    case "MetricRef":
      return transformMetricRef(expr, ctx);

    case "BinaryOp":
      return transformBinaryOp(expr, ctx);

    case "Call":
      return transformCall(expr, ctx);

    case "Window":
      return handleWindowNode(expr, ctx);

    case "Transform":
      return handleTransformNode(expr, ctx);

    default:
      // TypeScript exhaustiveness check
      const _exhaustive: never = expr;
      throw new TransformationError(
        `Unknown expression kind: ${(expr as any).kind}`,
        expr
      );
  }
}

// ---------------------------------------------------------------------------
// NODE TRANSFORMERS
// ---------------------------------------------------------------------------

/**
 * Transform a Literal node to LogicalConstant.
 */
function transformLiteral(
  expr: Extract<MetricExpr, { kind: "Literal" }>,
  ctx: TransformContext
): LogicalConstant {
  return {
    kind: "Constant",
    value: expr.value,
    dataType: DataTypes.number, // Literals in MetricExpr are always numbers
  };
}

/**
 * Transform an AttrRef node to LogicalAttributeRef.
 */
function transformAttrRef(
  expr: Extract<MetricExpr, { kind: "AttrRef" }>,
  ctx: TransformContext
): LogicalAttributeRef {
  // Special case: count(*) uses "*" as the attribute name
  if (expr.name === "*") {
    return {
      kind: "AttributeRef",
      attributeId: "*",
      logicalName: "*",
      physicalTable: "*",
      physicalColumn: "*",
      dataType: DataTypes.unknown,
      sourceKind: "fact",
    };
  }

  const resolved = resolveAttribute(expr.name, ctx.model, ctx.baseFact);

  return {
    kind: "AttributeRef",
    attributeId: resolved.attributeId,
    logicalName: expr.name,
    physicalTable: resolved.physicalTable,
    physicalColumn: resolved.physicalColumn,
    dataType: resolved.dataType,
    sourceKind: resolved.sourceKind,
  };
}

/**
 * Transform a MetricRef node to LogicalMetricRef.
 */
function transformMetricRef(
  expr: Extract<MetricExpr, { kind: "MetricRef" }>,
  ctx: TransformContext
): LogicalMetricRef {
  const metricDef = ctx.model.metrics[expr.name];

  if (!metricDef) {
    throw new TransformationError(
      `Unknown metric: "${expr.name}"`,
      expr
    );
  }

  return {
    kind: "MetricRef",
    metricName: expr.name,
    baseFact: metricDef.baseFact ?? null,
    resultType: DataTypes.number, // Metrics always return numbers
  };
}

/**
 * Transform a BinaryOp node to LogicalScalarOp.
 */
function transformBinaryOp(
  expr: Extract<MetricExpr, { kind: "BinaryOp" }>,
  ctx: TransformContext
): LogicalScalarOp {
  const left = transformExpr(expr.left, ctx);
  const right = transformExpr(expr.right, ctx);

  // Determine result type based on operator
  const resultType: DataType =
    expr.op === "/" ? DataTypes.decimal : DataTypes.number;

  return {
    kind: "ScalarOp",
    op: expr.op,
    left,
    right,
    resultType,
  };
}

/**
 * Transform a Call node.
 * Aggregate functions become LogicalAggregate.
 * Other functions become LogicalScalarFunction (future).
 */
function transformCall(
  expr: Extract<MetricExpr, { kind: "Call" }>,
  ctx: TransformContext
): LogicalExpr {
  const fn = expr.fn.toLowerCase();

  // Handle special functions
  if (fn === "last_year") {
    return transformLastYear(expr, ctx);
  }

  // Handle aggregate functions
  if (isAggregateFunction(fn)) {
    return transformAggregate(expr, fn, ctx);
  }

  // Unknown function
  throw new TransformationError(
    `Unknown function: "${expr.fn}"`,
    expr
  );
}

/**
 * Transform an aggregate function call to LogicalAggregate.
 */
function transformAggregate(
  expr: Extract<MetricExpr, { kind: "Call" }>,
  op: AggregationOperator,
  ctx: TransformContext
): LogicalAggregate {
  if (expr.args.length === 0) {
    throw new TransformationError(
      `Aggregate function "${op}" requires at least one argument`,
      expr
    );
  }

  // Transform the input expression
  const input = transformExpr(expr.args[0], ctx);

  return {
    kind: "Aggregate",
    op,
    input,
    distinct: false, // Could be extended for COUNT(DISTINCT ...)
    resultType: DataTypes.number,
  };
}

/**
 * Transform last_year() function.
 * This is a special case that involves time-based transformation.
 * For Phase 1, we represent it as a scalar function; in later phases
 * it may become a plan-level transform.
 */
function transformLastYear(
  expr: Extract<MetricExpr, { kind: "Call" }>,
  ctx: TransformContext
): LogicalExpr {
  // last_year(metric, by anchor) takes a metric ref and an anchor attribute
  if (expr.args.length < 1) {
    throw new TransformationError(
      `last_year() requires at least one argument (metric reference)`,
      expr
    );
  }

  const metricArg = expr.args[0];
  if (metricArg.kind !== "MetricRef") {
    throw new TransformationError(
      `last_year() first argument must be a metric reference`,
      expr
    );
  }

  // Transform to a scalar function for now
  // In Phase 3+, this will become a TransformNode in the plan
  return {
    kind: "ScalarFunction",
    fn: "last_year",
    args: expr.args.map((arg) => transformExpr(arg, ctx)),
    resultType: DataTypes.number,
  };
}

// ---------------------------------------------------------------------------
// DEFERRED NODE HANDLERS (Window, Transform)
// ---------------------------------------------------------------------------

/**
 * Handle Window nodes.
 * In Phase 1, we throw or return a placeholder since windows
 * should be handled at the plan level (Phase 3+).
 */
function handleWindowNode(
  expr: Extract<MetricExpr, { kind: "Window" }>,
  ctx: TransformContext
): LogicalExpr {
  if (ctx.options.strictMode) {
    throw new TransformationError(
      `Window expressions should be handled at the plan level, not expression level. ` +
      `This will be supported in Phase 3+.`,
      expr,
      { partitionBy: expr.partitionBy, orderBy: expr.orderBy }
    );
  }

  // Non-strict mode: return a placeholder scalar function
  return {
    kind: "ScalarFunction",
    fn: `__window_${expr.aggregate}__`,
    args: [transformExpr(expr.base, ctx)],
    resultType: DataTypes.number,
  };
}

/**
 * Handle Transform nodes.
 * In Phase 1, we throw or return a placeholder since transforms
 * should be handled at the plan level (Phase 3+).
 */
function handleTransformNode(
  expr: Extract<MetricExpr, { kind: "Transform" }>,
  ctx: TransformContext
): LogicalExpr {
  if (ctx.options.strictMode) {
    throw new TransformationError(
      `Transform expressions should be handled at the plan level, not expression level. ` +
      `This will be supported in Phase 3+.`,
      expr,
      { transformId: expr.transformId, transformKind: expr.transformKind }
    );
  }

  // Non-strict mode: return a placeholder scalar function
  return {
    kind: "ScalarFunction",
    fn: `__transform_${expr.transformId}__`,
    args: [transformExpr(expr.base, ctx)],
    resultType: DataTypes.number,
  };
}

// ---------------------------------------------------------------------------
// UTILITIES
// ---------------------------------------------------------------------------

/**
 * Collect all attribute references from a LogicalExpr tree.
 */
export function collectAttributeRefs(expr: LogicalExpr): LogicalAttributeRef[] {
  const refs: LogicalAttributeRef[] = [];

  function walk(e: LogicalExpr): void {
    switch (e.kind) {
      case "Constant":
        break;
      case "AttributeRef":
        refs.push(e);
        break;
      case "MetricRef":
        break;
      case "Aggregate":
        walk(e.input);
        if (e.filter) walk(e.filter);
        break;
      case "ScalarOp":
        walk(e.left);
        walk(e.right);
        break;
      case "ScalarFunction":
        e.args.forEach(walk);
        break;
      case "Conditional":
        walk(e.condition);
        walk(e.thenExpr);
        walk(e.elseExpr);
        break;
      case "Coalesce":
        e.exprs.forEach(walk);
        break;
      case "Comparison":
        walk(e.left);
        walk(e.right);
        break;
      case "LogicalOp":
        e.operands.forEach(walk);
        break;
      case "InList":
        walk(e.expr);
        break;
      case "Between":
        walk(e.expr);
        walk(e.low);
        walk(e.high);
        break;
      case "IsNull":
        walk(e.expr);
        break;
    }
  }

  walk(expr);
  return refs;
}

/**
 * Collect all metric references from a LogicalExpr tree.
 */
export function collectMetricRefs(expr: LogicalExpr): LogicalMetricRef[] {
  const refs: LogicalMetricRef[] = [];

  function walk(e: LogicalExpr): void {
    switch (e.kind) {
      case "Constant":
        break;
      case "AttributeRef":
        break;
      case "MetricRef":
        refs.push(e);
        break;
      case "Aggregate":
        walk(e.input);
        if (e.filter) walk(e.filter);
        break;
      case "ScalarOp":
        walk(e.left);
        walk(e.right);
        break;
      case "ScalarFunction":
        e.args.forEach(walk);
        break;
      case "Conditional":
        walk(e.condition);
        walk(e.thenExpr);
        walk(e.elseExpr);
        break;
      case "Coalesce":
        e.exprs.forEach(walk);
        break;
      case "Comparison":
        walk(e.left);
        walk(e.right);
        break;
      case "LogicalOp":
        e.operands.forEach(walk);
        break;
      case "InList":
        walk(e.expr);
        break;
      case "Between":
        walk(e.expr);
        walk(e.low);
        walk(e.high);
        break;
      case "IsNull":
        walk(e.expr);
        break;
    }
  }

  walk(expr);
  return refs;
}
