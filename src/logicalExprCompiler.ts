import type { LogicalAggregate } from "./logicalAst";
import {
  LogicalExpr,
  LogicalConstant,
  LogicalAttributeRef,
  LogicalMetricRef,
  LogicalScalarOp,
  LogicalScalarFunction,
  LogicalConditional,
  LogicalCoalesce,
  LogicalComparison,
  LogicalLogicalOp,
  LogicalInList,
  LogicalBetween,
  LogicalIsNull,
} from "./logicalAst";

// ---------------------------------------------------------------------------
// COMPILE LOGICAL EXPR (Phase 5)
// ---------------------------------------------------------------------------

/**
 * Context for evaluating a compiled LogicalExpr.
 * This provides the runtime environment for expression evaluation.
 */
export interface LogicalExprEvalContext {
  /** Current row being processed */
  row: Record<string, unknown>;
  /** Function to get a metric value by name */
  getMetric: (name: string) => number | undefined;
  /** Function to get an attribute value by id */
  getAttribute: (id: string) => unknown;
  /** Optional handler for aggregate expressions */
  aggregate?: (expr: LogicalAggregate, ctx: LogicalExprEvalContext) => unknown;
}

/**
 * A compiled evaluator function for a LogicalExpr.
 * Takes a context and returns the computed value.
 */
export type CompiledLogicalExpr = (ctx: LogicalExprEvalContext) => unknown;

/**
 * Compile a LogicalExpr into an executable evaluator function.
 * This is the bridge between the logical plan and runtime execution.
 *
 * @param expr - The LogicalExpr to compile
 * @returns A function that evaluates the expression given a context
 */
export function compileLogicalExpr(expr: LogicalExpr): CompiledLogicalExpr {
  switch (expr.kind) {
    case "Constant":
      return () => (expr as LogicalConstant).value;

    case "AttributeRef":
      return (ctx) => ctx.getAttribute((expr as LogicalAttributeRef).attributeId);

    case "MetricRef":
      return (ctx) => ctx.getMetric((expr as LogicalMetricRef).metricName);

    case "Aggregate": {
      const aggExpr = expr as LogicalAggregate;
      const inputFn = compileLogicalExpr(aggExpr.input);
      return (ctx) => {
        if (ctx.aggregate) {
          return ctx.aggregate(aggExpr, ctx);
        }
        return inputFn(ctx);
      };
    }

    case "ScalarOp": {
      const scalarExpr = expr as LogicalScalarOp;
      const leftFn = compileLogicalExpr(scalarExpr.left);
      const rightFn = compileLogicalExpr(scalarExpr.right);
      return (ctx) => {
        const l = Number(leftFn(ctx));
        const r = Number(rightFn(ctx));
        switch (scalarExpr.op) {
          case "+": return l + r;
          case "-": return l - r;
          case "*": return l * r;
          case "/": return r !== 0 ? l / r : undefined;
          case "%": return r !== 0 ? l % r : undefined;
          default: return undefined;
        }
      };
    }

    case "ScalarFunction": {
      const scalarExpr = expr as LogicalScalarFunction;
      const argFns = scalarExpr.args.map(compileLogicalExpr);
      return (ctx) => {
        const args = argFns.map((fn) => fn(ctx));
        const fn = scalarExpr.fn.toLowerCase();
        switch (fn) {
          case "abs": return Math.abs(Number(args[0]));
          case "round": return Math.round(Number(args[0]));
          case "floor": return Math.floor(Number(args[0]));
          case "ceil": return Math.ceil(Number(args[0]));
          case "sqrt": return Math.sqrt(Number(args[0]));
          case "power": return Math.pow(Number(args[0]), Number(args[1]));
          case "log": return Math.log(Number(args[0]));
          case "exp": return Math.exp(Number(args[0]));
          case "upper": return String(args[0]).toUpperCase();
          case "lower": return String(args[0]).toLowerCase();
          case "length": return String(args[0]).length;
          case "substring": return String(args[0]).substring(Number(args[1]), Number(args[2]));
          case "concat": return args.map(String).join("");
          case "trim": return String(args[0]).trim();
          case "coalesce": return args.find((a) => a != null);
          case "nullif": return args[0] === args[1] ? null : args[0];
          case "ifnull": return args[0] ?? args[1];
          default: return undefined;
        }
      };
    }

    case "Conditional": {
      const condExpr = expr as LogicalConditional;
      const condFn = compileLogicalExpr(condExpr.condition);
      const thenFn = compileLogicalExpr(condExpr.thenExpr);
      const elseFn = compileLogicalExpr(condExpr.elseExpr);
      return (ctx) => {
        const cond = condFn(ctx);
        return cond ? thenFn(ctx) : elseFn(ctx);
      };
    }

    case "Coalesce": {
      const coalesceExpr = expr as LogicalCoalesce;
      const exprFns = coalesceExpr.exprs.map(compileLogicalExpr);
      return (ctx) => {
        for (const fn of exprFns) {
          const val = fn(ctx);
          if (val != null) return val;
        }
        return null;
      };
    }

    case "Comparison": {
      const comparisonExpr = expr as LogicalComparison;
      const leftFn = compileLogicalExpr(comparisonExpr.left);
      const rightFn = compileLogicalExpr(comparisonExpr.right);
      return (ctx) => {
        const l = leftFn(ctx);
        const r = rightFn(ctx);
        switch (comparisonExpr.op) {
          case "=": return l === r;
          case "!=": return l !== r;
          case "<": return (l as number) < (r as number);
          case "<=": return (l as number) <= (r as number);
          case ">": return (l as number) > (r as number);
          case ">=": return (l as number) >= (r as number);
          default: return false;
        }
      };
    }

    case "LogicalOp": {
      const logicalExpr = expr as LogicalLogicalOp;
      const operandFns = logicalExpr.operands.map(compileLogicalExpr);
      return (ctx) => {
        switch (logicalExpr.op) {
          case "and":
            return operandFns.every((fn) => fn(ctx));
          case "or":
            return operandFns.some((fn) => fn(ctx));
          case "not":
            return !operandFns[0](ctx);
          default:
            return false;
        }
      };
    }

    case "InList": {
      const inListExpr = expr as LogicalInList;
      const exprFn = compileLogicalExpr(inListExpr.expr);
      const valueSet = new Set(inListExpr.values.map((v) => v.value));
      return (ctx) => {
        const val = exprFn(ctx);
        const inList = valueSet.has(val as string | number | boolean);
        return inListExpr.negated ? !inList : inList;
      };
    }

    case "Between": {
      const betweenExpr = expr as LogicalBetween;
      const exprFn = compileLogicalExpr(betweenExpr.expr);
      const lowFn = compileLogicalExpr(betweenExpr.low);
      const highFn = compileLogicalExpr(betweenExpr.high);
      return (ctx) => {
        const val = exprFn(ctx) as number;
        const low = lowFn(ctx) as number;
        const high = highFn(ctx) as number;
        return val >= low && val <= high;
      };
    }

    case "IsNull": {
      const isNullExpr = expr as LogicalIsNull;
      const exprFn = compileLogicalExpr(isNullExpr.expr);
      return (ctx) => {
        const val = exprFn(ctx);
        const isNull = val === null || val === undefined;
        return isNullExpr.negated ? !isNull : isNull;
      };
    }

    default:
      return () => undefined;
  }
}

// ---------------------------------------------------------------------------
// COMPILE LOGICAL EXPR TO SQL
// ---------------------------------------------------------------------------

/**
 * Compile a LogicalExpr to a SQL fragment string.
 * This is useful for generating SQL queries from logical plans.
 *
 * @param expr - The LogicalExpr to compile
 * @param aliasMap - Optional map of attribute IDs to SQL column names
 * @returns SQL fragment string
 */
export function compileLogicalExprToSql(
  expr: LogicalExpr,
  aliasMap: Map<string, string> = new Map()
): string {
  const col = (attrId: string) => aliasMap.get(attrId) ?? attrId;

  switch (expr.kind) {
    case "Constant": {
      const value = (expr as LogicalConstant).value;
      if (typeof value === "string") {
        return `'${value.replace(/'/g, "''")}'`;
      }
      if (value === null) return "NULL";
      if (typeof value === "boolean") {
        return value ? "TRUE" : "FALSE";
      }
      return String(value);
    }

    case "AttributeRef":
      return col((expr as LogicalAttributeRef).attributeId);

    case "MetricRef":
      // Metrics are referenced by name in SQL (typically as computed columns)
      return `"${(expr as LogicalMetricRef).metricName}"`;

    case "Aggregate":
      return `${(expr as LogicalAggregate).op.toUpperCase()}(${compileLogicalExprToSql((expr as LogicalAggregate).input, aliasMap)})`;

    case "ScalarOp":
      return `(${compileLogicalExprToSql((expr as LogicalScalarOp).left, aliasMap)} ${(expr as LogicalScalarOp).op} ${compileLogicalExprToSql((expr as LogicalScalarOp).right, aliasMap)})`;

    case "ScalarFunction":
      return `${(expr as LogicalScalarFunction).fn.toUpperCase()}(${(expr as LogicalScalarFunction).args.map((a) => compileLogicalExprToSql(a, aliasMap)).join(", ")})`;

    case "Conditional":
      return `CASE WHEN ${compileLogicalExprToSql((expr as LogicalConditional).condition, aliasMap)} THEN ${compileLogicalExprToSql((expr as LogicalConditional).thenExpr, aliasMap)} ELSE ${compileLogicalExprToSql((expr as LogicalConditional).elseExpr, aliasMap)} END`;

    case "Coalesce":
      return `COALESCE(${(expr as LogicalCoalesce).exprs.map((e) => compileLogicalExprToSql(e, aliasMap)).join(", ")})`;

    case "Comparison": {
      const comparisonExpr = expr as LogicalComparison;
      const sqlOp = comparisonExpr.op === "!=" ? "<>" : comparisonExpr.op;
      return `(${compileLogicalExprToSql(comparisonExpr.left, aliasMap)} ${sqlOp} ${compileLogicalExprToSql(comparisonExpr.right, aliasMap)})`;
    }

    case "LogicalOp": {
      const logicalExpr = expr as LogicalLogicalOp;
      if (logicalExpr.op === "not") {
        return `NOT (${compileLogicalExprToSql(logicalExpr.operands[0], aliasMap)})`;
      }
      return `(${logicalExpr.operands.map((o) => compileLogicalExprToSql(o, aliasMap)).join(` ${logicalExpr.op.toUpperCase()} `)})`;
    }

    case "InList": {
      const inListExpr = expr as LogicalInList;
      const valuesStr = inListExpr.values
        .map((v) => {
          if (typeof v.value === "string") return `'${v.value.replace(/'/g, "''")}'`;
          return String(v.value);
        })
        .join(", ");
      const notStr = inListExpr.negated ? " NOT" : "";
      return `(${compileLogicalExprToSql(inListExpr.expr, aliasMap)}${notStr} IN (${valuesStr}))`;
    }

    case "Between": {
      const betweenExpr = expr as LogicalBetween;
      return `(${compileLogicalExprToSql(betweenExpr.expr, aliasMap)} BETWEEN ${compileLogicalExprToSql(betweenExpr.low, aliasMap)} AND ${compileLogicalExprToSql(betweenExpr.high, aliasMap)})`;
    }

    case "IsNull": {
      const isNullExpr = expr as LogicalIsNull;
      const nullOp = isNullExpr.negated ? "IS NOT NULL" : "IS NULL";
      return `(${compileLogicalExprToSql(isNullExpr.expr, aliasMap)} ${nullOp})`;
    }

    default:
      return "NULL";
  }
}
