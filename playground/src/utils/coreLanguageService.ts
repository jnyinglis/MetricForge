import {
  getDslCompletions as getCoreDslCompletions,
  parseDslWithDiagnostics,
  parseMetricExprWithDiagnostics,
  type DslCompletionContext,
  type DslCompletionItem,
  type DslDiagnostic,
  type DslFileAst,
  type MetricExpr,
} from '../../../src/browser'
import type { ParseError } from '../types/workspace'

export interface DslParseResult {
  ast: DslFileAst | null
  errors: ParseError[]
}

export interface MetricExprParseResult {
  expr: MetricExpr | null
  errors: ParseError[]
}

function mapDiagnostics(errors: DslDiagnostic[]): ParseError[] {
  return errors.map((error) => ({
    message: error.message,
    line: error.line,
    column: error.column,
    severity: error.severity,
  }))
}

export function parseDsl(text: string): DslParseResult {
  const result = parseDslWithDiagnostics(text)
  return { ast: result.ast, errors: mapDiagnostics(result.errors) }
}

export function parseMetricExpression(text: string): MetricExprParseResult {
  const result = parseMetricExprWithDiagnostics(text)
  return { expr: result.expr, errors: mapDiagnostics(result.errors) }
}

export function getDslCompletions(
  context: DslCompletionContext,
  _position?: { line: number; column: number },
  _text?: string
): DslCompletionItem[] {
  return getCoreDslCompletions(context)
}
