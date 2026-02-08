// Legacy compatibility shim.
// The playground now uses core language-service APIs directly.

import {
  getDslCompletions as getCoreDslCompletions,
  parseDslWithDiagnostics,
  parseMetricExprWithDiagnostics,
  type DslCompletionContext,
  type DslCompletionItem,
  type DslDiagnostic,
  type DslFileAst,
  type FilterNode,
  type MetricExpr,
  type MetricHavingAst,
  type QuerySpecV2,
} from '../../../src/browser'
import type { ParseError } from '../types/workspace'

export type { MetricExpr, MetricHavingAst, DslFileAst, FilterNode }
export type QuerySpecAst = QuerySpecV2

function mapDiagnostics(errors: DslDiagnostic[]): ParseError[] {
  return errors.map((error) => ({
    message: error.message,
    line: error.line,
    column: error.column,
    severity: error.severity,
  }))
}

export function parseDsl(text: string): { ast: DslFileAst | null; errors: ParseError[] } {
  const result = parseDslWithDiagnostics(text)
  return { ast: result.ast, errors: mapDiagnostics(result.errors) }
}

export function parseMetricExpression(text: string): {
  expr: MetricExpr | null
  errors: ParseError[]
} {
  const result = parseMetricExprWithDiagnostics(text)
  return { expr: result.expr, errors: mapDiagnostics(result.errors) }
}

export function validateMetricExpr(
  expr: MetricExpr,
  knownAttributes: Set<string>,
  knownMetrics: Set<string>
): ParseError[] {
  const errors: ParseError[] = []

  const walk = (node: MetricExpr): void => {
    switch (node.kind) {
      case 'Literal':
        return
      case 'AttrRef':
        if (node.name !== '*' && !knownAttributes.has(node.name) && !knownMetrics.has(node.name)) {
          errors.push({
            message: `Unknown attribute or metric: ${node.name}`,
            severity: 'warning',
          })
        }
        return
      case 'MetricRef':
        if (!knownMetrics.has(node.name)) {
          errors.push({
            message: `Unknown metric: ${node.name}`,
            severity: 'error',
          })
        }
        return
      case 'Call':
        node.args.forEach(walk)
        return
      case 'BinaryOp':
        walk(node.left)
        walk(node.right)
        return
      case 'Window':
        walk(node.base)
        return
      case 'Transform':
        walk(node.base)
        return
    }
  }

  walk(expr)
  return errors
}

export function getDslCompletions(
  context: DslCompletionContext,
  _position: { line: number; column: number },
  _text: string
): DslCompletionItem[] {
  return getCoreDslCompletions(context)
}
