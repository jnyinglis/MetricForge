// Legacy compatibility shim.
// Logical plan construction now uses core planner APIs through coreBridge.

import {
  explainPlan,
  formatLogicalExpr,
  type LogicalExpr,
  type LogicalPlanNode,
  type LogicalQueryPlan,
  type PlanNodeId,
} from '../../../src/browser'
import { buildWorkspacePlan } from './coreBridge'
import type { MetricDefinition, SchemaDefinition } from '../types/workspace'

export type { LogicalExpr, LogicalPlanNode, LogicalQueryPlan, PlanNodeId }

interface QuerySpecAst {
  dimensions: string[]
  metrics: string[]
}

export function buildLogicalPlan(
  querySpec: QuerySpecAst,
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): LogicalQueryPlan {
  const queryDsl = `query compatibility_query {
  dimensions: ${querySpec.dimensions.join(', ')}
  metrics: ${querySpec.metrics.join(', ')}
}`
  const { plan, errors } = buildWorkspacePlan(queryDsl, schema, metrics)
  if (!plan || errors.length > 0) {
    throw new Error(errors[0]?.message ?? 'Failed to build logical plan')
  }
  return plan
}

export function formatLogicalPlan(plan: LogicalQueryPlan): string {
  return explainPlan(plan, {
    verbose: true,
    showExpressions: true,
  })
}

export { formatLogicalExpr }
