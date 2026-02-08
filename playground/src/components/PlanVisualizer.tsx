import { useMemo } from 'react'
import {
  explainPlan,
  formatLogicalExpr,
  type LogicalPlanNode,
  type LogicalQueryPlan,
} from '../../../src/browser'

interface PlanVisualizerProps {
  plan: LogicalQueryPlan
}

function describeNode(node: LogicalPlanNode): string {
  switch (node.kind) {
    case 'FactScan':
      return `${node.kind} ${node.tableName} [${node.requiredColumns
        .map((column) => column.attributeId)
        .join(', ')}]`
    case 'DimensionScan':
      return `${node.kind} ${node.tableName} [${node.requiredColumns
        .map((column) => column.attributeId)
        .join(', ')}]`
    case 'Join':
      return `${node.kind} ${node.joinType.toUpperCase()} on ${node.joinKeys
        .map((key) => `${key.leftAttr.attributeId}=${key.rightAttr.attributeId}`)
        .join(', ')}`
    case 'Filter':
      return `${node.kind} ${formatLogicalExpr(node.predicate)}`
    case 'Aggregate':
      return `${node.kind} groupBy(${node.groupBy
        .map((column) => column.attributeId)
        .join(', ')})`
    case 'Window':
      return `${node.kind} frame(${JSON.stringify(node.frame)})`
    case 'Transform':
      return `${node.kind} ${node.transformKind}:${node.transformId}`
    case 'Project':
      return `${node.kind} [${node.outputs.map((output) => output.name).join(', ')}]`
  }
}

export function PlanVisualizer({ plan }: PlanVisualizerProps) {
  const explainText = useMemo(
    () =>
      explainPlan(plan, {
        verbose: true,
        showExpressions: true,
      }),
    [plan]
  )

  const nodeSummaries = useMemo(
    () =>
      Array.from(plan.nodes.values()).map((node) => ({
        id: node.id,
        text: describeNode(node),
      })),
    [plan]
  )

  return (
    <div className="plan-visualizer">
      <div style={{ marginBottom: 12 }}>
        <h4 style={{ marginBottom: 8 }}>Core Logical Plan</h4>
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
          Root node: {plan.rootNodeId} | Nodes: {plan.nodes.size}
        </div>
      </div>

      <div style={{ marginBottom: 12 }}>
        {nodeSummaries.map((node) => (
          <div key={node.id} className="detail-row">
            <span className="detail-label">{node.id}</span>
            <span className="detail-value">{node.text}</span>
          </div>
        ))}
      </div>

      <div className="plan-text-view">
        <pre>{explainText}</pre>
      </div>
    </div>
  )
}

export function PlanTextView({ plan }: PlanVisualizerProps) {
  const explainText = useMemo(
    () =>
      explainPlan(plan, {
        verbose: true,
        showExpressions: true,
      }),
    [plan]
  )

  return (
    <div className="plan-text-view">
      <pre>{explainText}</pre>
    </div>
  )
}
