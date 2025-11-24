/**
 * Plan Visualizer Component
 *
 * Renders a LogicalQueryPlan as an interactive tree visualization.
 */

import { useState } from 'react'
import type {
  LogicalQueryPlan,
  LogicalPlanNode,
  PlanNodeId,
  LogicalExpr,
} from '../utils/logicalPlanBuilder'
import { formatExpr } from '../utils/logicalPlanBuilder'

interface PlanVisualizerProps {
  plan: LogicalQueryPlan
}

// Node kind to color mapping
const nodeColors: Record<LogicalPlanNode['kind'], string> = {
  FactScan: '#4a90d9',
  DimensionScan: '#5cb85c',
  Join: '#f0ad4e',
  Filter: '#d9534f',
  Aggregate: '#9b59b6',
  Project: '#3498db',
}

// Node kind to icon mapping
const nodeIcons: Record<LogicalPlanNode['kind'], string> = {
  FactScan: 'table',
  DimensionScan: 'cube',
  Join: 'link',
  Filter: 'filter',
  Aggregate: 'calculator',
  Project: 'list',
}

export function PlanVisualizer({ plan }: PlanVisualizerProps) {
  const [expandedNodes, setExpandedNodes] = useState<Set<PlanNodeId>>(new Set())
  const [selectedNode, setSelectedNode] = useState<PlanNodeId | null>(null)

  const toggleExpand = (nodeId: PlanNodeId) => {
    setExpandedNodes((prev) => {
      const next = new Set(prev)
      if (next.has(nodeId)) {
        next.delete(nodeId)
      } else {
        next.add(nodeId)
      }
      return next
    })
  }

  const renderNode = (nodeId: PlanNodeId, depth: number = 0): React.ReactNode => {
    const node = plan.nodes.get(nodeId)
    if (!node) return null

    const isExpanded = expandedNodes.has(nodeId)
    const isSelected = selectedNode === nodeId
    const color = nodeColors[node.kind]
    const hasChildren = 'inputId' in node || 'leftInputId' in node

    return (
      <div key={nodeId} className="plan-node-container" style={{ marginLeft: depth * 24 }}>
        <div
          className={`plan-node ${isSelected ? 'selected' : ''}`}
          style={{
            borderLeftColor: color,
            backgroundColor: isSelected ? `${color}20` : undefined,
          }}
          onClick={() => setSelectedNode(nodeId === selectedNode ? null : nodeId)}
        >
          <div className="plan-node-header">
            {hasChildren && (
              <button
                className="plan-node-toggle"
                onClick={(e) => {
                  e.stopPropagation()
                  toggleExpand(nodeId)
                }}
              >
                {isExpanded ? '[-]' : '[+]'}
              </button>
            )}
            <span className="plan-node-icon" style={{ color }}>
              {getIcon(node.kind)}
            </span>
            <span className="plan-node-kind">{node.kind}</span>
            <span className="plan-node-id">{nodeId}</span>
          </div>

          <div className="plan-node-details">
            {renderNodeDetails(node)}
          </div>
        </div>

        {isExpanded && (
          <div className="plan-node-children">
            {renderChildren(node, depth)}
          </div>
        )}
      </div>
    )
  }

  const renderChildren = (node: LogicalPlanNode, depth: number): React.ReactNode => {
    const children: React.ReactNode[] = []

    if ('inputId' in node && node.inputId) {
      children.push(renderNode(node.inputId, depth + 1))
    }

    if ('leftInputId' in node && node.leftInputId) {
      children.push(
        <div key={`${node.id}-left`} className="plan-child-label">Left Input:</div>,
        renderNode(node.leftInputId, depth + 1)
      )
    }

    if ('rightInputId' in node && node.rightInputId) {
      children.push(
        <div key={`${node.id}-right`} className="plan-child-label" style={{ marginTop: 8 }}>
          Right Input:
        </div>,
        renderNode(node.rightInputId, depth + 1)
      )
    }

    return children
  }

  const renderNodeDetails = (node: LogicalPlanNode): React.ReactNode => {
    switch (node.kind) {
      case 'FactScan':
        return (
          <>
            <div className="detail-row">
              <span className="detail-label">Table:</span>
              <span className="detail-value">{node.tableName}</span>
            </div>
            {node.columns.length > 0 && (
              <div className="detail-row">
                <span className="detail-label">Columns:</span>
                <span className="detail-value">{node.columns.join(', ')}</span>
              </div>
            )}
          </>
        )

      case 'DimensionScan':
        return (
          <>
            <div className="detail-row">
              <span className="detail-label">Table:</span>
              <span className="detail-value">{node.tableName}</span>
            </div>
            {node.columns.length > 0 && (
              <div className="detail-row">
                <span className="detail-label">Columns:</span>
                <span className="detail-value">{node.columns.join(', ')}</span>
              </div>
            )}
          </>
        )

      case 'Join':
        return (
          <>
            <div className="detail-row">
              <span className="detail-label">Type:</span>
              <span className="detail-value">{node.joinType.toUpperCase()}</span>
            </div>
            <div className="detail-row">
              <span className="detail-label">On:</span>
              <span className="detail-value">
                {node.leftKey} = {node.rightKey}
              </span>
            </div>
          </>
        )

      case 'Filter':
        return (
          <div className="detail-row">
            <span className="detail-label">Predicate:</span>
            <span className="detail-value predicate">{formatExpr(node.predicate)}</span>
          </div>
        )

      case 'Aggregate':
        return (
          <>
            <div className="detail-row">
              <span className="detail-label">Group By:</span>
              <span className="detail-value">{node.groupBy.join(', ') || '(none)'}</span>
            </div>
            {node.aggregates.map((agg, i) => (
              <div key={i} className="detail-row">
                <span className="detail-label">{agg.outputName}:</span>
                <span className="detail-value aggregate">
                  {agg.op}({agg.input})
                </span>
              </div>
            ))}
          </>
        )

      case 'Project':
        return (
          <div className="detail-row">
            <span className="detail-label">Outputs:</span>
            <span className="detail-value">{node.outputs.join(', ')}</span>
          </div>
        )

      default:
        return null
    }
  }

  // Start with root expanded
  if (!expandedNodes.has(plan.rootNodeId) && expandedNodes.size === 0) {
    setExpandedNodes(new Set([plan.rootNodeId]))
  }

  return (
    <div className="plan-visualizer">
      <div className="plan-header">
        <h4>Logical Query Plan</h4>
        <div className="plan-legend">
          {Object.entries(nodeColors).map(([kind, color]) => (
            <div key={kind} className="legend-item">
              <span className="legend-color" style={{ backgroundColor: color }} />
              <span className="legend-label">{kind}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="plan-tree">
        {renderNode(plan.rootNodeId)}
      </div>

      {plan.outputMetrics.length > 0 && (
        <div className="plan-metrics">
          <h5>Metric Evaluation Order</h5>
          <div className="metrics-list">
            {plan.metricEvalOrder.map((metricName, index) => {
              const metric = plan.outputMetrics.find((m) => m.name === metricName)
              return (
                <div key={metricName} className="metric-item">
                  <span className="metric-phase">Phase {metric?.executionPhase ?? index}</span>
                  <span className="metric-name">{metricName}</span>
                  {metric && (
                    <span className="metric-expr">{formatExpr(metric.expr)}</span>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

function getIcon(kind: LogicalPlanNode['kind']): string {
  // Using simple ASCII icons for now
  switch (kind) {
    case 'FactScan':
      return 'F'
    case 'DimensionScan':
      return 'D'
    case 'Join':
      return 'J'
    case 'Filter':
      return 'W'
    case 'Aggregate':
      return 'A'
    case 'Project':
      return 'P'
    default:
      return '?'
  }
}

// Also export a simple text view for fallback
export function PlanTextView({ plan }: PlanVisualizerProps) {
  const lines: string[] = []
  const visited = new Set<PlanNodeId>()

  function formatNode(nodeId: PlanNodeId, indent: number): void {
    if (visited.has(nodeId)) {
      lines.push(' '.repeat(indent) + `[${nodeId}] (already shown)`)
      return
    }
    visited.add(nodeId)

    const node = plan.nodes.get(nodeId)
    if (!node) return

    const prefix = ' '.repeat(indent)

    switch (node.kind) {
      case 'FactScan':
        lines.push(`${prefix}[${node.id}] FactScan: ${node.tableName}`)
        if (node.columns.length > 0) {
          lines.push(`${prefix}  columns: ${node.columns.join(', ')}`)
        }
        break

      case 'DimensionScan':
        lines.push(`${prefix}[${node.id}] DimensionScan: ${node.tableName}`)
        if (node.columns.length > 0) {
          lines.push(`${prefix}  columns: ${node.columns.join(', ')}`)
        }
        break

      case 'Join':
        lines.push(`${prefix}[${node.id}] ${node.joinType.toUpperCase()} JOIN`)
        lines.push(`${prefix}  on: ${node.leftKey} = ${node.rightKey}`)
        formatNode(node.leftInputId, indent + 2)
        formatNode(node.rightInputId, indent + 2)
        break

      case 'Filter':
        lines.push(`${prefix}[${node.id}] Filter`)
        lines.push(`${prefix}  predicate: ${formatExpr(node.predicate)}`)
        formatNode(node.inputId, indent + 2)
        break

      case 'Aggregate':
        lines.push(`${prefix}[${node.id}] Aggregate`)
        lines.push(`${prefix}  group by: ${node.groupBy.join(', ') || '(none)'}`)
        node.aggregates.forEach((agg) => {
          lines.push(`${prefix}  ${agg.outputName}: ${agg.op}(${agg.input})`)
        })
        formatNode(node.inputId, indent + 2)
        break

      case 'Project':
        lines.push(`${prefix}[${node.id}] Project`)
        lines.push(`${prefix}  outputs: ${node.outputs.join(', ')}`)
        formatNode(node.inputId, indent + 2)
        break
    }
  }

  formatNode(plan.rootNodeId, 0)

  return (
    <div className="plan-text-view">
      <pre>{lines.join('\n')}</pre>
    </div>
  )
}
