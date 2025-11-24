/**
 * Logical Plan Builder for Playground
 *
 * Browser-compatible version of the logical plan types and builder.
 * Creates a visual representation of the query execution plan.
 */

import type { QuerySpecAst, MetricExpr, FilterNode, MetricHavingAst } from './parserAdapter'
import type { SchemaDefinition, MetricDefinition } from '../types/workspace'

// ---------------------------------------------------------------------------
// DATA TYPE SYSTEM
// ---------------------------------------------------------------------------

export type DataType =
  | { kind: 'number' }
  | { kind: 'string' }
  | { kind: 'boolean' }
  | { kind: 'date' }
  | { kind: 'unknown' }

// ---------------------------------------------------------------------------
// LOGICAL EXPRESSION IR
// ---------------------------------------------------------------------------

export type LogicalExpr =
  | LogicalConstant
  | LogicalAttributeRef
  | LogicalMetricRef
  | LogicalAggregate
  | LogicalScalarOp
  | LogicalComparison
  | LogicalLogicalOp

export interface LogicalConstant {
  kind: 'Constant'
  value: number | string | boolean | null
  dataType: DataType
}

export interface LogicalAttributeRef {
  kind: 'AttributeRef'
  attributeId: string
  physicalTable: string
  physicalColumn: string
  dataType: DataType
  sourceKind: 'fact' | 'dimension'
}

export interface LogicalMetricRef {
  kind: 'MetricRef'
  metricName: string
  baseFact: string | null
  resultType: DataType
}

export interface LogicalAggregate {
  kind: 'Aggregate'
  op: 'sum' | 'avg' | 'count' | 'min' | 'max' | 'count_distinct'
  input: LogicalExpr
  distinct: boolean
  resultType: DataType
}

export interface LogicalScalarOp {
  kind: 'ScalarOp'
  op: '+' | '-' | '*' | '/'
  left: LogicalExpr
  right: LogicalExpr
  resultType: DataType
}

export interface LogicalComparison {
  kind: 'Comparison'
  left: LogicalExpr
  op: '=' | '!=' | '<' | '<=' | '>' | '>='
  right: LogicalExpr
  resultType: { kind: 'boolean' }
}

export interface LogicalLogicalOp {
  kind: 'LogicalOp'
  op: 'and' | 'or' | 'not'
  operands: LogicalExpr[]
  resultType: { kind: 'boolean' }
}

// ---------------------------------------------------------------------------
// LOGICAL PLAN NODE TYPES
// ---------------------------------------------------------------------------

export type PlanNodeId = string

export interface BasePlanNode {
  id: PlanNodeId
  annotations?: Record<string, unknown>
}

export type LogicalPlanNode =
  | FactScanNode
  | DimensionScanNode
  | JoinNode
  | FilterNode_
  | AggregateNode
  | ProjectNode

export interface FactScanNode extends BasePlanNode {
  kind: 'FactScan'
  tableName: string
  columns: string[]
}

export interface DimensionScanNode extends BasePlanNode {
  kind: 'DimensionScan'
  tableName: string
  columns: string[]
}

export interface JoinNode extends BasePlanNode {
  kind: 'Join'
  joinType: 'inner' | 'left'
  leftInputId: PlanNodeId
  rightInputId: PlanNodeId
  leftKey: string
  rightKey: string
}

export interface FilterNode_ extends BasePlanNode {
  kind: 'Filter'
  inputId: PlanNodeId
  predicate: LogicalExpr
}

export interface AggregateNode extends BasePlanNode {
  kind: 'Aggregate'
  inputId: PlanNodeId
  groupBy: string[]
  aggregates: Array<{
    outputName: string
    op: string
    input: string
  }>
}

export interface ProjectNode extends BasePlanNode {
  kind: 'Project'
  inputId: PlanNodeId
  outputs: string[]
}

// ---------------------------------------------------------------------------
// LOGICAL QUERY PLAN
// ---------------------------------------------------------------------------

export interface LogicalMetricPlan {
  name: string
  expr: LogicalExpr
  baseFact: string | null
  dependencies: string[]
  executionPhase: number
}

export interface LogicalQueryPlan {
  rootNodeId: PlanNodeId
  nodes: Map<PlanNodeId, LogicalPlanNode>
  outputDimensions: string[]
  outputMetrics: LogicalMetricPlan[]
  metricEvalOrder: string[]
}

// ---------------------------------------------------------------------------
// PLAN BUILDER
// ---------------------------------------------------------------------------

let nodeIdCounter = 0

function generateNodeId(prefix: string): PlanNodeId {
  return `${prefix}_${++nodeIdCounter}`
}

export function resetNodeIdCounter(): void {
  nodeIdCounter = 0
}

/**
 * Convert MetricExpr to LogicalExpr
 */
function convertExpr(expr: MetricExpr, schema: SchemaDefinition): LogicalExpr {
  switch (expr.kind) {
    case 'Literal':
      return {
        kind: 'Constant',
        value: expr.value,
        dataType: { kind: 'number' },
      }

    case 'AttrRef': {
      const attr = schema.attributes.find((a) => a.name === expr.name)
      if (attr) {
        const isFact = schema.facts.some((f) => f.table === attr.table)
        return {
          kind: 'AttributeRef',
          attributeId: expr.name,
          physicalTable: attr.table,
          physicalColumn: attr.column,
          dataType: { kind: 'unknown' },
          sourceKind: isFact ? 'fact' : 'dimension',
        }
      }
      // Fallback for * or unknown
      return {
        kind: 'AttributeRef',
        attributeId: expr.name,
        physicalTable: '',
        physicalColumn: expr.name,
        dataType: { kind: 'unknown' },
        sourceKind: 'fact',
      }
    }

    case 'MetricRef':
      return {
        kind: 'MetricRef',
        metricName: expr.name,
        baseFact: null,
        resultType: { kind: 'number' },
      }

    case 'Call': {
      const fnLower = expr.fn.toLowerCase()
      const ops = ['sum', 'avg', 'count', 'min', 'max']
      if (ops.includes(fnLower)) {
        const input =
          expr.args.length > 0 ? convertExpr(expr.args[0], schema) : { kind: 'Constant' as const, value: 1, dataType: { kind: 'number' as const } }
        return {
          kind: 'Aggregate',
          op: fnLower as 'sum' | 'avg' | 'count' | 'min' | 'max',
          input,
          distinct: false,
          resultType: { kind: 'number' },
        }
      }
      // For other functions, return as metric ref for now
      return {
        kind: 'MetricRef',
        metricName: expr.fn,
        baseFact: null,
        resultType: { kind: 'number' },
      }
    }

    case 'BinaryOp':
      return {
        kind: 'ScalarOp',
        op: expr.op,
        left: convertExpr(expr.left, schema),
        right: convertExpr(expr.right, schema),
        resultType: { kind: 'number' },
      }
  }
}

/**
 * Convert FilterNode to LogicalExpr
 */
function convertFilter(filter: FilterNode, schema: SchemaDefinition): LogicalExpr {
  switch (filter.kind) {
    case 'expression': {
      const attr = schema.attributes.find((a) => a.name === filter.field)
      const isFact = attr ? schema.facts.some((f) => f.table === attr.table) : false

      const left: LogicalAttributeRef = {
        kind: 'AttributeRef',
        attributeId: filter.field,
        physicalTable: attr?.table || '',
        physicalColumn: attr?.column || filter.field,
        dataType: { kind: 'unknown' },
        sourceKind: isFact ? 'fact' : 'dimension',
      }

      const right: LogicalConstant = {
        kind: 'Constant',
        value: filter.value as number | string | boolean | null,
        dataType:
          typeof filter.value === 'number'
            ? { kind: 'number' }
            : typeof filter.value === 'string'
              ? { kind: 'string' }
              : typeof filter.value === 'boolean'
                ? { kind: 'boolean' }
                : { kind: 'unknown' },
      }

      const opMap: Record<string, '=' | '!=' | '<' | '<=' | '>' | '>='> = {
        '==': '=',
        eq: '=',
        '!=': '!=',
        '>': '>',
        gt: '>',
        '>=': '>=',
        gte: '>=',
        '<': '<',
        lt: '<',
        '<=': '<=',
        lte: '<=',
      }

      return {
        kind: 'Comparison',
        left,
        op: opMap[filter.op] || '=',
        right,
        resultType: { kind: 'boolean' },
      }
    }

    case 'and':
      return {
        kind: 'LogicalOp',
        op: 'and',
        operands: filter.filters.map((f) => convertFilter(f, schema)),
        resultType: { kind: 'boolean' },
      }

    case 'or':
      return {
        kind: 'LogicalOp',
        op: 'or',
        operands: filter.filters.map((f) => convertFilter(f, schema)),
        resultType: { kind: 'boolean' },
      }
  }
}

/**
 * Build a logical plan from a query spec
 */
export function buildLogicalPlan(
  querySpec: QuerySpecAst,
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): LogicalQueryPlan {
  resetNodeIdCounter()

  const nodes = new Map<PlanNodeId, LogicalPlanNode>()

  // Find base fact table
  let baseFact = schema.facts[0]
  const baseTable = baseFact?.table || ''

  // Determine which dimensions are needed
  const dimensionTables = new Set<string>()
  for (const dimName of querySpec.dimensions) {
    const attr = schema.attributes.find((a) => a.name === dimName)
    if (attr) {
      const dim = schema.dimensions.find((d) => d.table === attr.table)
      if (dim) {
        dimensionTables.add(dim.table)
      }
    }
  }

  // Create fact scan node
  const factScanId = generateNodeId('fact_scan')
  const factColumns: string[] = []

  // Add metric-related columns from fact table
  for (const metricName of querySpec.metrics) {
    const metric = metrics.find((m) => m.name === metricName)
    if (metric) {
      // Simple column extraction from DSL
      const colMatch = metric.dsl.match(/\((\w+)\)/)
      if (colMatch && colMatch[1] !== '*') {
        factColumns.push(colMatch[1])
      }
    }
  }

  // Add join keys
  for (const join of schema.joins) {
    if (dimensionTables.has(schema.dimensions.find((d) => d.name === join.dimension)?.table || '')) {
      factColumns.push(join.factKey)
    }
  }

  nodes.set(factScanId, {
    id: factScanId,
    kind: 'FactScan',
    tableName: baseTable,
    columns: [...new Set(factColumns)],
  })

  let currentInputId = factScanId

  // Create dimension scan and join nodes
  for (const dimTable of dimensionTables) {
    const dimDef = schema.dimensions.find((d) => d.table === dimTable)
    if (!dimDef) continue

    const join = schema.joins.find((j) => j.dimension === dimDef.name)
    if (!join) continue

    const dimScanId = generateNodeId('dim_scan')
    const dimColumns: string[] = []

    // Find columns needed from this dimension
    for (const dimName of querySpec.dimensions) {
      const attr = schema.attributes.find((a) => a.name === dimName && a.table === dimTable)
      if (attr) {
        dimColumns.push(attr.column)
      }
    }
    dimColumns.push(join.dimensionKey)

    nodes.set(dimScanId, {
      id: dimScanId,
      kind: 'DimensionScan',
      tableName: dimTable,
      columns: [...new Set(dimColumns)],
    })

    // Create join node
    const joinId = generateNodeId('join')
    nodes.set(joinId, {
      id: joinId,
      kind: 'Join',
      joinType: 'inner',
      leftInputId: currentInputId,
      rightInputId: dimScanId,
      leftKey: join.factKey,
      rightKey: join.dimensionKey,
    })

    currentInputId = joinId
  }

  // Add filter node if where clause exists
  if (querySpec.where) {
    const filterId = generateNodeId('filter')
    nodes.set(filterId, {
      id: filterId,
      kind: 'Filter',
      inputId: currentInputId,
      predicate: convertFilter(querySpec.where, schema),
    })
    currentInputId = filterId
  }

  // Create aggregate node
  const aggregates: AggregateNode['aggregates'] = []
  for (const metricName of querySpec.metrics) {
    const metric = metrics.find((m) => m.name === metricName)
    if (metric) {
      // Extract aggregation info from DSL
      const aggMatch = metric.dsl.match(/(\w+)\s*\(\s*(\*|\w+)\s*\)/)
      if (aggMatch) {
        aggregates.push({
          outputName: metricName,
          op: aggMatch[1].toLowerCase(),
          input: aggMatch[2],
        })
      }
    }
  }

  const aggregateId = generateNodeId('aggregate')
  nodes.set(aggregateId, {
    id: aggregateId,
    kind: 'Aggregate',
    inputId: currentInputId,
    groupBy: querySpec.dimensions,
    aggregates,
  })
  currentInputId = aggregateId

  // Create project node
  const projectId = generateNodeId('project')
  nodes.set(projectId, {
    id: projectId,
    kind: 'Project',
    inputId: currentInputId,
    outputs: [...querySpec.dimensions, ...querySpec.metrics],
  })

  // Build metric plans
  const outputMetrics: LogicalMetricPlan[] = querySpec.metrics.map((metricName, index) => {
    const metric = metrics.find((m) => m.name === metricName)
    let expr: LogicalExpr = {
      kind: 'MetricRef',
      metricName,
      baseFact: baseFact?.name || null,
      resultType: { kind: 'number' },
    }

    if (metric) {
      // Parse the metric DSL to get the expression
      const metricDslMatch = metric.dsl.match(/=\s*(.+)$/)
      if (metricDslMatch) {
        // Simple parsing for common patterns
        const exprStr = metricDslMatch[1].trim()
        const aggMatch = exprStr.match(/^(\w+)\s*\(\s*(\*|\w+)\s*\)$/)
        if (aggMatch) {
          expr = {
            kind: 'Aggregate',
            op: aggMatch[1].toLowerCase() as 'sum' | 'avg' | 'count' | 'min' | 'max',
            input: {
              kind: 'AttributeRef',
              attributeId: aggMatch[2],
              physicalTable: baseTable,
              physicalColumn: aggMatch[2],
              dataType: { kind: 'number' },
              sourceKind: 'fact',
            },
            distinct: false,
            resultType: { kind: 'number' },
          }
        }
      }
    }

    return {
      name: metricName,
      expr,
      baseFact: baseFact?.name || null,
      dependencies: [],
      executionPhase: index,
    }
  })

  return {
    rootNodeId: projectId,
    nodes,
    outputDimensions: querySpec.dimensions,
    outputMetrics,
    metricEvalOrder: querySpec.metrics,
  }
}

/**
 * Format a logical plan as a text representation (for debugging)
 */
export function formatLogicalPlan(plan: LogicalQueryPlan): string {
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
        lines.push(`${prefix}  columns: ${node.columns.join(', ')}`)
        break

      case 'DimensionScan':
        lines.push(`${prefix}[${node.id}] DimensionScan: ${node.tableName}`)
        lines.push(`${prefix}  columns: ${node.columns.join(', ')}`)
        break

      case 'Join':
        lines.push(`${prefix}[${node.id}] Join: ${node.joinType}`)
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
        lines.push(`${prefix}  group by: ${node.groupBy.join(', ')}`)
        lines.push(`${prefix}  aggregates: ${node.aggregates.map((a) => `${a.outputName}=${a.op}(${a.input})`).join(', ')}`)
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
  return lines.join('\n')
}

/**
 * Format a logical expression as a string
 */
export function formatExpr(expr: LogicalExpr): string {
  switch (expr.kind) {
    case 'Constant':
      return String(expr.value)
    case 'AttributeRef':
      return expr.attributeId
    case 'MetricRef':
      return `@${expr.metricName}`
    case 'Aggregate':
      return `${expr.op}(${formatExpr(expr.input)})`
    case 'ScalarOp':
      return `(${formatExpr(expr.left)} ${expr.op} ${formatExpr(expr.right)})`
    case 'Comparison':
      return `${formatExpr(expr.left)} ${expr.op} ${formatExpr(expr.right)}`
    case 'LogicalOp':
      if (expr.op === 'not') {
        return `NOT ${formatExpr(expr.operands[0])}`
      }
      return `(${expr.operands.map(formatExpr).join(` ${expr.op.toUpperCase()} `)})`
  }
}
