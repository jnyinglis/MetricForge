// Legacy compatibility shim.
// Query execution is delegated to core runtime via coreBridge.

import { runWorkspaceQuery } from './coreBridge'
import type {
  MetricDefinition,
  QueryResult,
  SchemaDefinition,
  TableData,
} from '../types/workspace'

export function runQuery(
  queryName: string,
  queryDsl: string,
  tables: TableData[],
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): QueryResult {
  return runWorkspaceQuery(queryName, queryDsl, tables, schema, metrics)
}

export function runSimpleQuery(
  tables: TableData[],
  schema: SchemaDefinition,
  metrics: MetricDefinition[],
  dimensions: string[],
  metricNames: string[],
  metricDefs: Record<string, string>
): QueryResult {
  const syntheticMetrics: MetricDefinition[] = [
    ...metrics,
    ...Object.entries(metricDefs)
      .filter(([metricName]) => !metrics.some((metric) => metric.name === metricName))
      .map(([metricName, dsl]) => ({
        name: metricName,
        dsl,
        valid: true,
        errors: [],
      })),
  ]

  const queryDsl = `query simple_query {
  dimensions: ${dimensions.join(', ')}
  metrics: ${metricNames.join(', ')}
}`

  return runWorkspaceQuery('simple', queryDsl, tables, schema, syntheticMetrics)
}
