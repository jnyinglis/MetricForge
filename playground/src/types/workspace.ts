// Workspace types for the Semantic Engine Playground

export interface TableData {
  name: string
  rows: Record<string, unknown>[]
  columns: ColumnInfo[]
}

export interface ColumnInfo {
  name: string
  type: 'string' | 'number' | 'boolean' | 'date' | 'unknown'
  sampleValues: unknown[]
}

export interface SchemaFact {
  name: string
  table: string
  description?: string
}

export interface SchemaDimension {
  name: string
  table: string
  description?: string
}

export interface SchemaAttribute {
  name: string
  table: string
  column: string
  description?: string
}

export interface SchemaJoin {
  fact: string
  dimension: string
  factKey: string
  dimensionKey: string
}

export interface SchemaDefinition {
  facts: SchemaFact[]
  dimensions: SchemaDimension[]
  attributes: SchemaAttribute[]
  joins: SchemaJoin[]
}

export interface MetricDefinition {
  name: string
  dsl: string
  valid: boolean
  errors: ParseError[]
}

export interface QueryDefinition {
  name: string
  dsl: string
  valid: boolean
  errors: ParseError[]
}

export interface ParseError {
  message: string
  line?: number
  column?: number
  severity: 'error' | 'warning' | 'info'
}

export interface JsonFileDefinition {
  name: string
  content: string
  valid: boolean
  errors: ParseError[]
  parsedData: unknown
}

export type JsonPanelTab = 'tree' | 'preview' | 'errors'

export interface QueryResult {
  queryName: string
  rows: Record<string, unknown>[]
  columns: string[]
  executionTime: number
  error?: string
}

export interface WorkspaceExport {
  version: string
  data: Record<string, Record<string, unknown>[]>
  schema: SchemaDefinition
  metrics: Record<string, string>
  queries: Record<string, string>
}

export type EditorTab =
  | { type: 'data'; tableName: string }
  | { type: 'schema' }
  | { type: 'metric'; metricName: string }
  | { type: 'query'; queryName: string }
  | { type: 'json'; jsonFileName: string }

export type RightPanelTab = 'preview' | 'ast' | 'plan' | 'errors' | 'results'

export interface WorkspaceState {
  // Data
  tables: TableData[]

  // Schema
  schema: SchemaDefinition

  // Metrics
  metrics: MetricDefinition[]

  // Queries
  queries: QueryDefinition[]

  // JSON Files
  jsonFiles: JsonFileDefinition[]

  // Query Results
  queryResults: QueryResult[]

  // Query tab UI state
  openQueryTabs: string[]
  queryPanelTabs: Record<string, RightPanelTab>

  // JSON tab UI state
  openJsonTabs: string[]
  jsonPanelTabs: Record<string, JsonPanelTab>

  // UI State
  activeTab: EditorTab | null
  rightPanelTab: RightPanelTab
  sidebarExpanded: {
    data: boolean
    schema: boolean
    metrics: boolean
    queries: boolean
    json: boolean
  }

  // UI Preferences
  theme: 'light' | 'dark'
  sidebarCollapsed: boolean
  rightPanelCollapsed: boolean
}
