import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type {
  WorkspaceState,
  TableData,
  SchemaDefinition,
  MetricDefinition,
  QueryDefinition,
  QueryResult,
  EditorTab,
  RightPanelTab,
  WorkspaceExport,
  ColumnInfo,
} from '../types/workspace'

const DEFAULT_SCHEMA: SchemaDefinition = {
  facts: [],
  dimensions: [],
  attributes: [],
  joins: [],
}

interface WorkspaceActions {
  // Data actions
  addTable: (name: string, data: Record<string, unknown>[]) => void
  removeTable: (name: string) => void
  clearTables: () => void

  // Schema actions
  setSchema: (schema: SchemaDefinition) => void
  addFact: (fact: { name: string; table: string; description?: string }) => void
  removeFact: (name: string) => void
  addDimension: (dimension: { name: string; table: string; description?: string }) => void
  removeDimension: (name: string) => void
  addAttribute: (attribute: { name: string; table: string; column: string; description?: string }) => void
  removeAttribute: (name: string) => void
  addJoin: (join: { fact: string; dimension: string; factKey: string; dimensionKey: string }) => void
  removeJoin: (index: number) => void

  // Metric actions
  addMetric: (name: string, dsl?: string) => void
  updateMetric: (name: string, dsl: string, valid: boolean, errors: MetricDefinition['errors']) => void
  removeMetric: (name: string) => void

  // Query actions
  addQuery: (name: string, dsl?: string) => void
  updateQuery: (name: string, dsl: string, valid: boolean, errors: QueryDefinition['errors']) => void
  removeQuery: (name: string) => void

  // Results actions
  addQueryResult: (result: QueryResult) => void
  clearQueryResults: () => void

  // UI actions
  setActiveTab: (tab: EditorTab | null) => void
  setRightPanelTab: (tab: RightPanelTab) => void
  toggleSidebarSection: (section: keyof WorkspaceState['sidebarExpanded']) => void

  // Import/Export actions
  exportWorkspace: () => WorkspaceExport
  importWorkspace: (workspace: WorkspaceExport) => void
  resetWorkspace: () => void
}

function inferColumnType(values: unknown[]): ColumnInfo['type'] {
  const nonNullValues = values.filter((v) => v != null)
  if (nonNullValues.length === 0) return 'unknown'

  const first = nonNullValues[0]
  if (typeof first === 'number') return 'number'
  if (typeof first === 'boolean') return 'boolean'
  if (typeof first === 'string') {
    // Check if it looks like a date
    const datePattern = /^\d{4}-\d{2}-\d{2}/
    if (datePattern.test(first)) return 'date'
    return 'string'
  }
  return 'unknown'
}

function analyzeTableColumns(rows: Record<string, unknown>[]): ColumnInfo[] {
  if (rows.length === 0) return []

  const columnNames = new Set<string>()
  rows.forEach((row) => {
    Object.keys(row).forEach((key) => columnNames.add(key))
  })

  return Array.from(columnNames).map((name) => {
    const values = rows.slice(0, 100).map((row) => row[name])
    return {
      name,
      type: inferColumnType(values),
      sampleValues: values.slice(0, 5),
    }
  })
}

const initialState: WorkspaceState = {
  tables: [],
  schema: DEFAULT_SCHEMA,
  metrics: [],
  queries: [],
  queryResults: [],
  activeTab: null,
  rightPanelTab: 'preview',
  sidebarExpanded: {
    data: true,
    schema: true,
    metrics: true,
    queries: true,
  },
}

export const useWorkspaceStore = create<WorkspaceState & WorkspaceActions>()(
  persist(
    (set, get) => ({
      ...initialState,

      // Data actions
      addTable: (name, data) => {
        const columns = analyzeTableColumns(data)
        const table: TableData = { name, rows: data, columns }
        set((state) => ({
          tables: [...state.tables.filter((t) => t.name !== name), table],
        }))
      },

      removeTable: (name) => {
        set((state) => ({
          tables: state.tables.filter((t) => t.name !== name),
          activeTab:
            state.activeTab?.type === 'data' && state.activeTab.tableName === name
              ? null
              : state.activeTab,
        }))
      },

      clearTables: () => {
        set({ tables: [] })
      },

      // Schema actions
      setSchema: (schema) => {
        set({ schema })
      },

      addFact: (fact) => {
        set((state) => ({
          schema: {
            ...state.schema,
            facts: [...state.schema.facts.filter((f) => f.name !== fact.name), fact],
          },
        }))
      },

      removeFact: (name) => {
        set((state) => ({
          schema: {
            ...state.schema,
            facts: state.schema.facts.filter((f) => f.name !== name),
          },
        }))
      },

      addDimension: (dimension) => {
        set((state) => ({
          schema: {
            ...state.schema,
            dimensions: [
              ...state.schema.dimensions.filter((d) => d.name !== dimension.name),
              dimension,
            ],
          },
        }))
      },

      removeDimension: (name) => {
        set((state) => ({
          schema: {
            ...state.schema,
            dimensions: state.schema.dimensions.filter((d) => d.name !== name),
          },
        }))
      },

      addAttribute: (attribute) => {
        set((state) => ({
          schema: {
            ...state.schema,
            attributes: [
              ...state.schema.attributes.filter((a) => a.name !== attribute.name),
              attribute,
            ],
          },
        }))
      },

      removeAttribute: (name) => {
        set((state) => ({
          schema: {
            ...state.schema,
            attributes: state.schema.attributes.filter((a) => a.name !== name),
          },
        }))
      },

      addJoin: (join) => {
        set((state) => ({
          schema: {
            ...state.schema,
            joins: [...state.schema.joins, join],
          },
        }))
      },

      removeJoin: (index) => {
        set((state) => ({
          schema: {
            ...state.schema,
            joins: state.schema.joins.filter((_, i) => i !== index),
          },
        }))
      },

      // Metric actions
      addMetric: (name, dsl = '') => {
        set((state) => ({
          metrics: [
            ...state.metrics.filter((m) => m.name !== name),
            { name, dsl, valid: true, errors: [] },
          ],
        }))
      },

      updateMetric: (name, dsl, valid, errors) => {
        set((state) => ({
          metrics: state.metrics.map((m) =>
            m.name === name ? { ...m, dsl, valid, errors } : m
          ),
        }))
      },

      removeMetric: (name) => {
        set((state) => ({
          metrics: state.metrics.filter((m) => m.name !== name),
          activeTab:
            state.activeTab?.type === 'metric' && state.activeTab.metricName === name
              ? null
              : state.activeTab,
        }))
      },

      // Query actions
      addQuery: (name, dsl = '') => {
        set((state) => ({
          queries: [
            ...state.queries.filter((q) => q.name !== name),
            { name, dsl, valid: true, errors: [] },
          ],
        }))
      },

      updateQuery: (name, dsl, valid, errors) => {
        set((state) => ({
          queries: state.queries.map((q) =>
            q.name === name ? { ...q, dsl, valid, errors } : q
          ),
        }))
      },

      removeQuery: (name) => {
        set((state) => ({
          queries: state.queries.filter((q) => q.name !== name),
          activeTab:
            state.activeTab?.type === 'query' && state.activeTab.queryName === name
              ? null
              : state.activeTab,
        }))
      },

      // Results actions
      addQueryResult: (result) => {
        set((state) => ({
          queryResults: [
            ...state.queryResults.filter((r) => r.queryName !== result.queryName),
            result,
          ],
          rightPanelTab: 'results',
        }))
      },

      clearQueryResults: () => {
        set({ queryResults: [] })
      },

      // UI actions
      setActiveTab: (tab) => {
        set({ activeTab: tab })
      },

      setRightPanelTab: (tab) => {
        set({ rightPanelTab: tab })
      },

      toggleSidebarSection: (section) => {
        set((state) => ({
          sidebarExpanded: {
            ...state.sidebarExpanded,
            [section]: !state.sidebarExpanded[section],
          },
        }))
      },

      // Import/Export
      exportWorkspace: () => {
        const state = get()
        const data: Record<string, Record<string, unknown>[]> = {}
        state.tables.forEach((t) => {
          data[t.name] = t.rows
        })

        const metrics: Record<string, string> = {}
        state.metrics.forEach((m) => {
          metrics[m.name] = m.dsl
        })

        const queries: Record<string, string> = {}
        state.queries.forEach((q) => {
          queries[q.name] = q.dsl
        })

        return {
          version: '1.0',
          data,
          schema: state.schema,
          metrics,
          queries,
        }
      },

      importWorkspace: (workspace) => {
        const tables: TableData[] = Object.entries(workspace.data).map(([name, rows]) => ({
          name,
          rows,
          columns: analyzeTableColumns(rows),
        }))

        const metrics: MetricDefinition[] = Object.entries(workspace.metrics).map(
          ([name, dsl]) => ({
            name,
            dsl,
            valid: true,
            errors: [],
          })
        )

        const queries: QueryDefinition[] = Object.entries(workspace.queries).map(
          ([name, dsl]) => ({
            name,
            dsl,
            valid: true,
            errors: [],
          })
        )

        set({
          tables,
          schema: workspace.schema,
          metrics,
          queries,
          queryResults: [],
          activeTab: null,
        })
      },

      resetWorkspace: () => {
        set(initialState)
      },
    }),
    {
      name: 'semantic-engine-playground',
      version: 1,
    }
  )
)
