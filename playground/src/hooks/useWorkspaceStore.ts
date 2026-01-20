import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type {
  WorkspaceState,
  TableData,
  SchemaDefinition,
  MetricDefinition,
  QueryDefinition,
  JsonFileDefinition,
  QueryResult,
  EditorTab,
  RightPanelTab,
  JsonPanelTab,
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

  // JSON file actions
  addJsonFile: (name: string, content?: string) => void
  updateJsonFile: (name: string, content: string, valid: boolean, errors: JsonFileDefinition['errors'], parsedData: unknown) => void
  removeJsonFile: (name: string) => void

  // Results actions
  addQueryResult: (result: QueryResult) => void
  clearQueryResults: () => void

  // UI actions
  setActiveTab: (tab: EditorTab | null) => void
  setRightPanelTab: (tab: RightPanelTab) => void
  setQueryPanelTab: (queryName: string, tab: RightPanelTab) => void
  closeQueryTab: (queryName: string) => void
  setJsonPanelTab: (jsonFileName: string, tab: JsonPanelTab) => void
  closeJsonTab: (jsonFileName: string) => void
  toggleSidebarSection: (section: keyof WorkspaceState['sidebarExpanded']) => void

  // UI Preference actions
  toggleTheme: () => void
  setTheme: (theme: 'light' | 'dark') => void
  toggleSidebarCollapsed: () => void
  toggleRightPanelCollapsed: () => void

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
  jsonFiles: [],
  queryResults: [],
  openQueryTabs: [],
  queryPanelTabs: {},
  openJsonTabs: [],
  jsonPanelTabs: {},
  activeTab: null,
  rightPanelTab: 'preview',
  sidebarExpanded: {
    data: true,
    schema: true,
    metrics: true,
    queries: true,
    json: true,
  },
  theme: 'dark',
  sidebarCollapsed: false,
  rightPanelCollapsed: false,
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
          openQueryTabs: state.openQueryTabs.filter((q) => q !== name),
          queryPanelTabs: Object.fromEntries(
            Object.entries(state.queryPanelTabs).filter(([key]) => key !== name)
          ),
        }))
      },

      // JSON file actions
      addJsonFile: (name, content = '{}') => {
        let parsedData: unknown = null
        let valid = true
        const errors: JsonFileDefinition['errors'] = []

        try {
          parsedData = JSON.parse(content)
        } catch (err) {
          valid = false
          errors.push({
            message: err instanceof Error ? err.message : String(err),
            line: 1,
            column: 1,
            severity: 'error',
          })
        }

        set((state) => ({
          jsonFiles: [
            ...state.jsonFiles.filter((j) => j.name !== name),
            { name, content, valid, errors, parsedData },
          ],
        }))
      },

      updateJsonFile: (name, content, valid, errors, parsedData) => {
        set((state) => ({
          jsonFiles: state.jsonFiles.map((j) =>
            j.name === name ? { ...j, content, valid, errors, parsedData } : j
          ),
        }))
      },

      removeJsonFile: (name) => {
        set((state) => ({
          jsonFiles: state.jsonFiles.filter((j) => j.name !== name),
          activeTab:
            state.activeTab?.type === 'json' && state.activeTab.jsonFileName === name
              ? null
              : state.activeTab,
          openJsonTabs: state.openJsonTabs.filter((j) => j !== name),
          jsonPanelTabs: Object.fromEntries(
            Object.entries(state.jsonPanelTabs).filter(([key]) => key !== name)
          ),
        }))
      },

      // Results actions
      addQueryResult: (result) => {
        set((state) => ({
          queryResults: [
            ...state.queryResults.filter((r) => r.queryName !== result.queryName),
            result,
          ],
          queryPanelTabs: { ...state.queryPanelTabs, [result.queryName]: 'results' },
        }))
      },

      clearQueryResults: () => {
        set({ queryResults: [] })
      },

      // UI actions
      setActiveTab: (tab) => {
        set((state) => {
          if (!tab) {
            return { activeTab: tab }
          }

          if (tab.type === 'query') {
            const alreadyOpen = state.openQueryTabs.includes(tab.queryName)

            return {
              activeTab: tab,
              openQueryTabs: alreadyOpen
                ? state.openQueryTabs
                : [...state.openQueryTabs, tab.queryName],
              queryPanelTabs: state.queryPanelTabs[tab.queryName]
                ? state.queryPanelTabs
                : { ...state.queryPanelTabs, [tab.queryName]: 'preview' },
            }
          }

          if (tab.type === 'json') {
            const alreadyOpen = state.openJsonTabs.includes(tab.jsonFileName)

            return {
              activeTab: tab,
              openJsonTabs: alreadyOpen
                ? state.openJsonTabs
                : [...state.openJsonTabs, tab.jsonFileName],
              jsonPanelTabs: state.jsonPanelTabs[tab.jsonFileName]
                ? state.jsonPanelTabs
                : { ...state.jsonPanelTabs, [tab.jsonFileName]: 'tree' },
            }
          }

          return { activeTab: tab }
        })
      },

      setRightPanelTab: (tab) => {
        set({ rightPanelTab: tab })
      },

      setQueryPanelTab: (queryName, tab) => {
        set((state) => ({
          queryPanelTabs: { ...state.queryPanelTabs, [queryName]: tab },
        }))
      },

      closeQueryTab: (queryName) => {
        set((state) => {
          const remainingTabs = state.openQueryTabs.filter((name) => name !== queryName)
          const { [queryName]: _closed, ...restQueryTabs } = state.queryPanelTabs

          let nextActive = state.activeTab
          if (state.activeTab?.type === 'query' && state.activeTab.queryName === queryName) {
            nextActive = remainingTabs.length
              ? { type: 'query', queryName: remainingTabs[remainingTabs.length - 1] }
              : null
          }

          return {
            openQueryTabs: remainingTabs,
            queryPanelTabs: restQueryTabs,
            activeTab: nextActive,
          }
        })
      },

      setJsonPanelTab: (jsonFileName, tab) => {
        set((state) => ({
          jsonPanelTabs: { ...state.jsonPanelTabs, [jsonFileName]: tab },
        }))
      },

      closeJsonTab: (jsonFileName) => {
        set((state) => {
          const remainingTabs = state.openJsonTabs.filter((name) => name !== jsonFileName)
          const { [jsonFileName]: _closed, ...restJsonTabs } = state.jsonPanelTabs

          let nextActive = state.activeTab
          if (state.activeTab?.type === 'json' && state.activeTab.jsonFileName === jsonFileName) {
            nextActive = remainingTabs.length
              ? { type: 'json', jsonFileName: remainingTabs[remainingTabs.length - 1] }
              : null
          }

          return {
            openJsonTabs: remainingTabs,
            jsonPanelTabs: restJsonTabs,
            activeTab: nextActive,
          }
        })
      },

      toggleSidebarSection: (section) => {
        set((state) => ({
          sidebarExpanded: {
            ...state.sidebarExpanded,
            [section]: !state.sidebarExpanded[section],
          },
        }))
      },

      // UI Preference actions
      toggleTheme: () => {
        set((state) => ({ theme: state.theme === 'light' ? 'dark' : 'light' }))
      },

      setTheme: (theme) => {
        set({ theme })
      },

      toggleSidebarCollapsed: () => {
        set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed }))
      },

      toggleRightPanelCollapsed: () => {
        set((state) => ({ rightPanelCollapsed: !state.rightPanelCollapsed }))
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
          jsonFiles: [],
          queryResults: [],
          activeTab: null,
          openQueryTabs: [],
          queryPanelTabs: {},
          openJsonTabs: [],
          jsonPanelTabs: {},
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
