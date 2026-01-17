import { useWorkspaceStore } from '../hooks/useWorkspaceStore'
import { DataEditor } from './editors/DataEditor'
import { SchemaEditor } from './editors/SchemaEditor'
import { MetricEditor } from './editors/MetricEditor'
import { QueryEditor } from './editors/QueryEditor'
import { JsonEditor } from './editors/JsonEditor'
import { WelcomeScreen } from './WelcomeScreen'

export function Workspace() {
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const tables = useWorkspaceStore((state) => state.tables)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queries = useWorkspaceStore((state) => state.queries)
  const jsonFiles = useWorkspaceStore((state) => state.jsonFiles)
  const openQueryTabs = useWorkspaceStore((state) => state.openQueryTabs)
  const openJsonTabs = useWorkspaceStore((state) => state.openJsonTabs)
  const setActiveTab = useWorkspaceStore((state) => state.setActiveTab)
  const closeQueryTab = useWorkspaceStore((state) => state.closeQueryTab)
  const closeJsonTab = useWorkspaceStore((state) => state.closeJsonTab)

  const renderContent = () => {
    if (!activeTab) {
      return <WelcomeScreen />
    }

    switch (activeTab.type) {
      case 'data': {
        const table = tables.find((t) => t.name === activeTab.tableName)
        if (!table) return <div className="empty-state">Table not found</div>
        return <DataEditor table={table} />
      }

      case 'schema':
        return <SchemaEditor />

      case 'metric': {
        const metric = metrics.find((m) => m.name === activeTab.metricName)
        if (!metric) return <div className="empty-state">Metric not found</div>
        return <MetricEditor metric={metric} />
      }

      case 'query': {
        const query = queries.find((q) => q.name === activeTab.queryName)
        if (!query) return <div className="empty-state">Query not found</div>
        return <QueryEditor query={query} />
      }

      case 'json': {
        const jsonFile = jsonFiles.find((j) => j.name === activeTab.jsonFileName)
        if (!jsonFile) return <div className="empty-state">JSON file not found</div>
        return <JsonEditor jsonFile={jsonFile} />
      }

      default:
        return <WelcomeScreen />
    }
  }

  const getTabTitle = () => {
    if (!activeTab) return ''

    switch (activeTab.type) {
      case 'data':
        return `Data: ${activeTab.tableName}`
      case 'schema':
        return 'Schema Editor'
      case 'metric':
        return `Metric: ${activeTab.metricName}`
      case 'json':
        return `JSON: ${activeTab.jsonFileName}`
      default:
        return ''
    }
  }

  const queryTabs = openQueryTabs.filter((name) => queries.some((q) => q.name === name))
  const jsonTabs = openJsonTabs.filter((name) => jsonFiles.some((j) => j.name === name))

  const renderTabBar = () => {
    if (!activeTab) return null

    // Query tabs
    if (activeTab.type === 'query' && queryTabs.length > 0) {
      return (
        <div className="workspace-tabs">
          {queryTabs.map((name) => (
            <div
              key={name}
              className={`workspace-tab ${
                activeTab.type === 'query' && activeTab.queryName === name ? 'active' : ''
              }`}
              onClick={() => setActiveTab({ type: 'query', queryName: name })}
            >
              <span>{`Query: ${name}`}</span>
              <span
                className="workspace-tab-close"
                onClick={(e) => {
                  e.stopPropagation()
                  closeQueryTab(name)
                }}
              >
                ×
              </span>
            </div>
          ))}
        </div>
      )
    }

    // JSON tabs
    if (activeTab.type === 'json' && jsonTabs.length > 0) {
      return (
        <div className="workspace-tabs">
          {jsonTabs.map((name) => (
            <div
              key={name}
              className={`workspace-tab ${
                activeTab.type === 'json' && activeTab.jsonFileName === name ? 'active' : ''
              }`}
              onClick={() => setActiveTab({ type: 'json', jsonFileName: name })}
            >
              <span>{`JSON: ${name}`}</span>
              <span
                className="workspace-tab-close"
                onClick={(e) => {
                  e.stopPropagation()
                  closeJsonTab(name)
                }}
              >
                ×
              </span>
            </div>
          ))}
        </div>
      )
    }

    // Default single tab
    return (
      <div className="workspace-tabs">
        <div className="workspace-tab active">
          <span>{getTabTitle()}</span>
        </div>
      </div>
    )
  }

  return (
    <div className="workspace">
      {renderTabBar()}
      <div className="workspace-content">{renderContent()}</div>
    </div>
  )
}
