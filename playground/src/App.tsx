import { useWorkspaceStore } from './hooks/useWorkspaceStore'
import { Header } from './components/Header'
import { Sidebar } from './components/Sidebar'
import { Workspace } from './components/Workspace'
import { RightPanel } from './components/RightPanel'
import { ResizablePanel } from './components/ResizablePanel'

function App() {
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const sidebarCollapsed = useWorkspaceStore((state) => state.sidebarCollapsed)
  const rightPanelCollapsed = useWorkspaceStore((state) => state.rightPanelCollapsed)
  const toggleSidebarCollapsed = useWorkspaceStore((state) => state.toggleSidebarCollapsed)
  const toggleRightPanelCollapsed = useWorkspaceStore((state) => state.toggleRightPanelCollapsed)

  return (
    <div className="app-container">
      <Header />
      <div className="main-content">
        <ResizablePanel
          defaultWidth={280}
          minWidth={200}
          maxWidth={500}
          side="left"
          collapsed={sidebarCollapsed}
          onCollapse={toggleSidebarCollapsed}
          className="sidebar"
        >
          <Sidebar />
        </ResizablePanel>

        <Workspace />

        {activeTab && activeTab.type !== 'query' && (
          <ResizablePanel
            defaultWidth={360}
            minWidth={280}
            maxWidth={600}
            side="right"
            collapsed={rightPanelCollapsed}
            onCollapse={toggleRightPanelCollapsed}
            className="right-panel"
          >
            <RightPanel />
          </ResizablePanel>
        )}
      </div>
    </div>
  )
}

export default App
