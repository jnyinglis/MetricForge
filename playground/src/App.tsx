import { useWorkspaceStore } from './hooks/useWorkspaceStore'
import { Header } from './components/Header'
import { Sidebar } from './components/Sidebar'
import { Workspace } from './components/Workspace'
import { RightPanel } from './components/RightPanel'

function App() {
  const activeTab = useWorkspaceStore((state) => state.activeTab)

  return (
    <div className="app-container">
      <Header />
      <div className="main-content">
        <Sidebar />
        <Workspace />
        {activeTab && <RightPanel />}
      </div>
    </div>
  )
}

export default App
