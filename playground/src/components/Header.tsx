import { useRef } from 'react'
import { useWorkspaceStore } from '../hooks/useWorkspaceStore'

export function Header() {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const exportWorkspace = useWorkspaceStore((state) => state.exportWorkspace)
  const importWorkspace = useWorkspaceStore((state) => state.importWorkspace)
  const resetWorkspace = useWorkspaceStore((state) => state.resetWorkspace)

  const handleExport = () => {
    const workspace = exportWorkspace()
    const blob = new Blob([JSON.stringify(workspace, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'playground-workspace.json'
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleImport = () => {
    fileInputRef.current?.click()
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (event) => {
      try {
        const workspace = JSON.parse(event.target?.result as string)
        importWorkspace(workspace)
      } catch (err) {
        alert('Failed to import workspace: Invalid JSON')
      }
    }
    reader.readAsText(file)
    e.target.value = ''
  }

  const handleReset = () => {
    if (confirm('Are you sure you want to reset the workspace? All data will be lost.')) {
      resetWorkspace()
    }
  }

  return (
    <header className="header">
      <div className="header-title">Semantic Engine Playground</div>
      <div className="header-actions">
        <button className="btn btn-sm" onClick={handleImport}>
          Import
        </button>
        <button className="btn btn-sm" onClick={handleExport}>
          Export
        </button>
        <button className="btn btn-sm" onClick={handleReset}>
          Reset
        </button>
      </div>
      <input
        ref={fileInputRef}
        type="file"
        accept=".json"
        style={{ display: 'none' }}
        onChange={handleFileChange}
      />
    </header>
  )
}
