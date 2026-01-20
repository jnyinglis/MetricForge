import { useCallback, useRef, useEffect, useState } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'
import type { JsonFileDefinition, JsonPanelTab } from '../../types/workspace'
import { JsonTreeViewer, JsonSummary } from '../JsonTreeViewer'

interface JsonEditorProps {
  jsonFile: JsonFileDefinition
}

export function JsonEditor({ jsonFile }: JsonEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<typeof import('monaco-editor') | null>(null)
  const updateJsonFile = useWorkspaceStore((state) => state.updateJsonFile)
  const jsonPanelTab = useWorkspaceStore(
    (state) => state.jsonPanelTabs[jsonFile.name] ?? 'tree'
  )
  const setJsonPanelTab = useWorkspaceStore((state) => state.setJsonPanelTab)
  const theme = useWorkspaceStore((state) => state.theme)

  const [selectedPath, setSelectedPath] = useState<string | null>(null)
  const [selectedValue, setSelectedValue] = useState<unknown>(null)

  const handleEditorMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor
      monacoRef.current = monaco

      // Set theme based on current workspace theme
      const currentTheme = useWorkspaceStore.getState().theme
      monaco.editor.setTheme(currentTheme === 'light' ? 'vs' : 'vs-dark')

      // Validate on content change
      editor.onDidChangeModelContent(() => {
        validateContent(editor.getValue())
      })

      // Add format action
      editor.addAction({
        id: 'format-json',
        label: 'Format JSON',
        keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
        run: () => {
          const model = editor.getModel()
          if (model) {
            try {
              const formatted = JSON.stringify(JSON.parse(model.getValue()), null, 2)
              model.setValue(formatted)
            } catch {
              // Invalid JSON, can't format
            }
          }
        },
      })

      // Add minify action
      editor.addAction({
        id: 'minify-json',
        label: 'Minify JSON',
        run: () => {
          const model = editor.getModel()
          if (model) {
            try {
              const minified = JSON.stringify(JSON.parse(model.getValue()))
              model.setValue(minified)
            } catch {
              // Invalid JSON, can't minify
            }
          }
        },
      })
    },
    []
  )

  const validateContent = useCallback(
    (content: string) => {
      if (!editorRef.current) return

      const model = editorRef.current.getModel()
      if (!model) return

      let parsedData: unknown = null
      const errors: JsonFileDefinition['errors'] = []

      try {
        parsedData = JSON.parse(content)
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err)
        // Try to extract line number from JSON parse error
        const lineMatch = errorMessage.match(/position (\d+)/)
        let line = 1
        let column = 1

        if (lineMatch) {
          const position = parseInt(lineMatch[1], 10)
          const lines = content.substring(0, position).split('\n')
          line = lines.length
          column = lines[lines.length - 1].length + 1
        }

        errors.push({
          message: errorMessage,
          line,
          column,
          severity: 'error',
        })
      }

      // Set markers
      const monaco = (window as unknown as { monaco: typeof import('monaco-editor') }).monaco
      if (monaco) {
        monaco.editor.setModelMarkers(
          model,
          'json',
          errors.map((err) => ({
            severity: monaco.MarkerSeverity.Error,
            message: err.message,
            startLineNumber: err.line || 1,
            startColumn: err.column || 1,
            endLineNumber: err.line || 1,
            endColumn: (err.column || 1) + 10,
          }))
        )
      }

      // Update json file state
      updateJsonFile(jsonFile.name, content, errors.length === 0, errors, parsedData)
    },
    [jsonFile.name, updateJsonFile]
  )

  const handleChange: OnChange = useCallback(
    (value) => {
      if (value !== undefined) {
        validateContent(value)
      }
    },
    [validateContent]
  )

  const handleFormat = useCallback(() => {
    if (!editorRef.current) return
    const model = editorRef.current.getModel()
    if (!model) return

    try {
      const formatted = JSON.stringify(JSON.parse(model.getValue()), null, 2)
      model.setValue(formatted)
    } catch {
      // Invalid JSON
    }
  }, [])

  const handleMinify = useCallback(() => {
    if (!editorRef.current) return
    const model = editorRef.current.getModel()
    if (!model) return

    try {
      const minified = JSON.stringify(JSON.parse(model.getValue()))
      model.setValue(minified)
    } catch {
      // Invalid JSON
    }
  }, [])

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(jsonFile.content)
  }, [jsonFile.content])

  const handlePathSelect = useCallback((path: string, value: unknown) => {
    setSelectedPath(path)
    setSelectedValue(value)
  }, [])

  const panelTabs: Array<{ id: JsonPanelTab; label: string }> = [
    { id: 'tree', label: 'Tree' },
    { id: 'preview', label: 'Preview' },
    { id: 'errors', label: 'Errors' },
  ]

  const renderTree = () => {
    if (!jsonFile.valid || jsonFile.parsedData === null) {
      return (
        <div style={{ color: 'var(--error)', padding: 16 }}>
          Invalid JSON - fix errors to view tree
        </div>
      )
    }

    return (
      <div style={{ height: '100%', overflow: 'auto' }}>
        <JsonTreeViewer
          data={jsonFile.parsedData}
          initialExpandLevel={2}
          onPathSelect={handlePathSelect}
        />
        {selectedPath && (
          <div className="json-selection-info">
            <div className="json-selection-path">
              <strong>Path:</strong> {selectedPath}
            </div>
            <div className="json-selection-value">
              <strong>Value:</strong>
              <pre>{JSON.stringify(selectedValue, null, 2)}</pre>
            </div>
          </div>
        )}
      </div>
    )
  }

  const renderPreview = () => {
    return (
      <div>
        <h4 style={{ marginBottom: 8 }}>JSON File: {jsonFile.name}</h4>
        <div style={{ fontSize: 12, marginBottom: 8 }}>
          <strong>Status:</strong>{' '}
          <span className={`badge ${jsonFile.valid ? 'badge-success' : 'badge-error'}`}>
            {jsonFile.valid ? 'Valid' : 'Invalid'}
          </span>
        </div>
        <div style={{ fontSize: 12, marginBottom: 12, color: 'var(--text-muted)' }}>
          Size: {jsonFile.content.length.toLocaleString()} characters
        </div>

        {jsonFile.valid && jsonFile.parsedData !== null && (
          <div style={{ marginTop: 16 }}>
            <h5 style={{ marginBottom: 8, fontSize: 12, color: 'var(--text-secondary)' }}>
              Structure Summary
            </h5>
            <JsonSummary data={jsonFile.parsedData} />
          </div>
        )}
      </div>
    )
  }

  const renderErrors = () => {
    if (jsonFile.errors.length === 0) {
      return <div style={{ color: 'var(--success)' }}>No errors - JSON is valid</div>
    }

    return (
      <div>
        {jsonFile.errors.map((err, i) => (
          <div key={i} className="problem-item">
            <span className={`problem-icon ${err.severity === 'error' ? 'error' : 'warning'}`}>
              {err.severity === 'error' ? '●' : '▲'}
            </span>
            <div>
              <div className="problem-message">{err.message}</div>
              <div className="problem-location">
                Line {err.line ?? 1}, Column {err.column ?? 1}
              </div>
            </div>
          </div>
        ))}
      </div>
    )
  }

  // Initial validation
  useEffect(() => {
    if (jsonFile.content) {
      validateContent(jsonFile.content)
    }
  }, [])

  // Switch Monaco editor theme when workspace theme changes
  useEffect(() => {
    if (monacoRef.current) {
      monacoRef.current.editor.setTheme(theme === 'light' ? 'vs' : 'vs-dark')
    }
  }, [theme])

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Toolbar */}
      <div
        style={{
          padding: '8px 12px',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
        }}
      >
        <span style={{ fontWeight: 600 }}>{jsonFile.name}</span>
        <span className={`badge ${jsonFile.valid ? 'badge-success' : 'badge-error'}`}>
          {jsonFile.valid ? 'Valid JSON' : 'Invalid JSON'}
        </span>
        <span style={{ flex: 1 }} />
        <button className="btn btn-sm" onClick={handleFormat} disabled={!jsonFile.valid}>
          Format
        </button>
        <button className="btn btn-sm" onClick={handleMinify} disabled={!jsonFile.valid}>
          Minify
        </button>
        <button className="btn btn-sm" onClick={handleCopy}>
          Copy
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
        {/* Editor */}
        <div className="editor-container" style={{ flex: 1 }}>
          <Editor
            height="100%"
            defaultLanguage="json"
            theme={theme === 'light' ? 'vs' : 'vs-dark'}
            value={jsonFile.content}
            onChange={handleChange}
            onMount={handleEditorMount}
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              lineNumbers: 'on',
              scrollBeyondLastLine: false,
              wordWrap: 'on',
              automaticLayout: true,
              tabSize: 2,
              padding: { top: 8, bottom: 8 },
              folding: true,
              foldingStrategy: 'indentation',
              formatOnPaste: true,
              bracketPairColorization: { enabled: true },
            }}
          />
        </div>

        {/* Inline panel */}
        <div className="query-panel">
          <div className="panel-tabs">
            {panelTabs.map((tab) => (
              <div
                key={tab.id}
                className={`panel-tab ${jsonPanelTab === tab.id ? 'active' : ''}`}
                onClick={() => setJsonPanelTab(jsonFile.name, tab.id)}
              >
                {tab.label}
                {tab.id === 'errors' && jsonFile.errors.length > 0 && (
                  <span style={{ marginLeft: 4 }}>({jsonFile.errors.length})</span>
                )}
              </div>
            ))}
          </div>
          <div className="panel-content query-panel-content">
            {jsonPanelTab === 'tree' && renderTree()}
            {jsonPanelTab === 'preview' && renderPreview()}
            {jsonPanelTab === 'errors' && renderErrors()}
          </div>
        </div>
      </div>

      {/* Help */}
      <div
        style={{
          padding: '8px 12px',
          borderTop: '1px solid var(--border-color)',
          fontSize: 11,
          color: 'var(--text-muted)',
        }}
      >
        <strong>Tips:</strong> Shift+Alt+F to format
        <span style={{ margin: '0 12px' }}>|</span>
        Click tree nodes to inspect values
        <span style={{ margin: '0 12px' }}>|</span>
        Use P/V buttons to copy path/value
      </div>
    </div>
  )
}
