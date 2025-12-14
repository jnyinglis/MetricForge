import { useCallback, useRef, useEffect } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'
import type { QueryDefinition, RightPanelTab } from '../../types/workspace'
import { parseDsl, getDslCompletions } from '../../utils/parserAdapter'
import { runQuery } from '../../utils/engineRunner'

interface QueryEditorProps {
  query: QueryDefinition
}

export function QueryEditor({ query }: QueryEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<typeof import('monaco-editor') | null>(null)
  const updateQuery = useWorkspaceStore((state) => state.updateQuery)
  const addQueryResult = useWorkspaceStore((state) => state.addQueryResult)
  const tables = useWorkspaceStore((state) => state.tables)
  const schema = useWorkspaceStore((state) => state.schema)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queryResults = useWorkspaceStore((state) => state.queryResults)
  const queryPanelTab = useWorkspaceStore(
    (state) => state.queryPanelTabs[query.name] ?? 'preview'
  )
  const setQueryPanelTab = useWorkspaceStore((state) => state.setQueryPanelTab)
  const theme = useWorkspaceStore((state) => state.theme)

  const handleEditorMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor
      monacoRef.current = monaco

      // Register DSL language if not already registered
      const languages = monaco.languages.getLanguages()
      if (!languages.some((l) => l.id === 'query-dsl')) {
        monaco.languages.register({ id: 'query-dsl' })

        // Define tokens for syntax highlighting
        monaco.languages.setMonarchTokensProvider('query-dsl', {
          keywords: [
            'metric',
            'on',
            'query',
            'dimensions',
            'metrics',
            'where',
            'having',
            'and',
            'or',
            'by',
          ],
          operators: ['+', '-', '*', '/', '=', '>', '<', '>=', '<=', '==', '!='],
          functions: ['sum', 'avg', 'min', 'max', 'count', 'last_year'],

          tokenizer: {
            root: [
              // Keywords
              [
                /\b(metric|on|query|dimensions|metrics|where|having|and|or|by)\b/,
                'keyword',
              ],
              // Functions
              [/\b(sum|avg|min|max|count|last_year)\b/, 'function'],
              // Numbers
              [/-?\d+(?:\.\d+)?/, 'number'],
              // Strings
              [/"[^"]*"/, 'string'],
              [/'[^']*'/, 'string'],
              // Identifiers
              [/[A-Za-z_][A-Za-z0-9_]*/, 'identifier'],
              // Operators
              [/[+\-*/=><]/, 'operator'],
              // Punctuation
              [/[(){},:;]/, 'delimiter'],
              // Whitespace
              [/\s+/, 'white'],
              // Comments
              [/\/\/.*$/, 'comment'],
            ],
          },
        })

        // Define dark theme
        monaco.editor.defineTheme('query-dsl-dark', {
          base: 'vs-dark',
          inherit: true,
          rules: [
            { token: 'keyword', foreground: '569cd6', fontStyle: 'bold' },
            { token: 'function', foreground: 'dcdcaa' },
            { token: 'number', foreground: 'b5cea8' },
            { token: 'string', foreground: 'ce9178' },
            { token: 'identifier', foreground: '9cdcfe' },
            { token: 'operator', foreground: 'd4d4d4' },
            { token: 'delimiter', foreground: 'd4d4d4' },
            { token: 'comment', foreground: '6a9955' },
          ],
          colors: {
            'editor.background': '#1e1e1e',
          },
        })

        // Define light theme
        monaco.editor.defineTheme('query-dsl-light', {
          base: 'vs',
          inherit: true,
          rules: [
            { token: 'keyword', foreground: '0000FF', fontStyle: 'bold' },
            { token: 'function', foreground: '795E26' },
            { token: 'number', foreground: '098658' },
            { token: 'string', foreground: 'A31515' },
            { token: 'identifier', foreground: '001080' },
            { token: 'operator', foreground: '000000' },
            { token: 'delimiter', foreground: '000000' },
            { token: 'comment', foreground: '008000' },
          ],
          colors: {
            'editor.background': '#FFFFFF',
          },
        })
      }

      // Set theme based on current workspace theme
      const currentTheme = useWorkspaceStore.getState().theme
      monaco.editor.setTheme(currentTheme === 'light' ? 'query-dsl-light' : 'query-dsl-dark')

      // Register completion provider
      monaco.languages.registerCompletionItemProvider('query-dsl', {
        provideCompletionItems: (model, position) => {
          const word = model.getWordUntilPosition(position)
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          }

          const completions = getDslCompletions(
            {
              attributes: schema.attributes.map((a) => a.name),
              metrics: metrics.map((m) => m.name),
              facts: schema.facts.map((f) => f.name),
              dimensions: schema.dimensions.map((d) => d.name),
            },
            { line: position.lineNumber, column: position.column },
            model.getValue()
          )

          return {
            suggestions: completions.map((c) => ({
              label: c.label,
              kind:
                c.kind === 'keyword'
                  ? monaco.languages.CompletionItemKind.Keyword
                  : c.kind === 'function'
                  ? monaco.languages.CompletionItemKind.Function
                  : c.kind === 'metric'
                  ? monaco.languages.CompletionItemKind.Variable
                  : monaco.languages.CompletionItemKind.Field,
              insertText: c.label,
              detail: c.detail,
              range,
            })),
          }
        },
      })

      // Validate on content change
      editor.onDidChangeModelContent(() => {
        validateContent(editor.getValue())
      })
    },
    [schema, metrics]
  )

  const validateContent = useCallback(
    (content: string) => {
      if (!editorRef.current) return

      const model = editorRef.current.getModel()
      if (!model) return

      const { errors } = parseDsl(content)

      // Set markers
      const monaco = (window as unknown as { monaco: typeof import('monaco-editor') }).monaco
      if (monaco) {
        monaco.editor.setModelMarkers(
          model,
          'query-dsl',
          errors.map((err) => ({
            severity:
              err.severity === 'error'
                ? monaco.MarkerSeverity.Error
                : monaco.MarkerSeverity.Warning,
            message: err.message,
            startLineNumber: err.line || 1,
            startColumn: err.column || 1,
            endLineNumber: err.line || 1,
            endColumn: (err.column || 1) + 10,
          }))
        )
      }

      // Update query state
      updateQuery(query.name, content, errors.length === 0, errors)
    },
    [query.name, updateQuery]
  )

  const handleChange: OnChange = useCallback(
    (value) => {
      if (value !== undefined) {
        validateContent(value)
      }
    },
    [validateContent]
  )

  const handleRunQuery = useCallback(() => {
    const result = runQuery(query.name, query.dsl, tables, schema, metrics)
    addQueryResult(result)
  }, [query.name, query.dsl, tables, schema, metrics, addQueryResult])

  const panelTabs: Array<{ id: RightPanelTab; label: string }> = [
    { id: 'preview', label: 'Preview' },
    { id: 'ast', label: 'AST' },
    { id: 'errors', label: 'Errors' },
    { id: 'results', label: 'Results' },
  ]

  const renderPreview = () => {
    return (
      <div>
        <h4 style={{ marginBottom: 8 }}>Query: {query.name}</h4>
        <div style={{ fontSize: 12, marginBottom: 8 }}>
          <strong>Status:</strong>{' '}
          <span className={`badge ${query.valid ? 'badge-success' : 'badge-error'}`}>
            {query.valid ? 'Valid' : 'Invalid'}
          </span>
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
          Uses metrics: {metrics.length} | schema tables: {schema.facts.length + schema.dimensions.length}
        </div>
      </div>
    )
  }

  const renderAst = () => {
    if (!query.dsl) return <div style={{ color: 'var(--text-muted)' }}>No AST available</div>

    const { ast, errors } = parseDsl(query.dsl)
    if (errors.length > 0 || !ast) {
      return <div style={{ color: 'var(--error)' }}>Parse error</div>
    }

    return (
      <div className="json-preview" style={{ maxHeight: '100%', overflow: 'auto' }}>
        <pre>{JSON.stringify(ast.queries[0] || {}, null, 2)}</pre>
      </div>
    )
  }

  const renderErrors = () => {
    if (query.errors.length === 0) {
      return <div style={{ color: 'var(--success)' }}>No errors</div>
    }

    return (
      <div>
        {query.errors.map((err, i) => (
          <div key={i} className="problem-item">
            <span className={`problem-icon ${err.severity === 'error' ? 'error' : 'warning'}`}>
              {err.severity === 'error' ? '●' : '▲'}
            </span>
            <div>
              <div className="problem-message">{err.message}</div>
              <div className="problem-location">Line {err.line ?? 1}</div>
            </div>
          </div>
        ))}
      </div>
    )
  }

  const renderResults = () => {
    const result = queryResults.find((r) => r.queryName === query.name)

    if (!result) {
      return <div style={{ color: 'var(--text-muted)' }}>No query results yet</div>
    }

    return (
      <div>
        <div style={{ marginBottom: 8, fontSize: 12 }}>
          <strong>Query:</strong> {result.queryName}
          <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>
            ({result.executionTime.toFixed(2)}ms)
          </span>
        </div>

        {result.error ? (
          <div style={{ color: 'var(--error)' }}>{result.error}</div>
        ) : (
          <>
            <div style={{ marginBottom: 8, fontSize: 12 }}>
              <strong>Rows:</strong> {result.rows.length}
            </div>
            <div className="table-container">
              <table className="data-table">
                <thead>
                  <tr>
                    {result.columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.slice(0, 100).map((row, i) => (
                    <tr key={i}>
                      {result.columns.map((col) => (
                        <td key={col}>
                          {row[col] === undefined ? (
                            <span style={{ color: 'var(--text-muted)' }}>null</span>
                          ) : typeof row[col] === 'number' ? (
                            (row[col] as number).toLocaleString()
                          ) : (
                            String(row[col])
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    )
  }

  // Initial validation
  useEffect(() => {
    if (query.dsl) {
      validateContent(query.dsl)
    }
  }, [])

  // Switch Monaco editor theme when workspace theme changes
  useEffect(() => {
    if (monacoRef.current) {
      monacoRef.current.editor.setTheme(theme === 'light' ? 'query-dsl-light' : 'query-dsl-dark')
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
        <span style={{ fontWeight: 600 }}>{query.name}</span>
        <span className={`badge ${query.valid ? 'badge-success' : 'badge-error'}`}>
          {query.valid ? 'Valid' : 'Invalid'}
        </span>
        <span style={{ flex: 1 }} />
        <button
          className="btn btn-primary btn-sm"
          onClick={handleRunQuery}
          disabled={!query.valid || tables.length === 0}
        >
          Run Query
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
        {/* Editor */}
        <div className="editor-container" style={{ flex: 1 }}>
          <Editor
            height="100%"
            defaultLanguage="query-dsl"
            theme={theme === 'light' ? 'query-dsl-light' : 'query-dsl-dark'}
            value={query.dsl}
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
            }}
          />
        </div>

        {/* Inline panel */}
        <div className="query-panel">
          <div className="panel-tabs">
            {panelTabs.map((tab) => (
              <div
                key={tab.id}
                className={`panel-tab ${queryPanelTab === tab.id ? 'active' : ''}`}
                onClick={() => setQueryPanelTab(query.name, tab.id)}
              >
                {tab.label}
                {tab.id === 'errors' && query.errors.length > 0 && (
                  <span style={{ marginLeft: 4 }}>({query.errors.length})</span>
                )}
              </div>
            ))}
          </div>
          <div className="panel-content query-panel-content">
            {queryPanelTab === 'preview' && renderPreview()}
            {queryPanelTab === 'ast' && renderAst()}
            {queryPanelTab === 'errors' && renderErrors()}
            {queryPanelTab === 'results' && renderResults()}
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
        <strong>Syntax:</strong> query [name] {'{'} dimensions: ... metrics: ... {'}'}
        <span style={{ margin: '0 12px' }}>|</span>
        <strong>Clauses:</strong> dimensions, metrics, where, having
      </div>
    </div>
  )
}
