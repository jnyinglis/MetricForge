import { useCallback, useRef, useEffect } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'
import type { QueryDefinition } from '../../types/workspace'
import { parseDsl, getDslCompletions } from '../../utils/parserAdapter'
import { runQuery } from '../../utils/engineRunner'

interface QueryEditorProps {
  query: QueryDefinition
}

export function QueryEditor({ query }: QueryEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const updateQuery = useWorkspaceStore((state) => state.updateQuery)
  const addQueryResult = useWorkspaceStore((state) => state.addQueryResult)
  const tables = useWorkspaceStore((state) => state.tables)
  const schema = useWorkspaceStore((state) => state.schema)
  const metrics = useWorkspaceStore((state) => state.metrics)

  const handleEditorMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor

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

        // Define theme
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
      }

      monaco.editor.setTheme('query-dsl-dark')

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

  // Initial validation
  useEffect(() => {
    if (query.dsl) {
      validateContent(query.dsl)
    }
  }, [])

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

      {/* Editor */}
      <div className="editor-container" style={{ flex: 1 }}>
        <Editor
          height="100%"
          defaultLanguage="query-dsl"
          theme="query-dsl-dark"
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
