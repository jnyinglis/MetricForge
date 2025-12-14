import { useCallback, useRef, useEffect } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'
import type { MetricDefinition } from '../../types/workspace'
import { parseDsl, getDslCompletions } from '../../utils/parserAdapter'

interface MetricEditorProps {
  metric: MetricDefinition
}

export function MetricEditor({ metric }: MetricEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<typeof import('monaco-editor') | null>(null)
  const updateMetric = useWorkspaceStore((state) => state.updateMetric)
  const schema = useWorkspaceStore((state) => state.schema)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const theme = useWorkspaceStore((state) => state.theme)

  const handleEditorMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor
      monacoRef.current = monaco

      // Register DSL language
      monaco.languages.register({ id: 'metric-dsl' })

      // Define tokens for syntax highlighting
      monaco.languages.setMonarchTokensProvider('metric-dsl', {
        keywords: ['metric', 'on', 'query', 'dimensions', 'metrics', 'where', 'having', 'and', 'or', 'by'],
        operators: ['+', '-', '*', '/', '=', '>', '<', '>=', '<=', '==', '!='],
        functions: ['sum', 'avg', 'min', 'max', 'count', 'last_year'],

        tokenizer: {
          root: [
            // Keywords
            [/\b(metric|on|query|dimensions|metrics|where|having|and|or|by)\b/, 'keyword'],
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
            [/[(),:]/, 'delimiter'],
            // Whitespace
            [/\s+/, 'white'],
            // Comments
            [/\/\/.*$/, 'comment'],
          ],
        },
      })

      // Define dark theme
      monaco.editor.defineTheme('metric-dsl-dark', {
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
      monaco.editor.defineTheme('metric-dsl-light', {
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

      // Set theme based on current workspace theme
      const currentTheme = useWorkspaceStore.getState().theme
      monaco.editor.setTheme(currentTheme === 'light' ? 'metric-dsl-light' : 'metric-dsl-dark')

      // Register completion provider
      monaco.languages.registerCompletionItemProvider('metric-dsl', {
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

      // Parse the content
      const dslText = content.trim().startsWith('metric')
        ? content
        : `metric ${metric.name} on default = ${content}`

      const { errors } = parseDsl(dslText)

      // Set markers
      const monaco = (window as unknown as { monaco: typeof import('monaco-editor') }).monaco
      if (monaco) {
        monaco.editor.setModelMarkers(
          model,
          'metric-dsl',
          errors.map((err) => ({
            severity: err.severity === 'error' ? monaco.MarkerSeverity.Error : monaco.MarkerSeverity.Warning,
            message: err.message,
            startLineNumber: err.line || 1,
            startColumn: err.column || 1,
            endLineNumber: err.line || 1,
            endColumn: (err.column || 1) + 10,
          }))
        )
      }

      // Update metric state
      updateMetric(metric.name, content, errors.length === 0, errors)
    },
    [metric.name, updateMetric]
  )

  const handleChange: OnChange = useCallback(
    (value) => {
      if (value !== undefined) {
        validateContent(value)
      }
    },
    [validateContent]
  )

  // Initial validation
  useEffect(() => {
    if (metric.dsl) {
      validateContent(metric.dsl)
    }
  }, [])

  // Switch Monaco editor theme when workspace theme changes
  useEffect(() => {
    if (monacoRef.current) {
      monacoRef.current.editor.setTheme(theme === 'light' ? 'metric-dsl-light' : 'metric-dsl-dark')
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
        <span style={{ fontWeight: 600 }}>{metric.name}</span>
        <span className={`badge ${metric.valid ? 'badge-success' : 'badge-error'}`}>
          {metric.valid ? 'Valid' : 'Invalid'}
        </span>
        <span style={{ flex: 1 }} />
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          Tip: Use sum(), avg(), min(), max(), count() for aggregates
        </span>
      </div>

      {/* Editor */}
      <div className="editor-container" style={{ flex: 1 }}>
        <Editor
          height="100%"
          defaultLanguage="metric-dsl"
          theme={theme === 'light' ? 'metric-dsl-light' : 'metric-dsl-dark'}
          value={metric.dsl}
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
        <strong>Syntax:</strong> metric [name] on [fact] = [expression]
        <span style={{ margin: '0 12px' }}>|</span>
        <strong>Example:</strong> metric total_sales on sales = sum(amount)
      </div>
    </div>
  )
}
