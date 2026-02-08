import { useCallback, useEffect, useRef, useState } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'
import type { MetricDefinition } from '../../types/workspace'
import {
  parseDsl,
  parseMetricExpression,
  getDslCompletions,
} from '../../utils/coreLanguageService'

interface MetricEditorProps {
  metric: MetricDefinition
}

export function MetricEditor({ metric }: MetricEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<typeof import('monaco-editor') | null>(null)
  const updateMetric = useWorkspaceStore((state) => state.updateMetric)
  const theme = useWorkspaceStore((state) => state.theme)
  const [draftDsl, setDraftDsl] = useState(metric.dsl)
  const [draftErrors, setDraftErrors] = useState<MetricDefinition['errors']>(metric.errors)
  const [isDraftValid, setIsDraftValid] = useState(metric.valid)

  const isDirty = draftDsl !== metric.dsl

  const setMarkers = useCallback((errors: MetricDefinition['errors']) => {
    const model = editorRef.current?.getModel()
    const monaco = monacoRef.current
    if (!model || !monaco) return

    monaco.editor.setModelMarkers(
      model,
      'metric-dsl',
      errors.map((err) => ({
        severity:
          err.severity === 'error'
            ? monaco.MarkerSeverity.Error
            : err.severity === 'warning'
            ? monaco.MarkerSeverity.Warning
            : monaco.MarkerSeverity.Info,
        message: err.message,
        startLineNumber: err.line || 1,
        startColumn: err.column || 1,
        endLineNumber: err.line || 1,
        endColumn: (err.column || 1) + 10,
      }))
    )
  }, [])

  const validateContent = useCallback(
    (content: string) => {
      const trimmed = content.trim()
      const errors = trimmed.startsWith('metric')
        ? parseDsl(content).errors
        : parseMetricExpression(content).errors

      setDraftErrors(errors)
      setIsDraftValid(errors.length === 0)
      setMarkers(errors)

      return { errors, valid: errors.length === 0 }
    },
    [setMarkers]
  )

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
          const state = useWorkspaceStore.getState()
          const word = model.getWordUntilPosition(position)
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          }

          const completions = getDslCompletions(
            {
              attributes: state.schema.attributes.map((a) => a.name),
              metrics: state.metrics.map((m) => m.name),
              facts: state.schema.facts.map((f) => f.name),
              dimensions: state.schema.dimensions.map((d) => d.name),
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
      validateContent(metric.dsl)
    },
    [metric.dsl, validateContent]
  )

  const handleChange: OnChange = useCallback(
    (value) => {
      if (value !== undefined) {
        setDraftDsl(value)
        validateContent(value)
      }
    },
    [validateContent]
  )

  useEffect(() => {
    setDraftDsl(metric.dsl)
    validateContent(metric.dsl)
  }, [metric.name, metric.dsl, validateContent])

  // Switch Monaco editor theme when workspace theme changes
  useEffect(() => {
    if (monacoRef.current) {
      monacoRef.current.editor.setTheme(theme === 'light' ? 'metric-dsl-light' : 'metric-dsl-dark')
    }
  }, [theme])

  const handleSave = useCallback(() => {
    const result = validateContent(draftDsl)
    updateMetric(metric.name, draftDsl, result.valid, result.errors)
  }, [draftDsl, metric.name, updateMetric, validateContent])

  const handleCancel = useCallback(() => {
    setDraftDsl(metric.dsl)
    setDraftErrors(metric.errors)
    setIsDraftValid(metric.valid)
    setMarkers(metric.errors)
  }, [metric.dsl, metric.errors, metric.valid, setMarkers])

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
        <span className={`badge ${isDraftValid ? 'badge-success' : 'badge-error'}`}>
          {isDraftValid ? 'Valid' : 'Invalid'}
        </span>
        {isDirty && <span style={{ fontSize: 12, color: 'var(--warning)' }}>Unsaved changes</span>}
        {draftErrors.length > 0 && (
          <span style={{ fontSize: 12, color: 'var(--error)' }}>{draftErrors.length} error(s)</span>
        )}
        <span style={{ flex: 1 }} />
        <button className="btn btn-sm btn-primary" onClick={handleSave} disabled={!isDirty}>
          Save
        </button>
        <button className="btn btn-sm" onClick={handleCancel} disabled={!isDirty}>
          Cancel
        </button>
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
          value={draftDsl}
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
