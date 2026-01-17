/**
 * JSON Tree Viewer Component
 *
 * Renders JSON data as an interactive tree visualization with expand/collapse,
 * type-based coloring, and path copying functionality.
 */

import { useState, useCallback, useMemo } from 'react'

interface JsonTreeViewerProps {
  data: unknown
  initialExpandLevel?: number
  onPathSelect?: (path: string, value: unknown) => void
}

type JsonValueType = 'string' | 'number' | 'boolean' | 'null' | 'object' | 'array'

// Type to color mapping
const typeColors: Record<JsonValueType, string> = {
  string: '#ce9178',
  number: '#b5cea8',
  boolean: '#569cd6',
  null: '#808080',
  object: '#dcdcaa',
  array: '#4ec9b0',
}

// Light theme type colors
const typeColorsLight: Record<JsonValueType, string> = {
  string: '#a31515',
  number: '#098658',
  boolean: '#0000ff',
  null: '#808080',
  object: '#795e26',
  array: '#267f99',
}

function getValueType(value: unknown): JsonValueType {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  const type = typeof value
  if (type === 'object') return 'object'
  if (type === 'string') return 'string'
  if (type === 'number') return 'number'
  if (type === 'boolean') return 'boolean'
  return 'string'
}

function formatValue(value: unknown, type: JsonValueType): string {
  switch (type) {
    case 'string':
      return `"${value}"`
    case 'null':
      return 'null'
    case 'boolean':
      return value ? 'true' : 'false'
    case 'number':
      return String(value)
    case 'object':
      return `{${Object.keys(value as object).length}}`
    case 'array':
      return `[${(value as unknown[]).length}]`
    default:
      return String(value)
  }
}

interface TreeNodeProps {
  keyName: string | number | null
  value: unknown
  path: string
  depth: number
  initialExpandLevel: number
  expandedPaths: Set<string>
  selectedPath: string | null
  onToggle: (path: string) => void
  onSelect: (path: string, value: unknown) => void
  isLightTheme: boolean
}

function TreeNode({
  keyName,
  value,
  path,
  depth,
  initialExpandLevel,
  expandedPaths,
  selectedPath,
  onToggle,
  onSelect,
  isLightTheme,
}: TreeNodeProps) {
  const type = getValueType(value)
  const isExpandable = type === 'object' || type === 'array'
  const isExpanded = expandedPaths.has(path)
  const isSelected = selectedPath === path
  const colors = isLightTheme ? typeColorsLight : typeColors

  const handleToggle = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      if (isExpandable) {
        onToggle(path)
      }
    },
    [isExpandable, onToggle, path]
  )

  const handleSelect = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      onSelect(path, value)
    },
    [onSelect, path, value]
  )

  const handleCopyPath = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      navigator.clipboard.writeText(path)
    },
    [path]
  )

  const handleCopyValue = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation()
      navigator.clipboard.writeText(JSON.stringify(value, null, 2))
    },
    [value]
  )

  const renderChildren = () => {
    if (!isExpanded || !isExpandable) return null

    if (type === 'array') {
      const arr = value as unknown[]
      if (arr.length === 0) {
        return (
          <div className="json-tree-empty" style={{ marginLeft: (depth + 1) * 16 }}>
            (empty array)
          </div>
        )
      }
      return arr.map((item, index) => (
        <TreeNode
          key={index}
          keyName={index}
          value={item}
          path={`${path}[${index}]`}
          depth={depth + 1}
          initialExpandLevel={initialExpandLevel}
          expandedPaths={expandedPaths}
          selectedPath={selectedPath}
          onToggle={onToggle}
          onSelect={onSelect}
          isLightTheme={isLightTheme}
        />
      ))
    }

    if (type === 'object') {
      const obj = value as Record<string, unknown>
      const keys = Object.keys(obj)
      if (keys.length === 0) {
        return (
          <div className="json-tree-empty" style={{ marginLeft: (depth + 1) * 16 }}>
            (empty object)
          </div>
        )
      }
      return keys.map((key) => (
        <TreeNode
          key={key}
          keyName={key}
          value={obj[key]}
          path={path ? `${path}.${key}` : key}
          depth={depth + 1}
          initialExpandLevel={initialExpandLevel}
          expandedPaths={expandedPaths}
          selectedPath={selectedPath}
          onToggle={onToggle}
          onSelect={onSelect}
          isLightTheme={isLightTheme}
        />
      ))
    }

    return null
  }

  return (
    <div className="json-tree-node-container">
      <div
        className={`json-tree-node ${isSelected ? 'selected' : ''}`}
        style={{ paddingLeft: depth * 16 }}
        onClick={handleSelect}
      >
        {isExpandable && (
          <button className="json-tree-toggle" onClick={handleToggle}>
            {isExpanded ? '▼' : '▶'}
          </button>
        )}
        {!isExpandable && <span className="json-tree-toggle-spacer" />}

        {keyName !== null && (
          <>
            <span className="json-tree-key">{typeof keyName === 'number' ? keyName : `"${keyName}"`}</span>
            <span className="json-tree-colon">:</span>
          </>
        )}

        <span className="json-tree-value" style={{ color: colors[type] }}>
          {isExpandable ? (
            <>
              <span className="json-tree-type">{type === 'array' ? 'Array' : 'Object'}</span>
              <span className="json-tree-count">{formatValue(value, type)}</span>
            </>
          ) : (
            formatValue(value, type)
          )}
        </span>

        <div className="json-tree-actions">
          <button className="json-tree-action" onClick={handleCopyPath} title="Copy path">
            P
          </button>
          <button className="json-tree-action" onClick={handleCopyValue} title="Copy value">
            V
          </button>
        </div>
      </div>
      {renderChildren()}
    </div>
  )
}

export function JsonTreeViewer({ data, initialExpandLevel = 2, onPathSelect }: JsonTreeViewerProps) {
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(() => {
    // Initially expand paths up to initialExpandLevel
    const paths = new Set<string>()

    function collectPaths(value: unknown, path: string, level: number) {
      if (level >= initialExpandLevel) return

      const type = getValueType(value)
      if (type === 'object') {
        paths.add(path || '$')
        const obj = value as Record<string, unknown>
        Object.keys(obj).forEach((key) => {
          collectPaths(obj[key], path ? `${path}.${key}` : key, level + 1)
        })
      } else if (type === 'array') {
        paths.add(path || '$')
        const arr = value as unknown[]
        arr.forEach((item, index) => {
          collectPaths(item, `${path || '$'}[${index}]`, level + 1)
        })
      }
    }

    collectPaths(data, '$', 0)
    return paths
  })

  const [selectedPath, setSelectedPath] = useState<string | null>(null)

  // Detect theme from CSS variable
  const isLightTheme = useMemo(() => {
    if (typeof window === 'undefined') return false
    return document.documentElement.getAttribute('data-theme') === 'light'
  }, [])

  const handleToggle = useCallback((path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev)
      if (next.has(path)) {
        next.delete(path)
      } else {
        next.add(path)
      }
      return next
    })
  }, [])

  const handleSelect = useCallback(
    (path: string, value: unknown) => {
      setSelectedPath(path === selectedPath ? null : path)
      onPathSelect?.(path, value)
    },
    [selectedPath, onPathSelect]
  )

  const handleExpandAll = useCallback(() => {
    const paths = new Set<string>()

    function collectAllPaths(value: unknown, path: string) {
      const type = getValueType(value)
      if (type === 'object') {
        paths.add(path || '$')
        const obj = value as Record<string, unknown>
        Object.keys(obj).forEach((key) => {
          collectAllPaths(obj[key], path ? `${path}.${key}` : key)
        })
      } else if (type === 'array') {
        paths.add(path || '$')
        const arr = value as unknown[]
        arr.forEach((item, index) => {
          collectAllPaths(item, `${path || '$'}[${index}]`)
        })
      }
    }

    collectAllPaths(data, '$')
    setExpandedPaths(paths)
  }, [data])

  const handleCollapseAll = useCallback(() => {
    setExpandedPaths(new Set(['$']))
  }, [])

  if (data === undefined) {
    return (
      <div className="json-tree-viewer">
        <div className="json-tree-empty-state">No data to display</div>
      </div>
    )
  }

  const rootType = getValueType(data)
  const isRootExpandable = rootType === 'object' || rootType === 'array'

  return (
    <div className="json-tree-viewer">
      <div className="json-tree-toolbar">
        <button className="btn btn-sm" onClick={handleExpandAll}>
          Expand All
        </button>
        <button className="btn btn-sm" onClick={handleCollapseAll}>
          Collapse All
        </button>
        <div className="json-tree-legend">
          {Object.entries(isLightTheme ? typeColorsLight : typeColors).map(([type, color]) => (
            <div key={type} className="json-tree-legend-item">
              <span className="json-tree-legend-color" style={{ backgroundColor: color }} />
              <span>{type}</span>
            </div>
          ))}
        </div>
      </div>
      <div className="json-tree-content">
        {isRootExpandable ? (
          <TreeNode
            keyName={null}
            value={data}
            path="$"
            depth={0}
            initialExpandLevel={initialExpandLevel}
            expandedPaths={expandedPaths}
            selectedPath={selectedPath}
            onToggle={handleToggle}
            onSelect={handleSelect}
            isLightTheme={isLightTheme}
          />
        ) : (
          <div className="json-tree-primitive">
            <span
              className="json-tree-value"
              style={{ color: (isLightTheme ? typeColorsLight : typeColors)[rootType] }}
            >
              {formatValue(data, rootType)}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

// Also export a compact summary view
export function JsonSummary({ data }: { data: unknown }) {
  const type = getValueType(data)

  const getSummary = (): string => {
    switch (type) {
      case 'object': {
        const keys = Object.keys(data as object)
        return `Object with ${keys.length} ${keys.length === 1 ? 'key' : 'keys'}`
      }
      case 'array': {
        const len = (data as unknown[]).length
        return `Array with ${len} ${len === 1 ? 'item' : 'items'}`
      }
      case 'string':
        return `String (${(data as string).length} chars)`
      case 'number':
        return `Number: ${data}`
      case 'boolean':
        return `Boolean: ${data}`
      case 'null':
        return 'null'
      default:
        return 'Unknown type'
    }
  }

  return (
    <div className="json-summary">
      <div className="json-summary-type">{type.charAt(0).toUpperCase() + type.slice(1)}</div>
      <div className="json-summary-info">{getSummary()}</div>
    </div>
  )
}
