import { useRef, useState } from 'react'
import { useWorkspaceStore } from '../hooks/useWorkspaceStore'

export function WelcomeScreen() {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const addTable = useWorkspaceStore((state) => state.addTable)
  const setActiveTab = useWorkspaceStore((state) => state.setActiveTab)
  const addMetric = useWorkspaceStore((state) => state.addMetric)
  const addQuery = useWorkspaceStore((state) => state.addQuery)

  const [jsonInput, setJsonInput] = useState('')
  const [tableName, setTableName] = useState('')
  const [showJsonInput, setShowJsonInput] = useState(false)
  const [dragActive, setDragActive] = useState(false)

  const handleFileUpload = (file: File) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const data = JSON.parse(e.target?.result as string)
        const name = file.name.replace('.json', '')

        if (Array.isArray(data)) {
          addTable(name, data)
          setActiveTab({ type: 'data', tableName: name })
        } else if (typeof data === 'object') {
          // Check if it's an object with array values (multiple tables)
          const entries = Object.entries(data)
          const arrayEntries = entries.filter(([, v]) => Array.isArray(v))

          if (arrayEntries.length > 0) {
            arrayEntries.forEach(([key, value]) => {
              addTable(key, value as Record<string, unknown>[])
            })
            setActiveTab({ type: 'data', tableName: arrayEntries[0][0] })
          } else {
            alert('JSON must be an array of objects or an object with array properties')
          }
        }
      } catch (err) {
        alert('Failed to parse JSON file')
      }
    }
    reader.readAsText(file)
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setDragActive(false)
    const file = e.dataTransfer.files[0]
    if (file && file.name.endsWith('.json')) {
      handleFileUpload(file)
    }
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setDragActive(true)
  }

  const handleDragLeave = () => {
    setDragActive(false)
  }

  const handlePasteJson = () => {
    if (!jsonInput.trim()) {
      alert('Please provide JSON data')
      return
    }

    try {
      const data = JSON.parse(jsonInput)

      if (Array.isArray(data)) {
        // Single table: require table name
        if (!tableName.trim()) {
          alert('Please provide a table name for the array data')
          return
        }
        addTable(tableName.trim(), data)
        setActiveTab({ type: 'data', tableName: tableName.trim() })
      } else if (typeof data === 'object' && data !== null) {
        // Multiple tables: extract from object keys
        const entries = Object.entries(data)
        const arrayEntries = entries.filter(([, v]) => Array.isArray(v))

        if (arrayEntries.length === 0) {
          alert('JSON must be an array of objects or an object with array properties')
          return
        }

        arrayEntries.forEach(([key, value]) => {
          addTable(key, value as Record<string, unknown>[])
        })
        setActiveTab({ type: 'data', tableName: arrayEntries[0][0] })
      } else {
        alert('JSON must be an array of objects or an object with array properties')
        return
      }

      setJsonInput('')
      setTableName('')
      setShowJsonInput(false)
    } catch (err) {
      alert('Invalid JSON')
    }
  }

  const handleLoadSample = () => {
    // Sample sales data
    const salesData = [
      { sale_id: 1, product_id: 1, region_id: 1, date: '2024-01-15', amount: 1500, quantity: 3 },
      { sale_id: 2, product_id: 2, region_id: 1, date: '2024-01-16', amount: 2500, quantity: 5 },
      { sale_id: 3, product_id: 1, region_id: 2, date: '2024-01-17', amount: 1200, quantity: 2 },
      { sale_id: 4, product_id: 3, region_id: 2, date: '2024-01-18', amount: 3200, quantity: 4 },
      { sale_id: 5, product_id: 2, region_id: 1, date: '2024-01-19', amount: 1800, quantity: 3 },
      { sale_id: 6, product_id: 1, region_id: 3, date: '2024-01-20', amount: 900, quantity: 1 },
      { sale_id: 7, product_id: 3, region_id: 3, date: '2024-01-21', amount: 4100, quantity: 5 },
      { sale_id: 8, product_id: 2, region_id: 2, date: '2024-01-22', amount: 2200, quantity: 4 },
    ]

    const productsData = [
      { product_id: 1, name: 'Widget A', category: 'Widgets', price: 500 },
      { product_id: 2, name: 'Gadget B', category: 'Gadgets', price: 500 },
      { product_id: 3, name: 'Gizmo C', category: 'Gizmos', price: 800 },
    ]

    const regionsData = [
      { region_id: 1, name: 'North', country: 'USA' },
      { region_id: 2, name: 'South', country: 'USA' },
      { region_id: 3, name: 'West', country: 'USA' },
    ]

    addTable('sales', salesData)
    addTable('products', productsData)
    addTable('regions', regionsData)

    // Add sample schema
    const { setSchema, addFact, addDimension, addAttribute, addJoin } = useWorkspaceStore.getState()

    setSchema({
      facts: [],
      dimensions: [],
      attributes: [],
      joins: [],
    })

    addFact({ name: 'sales', table: 'sales' })
    addDimension({ name: 'products', table: 'products' })
    addDimension({ name: 'regions', table: 'regions' })

    addAttribute({ name: 'amount', table: 'sales', column: 'amount' })
    addAttribute({ name: 'quantity', table: 'sales', column: 'quantity' })
    addAttribute({ name: 'date', table: 'sales', column: 'date' })
    addAttribute({ name: 'product_name', table: 'products', column: 'name' })
    addAttribute({ name: 'category', table: 'products', column: 'category' })
    addAttribute({ name: 'region_name', table: 'regions', column: 'name' })

    addJoin({ fact: 'sales', dimension: 'products', factKey: 'product_id', dimensionKey: 'product_id' })
    addJoin({ fact: 'sales', dimension: 'regions', factKey: 'region_id', dimensionKey: 'region_id' })

    // Add sample metric
    addMetric('total_revenue', 'metric total_revenue on sales = sum(amount)')
    addMetric('total_quantity', 'metric total_quantity on sales = sum(quantity)')
    addMetric('avg_order_value', 'metric avg_order_value on sales = avg(amount)')

    // Add sample query
    addQuery('revenue_by_region', `query revenue_by_region {
  dimensions: region_name
  metrics: total_revenue, total_quantity
}`)

    setActiveTab({ type: 'data', tableName: 'sales' })
  }

  return (
    <div className="empty-state" style={{ padding: 40 }}>
      <div className="empty-state-icon">🎯</div>
      <h2 className="empty-state-title">Welcome to Semantic Engine Playground</h2>
      <p className="empty-state-description" style={{ marginBottom: 24 }}>
        Import data, define schemas and metrics, and explore the semantic metrics engine.
      </p>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 16, alignItems: 'center' }}>
        {/* File drop zone */}
        <div
          className={`drop-zone ${dragActive ? 'active' : ''}`}
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onClick={() => fileInputRef.current?.click()}
          style={{ width: '100%', maxWidth: 400 }}
        >
          <div className="drop-zone-icon">📁</div>
          <div>Drop a JSON file here or click to upload</div>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 8 }}>
            Accepts .json files with array data
          </div>
        </div>

        <input
          ref={fileInputRef}
          type="file"
          accept=".json"
          style={{ display: 'none' }}
          onChange={(e) => {
            const file = e.target.files?.[0]
            if (file) handleFileUpload(file)
            e.target.value = ''
          }}
        />

        {/* Or divider */}
        <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>or</div>

        {/* Paste JSON */}
        {!showJsonInput ? (
          <button className="btn" onClick={() => setShowJsonInput(true)}>
            Paste JSON Data
          </button>
        ) : (
          <div style={{ width: '100%', maxWidth: 400 }}>
            <div className="form-group">
              <input
                type="text"
                className="form-input"
                placeholder="Table name"
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
              />
            </div>
            <div className="form-group">
              <textarea
                className="form-textarea"
                placeholder='Paste JSON array, e.g.: [{"id": 1, "name": "test"}]'
                value={jsonInput}
                onChange={(e) => setJsonInput(e.target.value)}
                rows={6}
              />
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary" onClick={handlePasteJson}>
                Add Table
              </button>
              <button className="btn" onClick={() => setShowJsonInput(false)}>
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* Load sample data */}
        <div style={{ marginTop: 16 }}>
          <button className="btn btn-success" onClick={handleLoadSample}>
            Load Sample Data
          </button>
        </div>
      </div>
    </div>
  )
}
