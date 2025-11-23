import { useState } from 'react'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'

type SchemaSection = 'facts' | 'dimensions' | 'attributes' | 'joins'

export function SchemaEditor() {
  const schema = useWorkspaceStore((state) => state.schema)
  const tables = useWorkspaceStore((state) => state.tables)
  const addFact = useWorkspaceStore((state) => state.addFact)
  const removeFact = useWorkspaceStore((state) => state.removeFact)
  const addDimension = useWorkspaceStore((state) => state.addDimension)
  const removeDimension = useWorkspaceStore((state) => state.removeDimension)
  const addAttribute = useWorkspaceStore((state) => state.addAttribute)
  const removeAttribute = useWorkspaceStore((state) => state.removeAttribute)
  const addJoin = useWorkspaceStore((state) => state.addJoin)
  const removeJoin = useWorkspaceStore((state) => state.removeJoin)

  const [activeSection, setActiveSection] = useState<SchemaSection>('facts')
  const [showAddForm, setShowAddForm] = useState(false)

  // Form states
  const [newFact, setNewFact] = useState({ name: '', table: '' })
  const [newDimension, setNewDimension] = useState({ name: '', table: '' })
  const [newAttribute, setNewAttribute] = useState({ name: '', table: '', column: '' })
  const [newJoin, setNewJoin] = useState({ fact: '', dimension: '', factKey: '', dimensionKey: '' })

  const tableNames = tables.map((t) => t.name)

  const getColumnsForTable = (tableName: string): string[] => {
    const table = tables.find((t) => t.name === tableName)
    return table ? table.columns.map((c) => c.name) : []
  }

  const handleAddFact = () => {
    if (newFact.name && newFact.table) {
      addFact(newFact)
      setNewFact({ name: '', table: '' })
      setShowAddForm(false)
    }
  }

  const handleAddDimension = () => {
    if (newDimension.name && newDimension.table) {
      addDimension(newDimension)
      setNewDimension({ name: '', table: '' })
      setShowAddForm(false)
    }
  }

  const handleAddAttribute = () => {
    if (newAttribute.name && newAttribute.table && newAttribute.column) {
      addAttribute(newAttribute)
      setNewAttribute({ name: '', table: '', column: '' })
      setShowAddForm(false)
    }
  }

  const handleAddJoin = () => {
    if (newJoin.fact && newJoin.dimension && newJoin.factKey && newJoin.dimensionKey) {
      addJoin(newJoin)
      setNewJoin({ fact: '', dimension: '', factKey: '', dimensionKey: '' })
      setShowAddForm(false)
    }
  }

  const renderAddForm = () => {
    switch (activeSection) {
      case 'facts':
        return (
          <div style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}>
            <h4 style={{ marginBottom: 12 }}>Add Fact</h4>
            <div className="form-group">
              <label className="form-label">Name</label>
              <input
                type="text"
                className="form-input"
                placeholder="e.g., sales"
                value={newFact.name}
                onChange={(e) => setNewFact({ ...newFact, name: e.target.value })}
              />
            </div>
            <div className="form-group">
              <label className="form-label">Table</label>
              <select
                className="form-select"
                value={newFact.table}
                onChange={(e) => setNewFact({ ...newFact, table: e.target.value })}
              >
                <option value="">Select table...</option>
                {tableNames.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddFact}>Add</button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>Cancel</button>
            </div>
          </div>
        )

      case 'dimensions':
        return (
          <div style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}>
            <h4 style={{ marginBottom: 12 }}>Add Dimension</h4>
            <div className="form-group">
              <label className="form-label">Name</label>
              <input
                type="text"
                className="form-input"
                placeholder="e.g., products"
                value={newDimension.name}
                onChange={(e) => setNewDimension({ ...newDimension, name: e.target.value })}
              />
            </div>
            <div className="form-group">
              <label className="form-label">Table</label>
              <select
                className="form-select"
                value={newDimension.table}
                onChange={(e) => setNewDimension({ ...newDimension, table: e.target.value })}
              >
                <option value="">Select table...</option>
                {tableNames.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddDimension}>Add</button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>Cancel</button>
            </div>
          </div>
        )

      case 'attributes':
        return (
          <div style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}>
            <h4 style={{ marginBottom: 12 }}>Add Attribute</h4>
            <div className="form-group">
              <label className="form-label">Logical Name</label>
              <input
                type="text"
                className="form-input"
                placeholder="e.g., product_name"
                value={newAttribute.name}
                onChange={(e) => setNewAttribute({ ...newAttribute, name: e.target.value })}
              />
            </div>
            <div className="form-group">
              <label className="form-label">Table</label>
              <select
                className="form-select"
                value={newAttribute.table}
                onChange={(e) => setNewAttribute({ ...newAttribute, table: e.target.value, column: '' })}
              >
                <option value="">Select table...</option>
                {tableNames.map((name) => (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label className="form-label">Column</label>
              <select
                className="form-select"
                value={newAttribute.column}
                onChange={(e) => setNewAttribute({ ...newAttribute, column: e.target.value })}
                disabled={!newAttribute.table}
              >
                <option value="">Select column...</option>
                {getColumnsForTable(newAttribute.table).map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddAttribute}>Add</button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>Cancel</button>
            </div>
          </div>
        )

      case 'joins':
        const factTable = schema.facts.find(f => f.name === newJoin.fact)?.table
        const dimTable = schema.dimensions.find(d => d.name === newJoin.dimension)?.table
        return (
          <div style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}>
            <h4 style={{ marginBottom: 12 }}>Add Join</h4>
            <div className="form-group">
              <label className="form-label">Fact</label>
              <select
                className="form-select"
                value={newJoin.fact}
                onChange={(e) => setNewJoin({ ...newJoin, fact: e.target.value, factKey: '' })}
              >
                <option value="">Select fact...</option>
                {schema.facts.map((f) => (
                  <option key={f.name} value={f.name}>{f.name}</option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label className="form-label">Fact Key Column</label>
              <select
                className="form-select"
                value={newJoin.factKey}
                onChange={(e) => setNewJoin({ ...newJoin, factKey: e.target.value })}
                disabled={!newJoin.fact}
              >
                <option value="">Select column...</option>
                {factTable && getColumnsForTable(factTable).map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label className="form-label">Dimension</label>
              <select
                className="form-select"
                value={newJoin.dimension}
                onChange={(e) => setNewJoin({ ...newJoin, dimension: e.target.value, dimensionKey: '' })}
              >
                <option value="">Select dimension...</option>
                {schema.dimensions.map((d) => (
                  <option key={d.name} value={d.name}>{d.name}</option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label className="form-label">Dimension Key Column</label>
              <select
                className="form-select"
                value={newJoin.dimensionKey}
                onChange={(e) => setNewJoin({ ...newJoin, dimensionKey: e.target.value })}
                disabled={!newJoin.dimension}
              >
                <option value="">Select column...</option>
                {dimTable && getColumnsForTable(dimTable).map((col) => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddJoin}>Add</button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>Cancel</button>
            </div>
          </div>
        )
    }
  }

  const renderList = () => {
    switch (activeSection) {
      case 'facts':
        return schema.facts.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No facts defined. Facts represent your transaction/event tables.
          </div>
        ) : (
          <div>
            {schema.facts.map((fact) => (
              <div key={fact.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                <span style={{ flex: 1 }}>
                  <strong>{fact.name}</strong>
                  <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>→ {fact.table}</span>
                </span>
                <span
                  onClick={() => removeFact(fact.name)}
                  style={{ cursor: 'pointer', opacity: 0.5 }}
                >×</span>
              </div>
            ))}
          </div>
        )

      case 'dimensions':
        return schema.dimensions.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No dimensions defined. Dimensions represent your lookup/reference tables.
          </div>
        ) : (
          <div>
            {schema.dimensions.map((dim) => (
              <div key={dim.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                <span style={{ flex: 1 }}>
                  <strong>{dim.name}</strong>
                  <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>→ {dim.table}</span>
                </span>
                <span
                  onClick={() => removeDimension(dim.name)}
                  style={{ cursor: 'pointer', opacity: 0.5 }}
                >×</span>
              </div>
            ))}
          </div>
        )

      case 'attributes':
        return schema.attributes.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No attributes defined. Attributes map logical names to physical columns.
          </div>
        ) : (
          <div>
            {schema.attributes.map((attr) => (
              <div key={attr.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                <span style={{ flex: 1 }}>
                  <strong>{attr.name}</strong>
                  <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>
                    → {attr.table}.{attr.column}
                  </span>
                </span>
                <span
                  onClick={() => removeAttribute(attr.name)}
                  style={{ cursor: 'pointer', opacity: 0.5 }}
                >×</span>
              </div>
            ))}
          </div>
        )

      case 'joins':
        return schema.joins.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No joins defined. Joins connect facts to dimensions.
          </div>
        ) : (
          <div>
            {schema.joins.map((join, i) => (
              <div key={i} className="sidebar-item" style={{ padding: '8px 12px' }}>
                <span style={{ flex: 1 }}>
                  <span style={{ color: 'var(--info)' }}>{join.fact}</span>
                  <span style={{ color: 'var(--text-muted)' }}>.{join.factKey}</span>
                  <span style={{ margin: '0 8px' }}>→</span>
                  <span style={{ color: 'var(--success)' }}>{join.dimension}</span>
                  <span style={{ color: 'var(--text-muted)' }}>.{join.dimensionKey}</span>
                </span>
                <span
                  onClick={() => removeJoin(i)}
                  style={{ cursor: 'pointer', opacity: 0.5 }}
                >×</span>
              </div>
            ))}
          </div>
        )
    }
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Section tabs */}
      <div style={{ display: 'flex', borderBottom: '1px solid var(--border-color)' }}>
        {(['facts', 'dimensions', 'attributes', 'joins'] as SchemaSection[]).map((section) => (
          <div
            key={section}
            className={`panel-tab ${activeSection === section ? 'active' : ''}`}
            onClick={() => { setActiveSection(section); setShowAddForm(false) }}
            style={{ textTransform: 'capitalize' }}
          >
            {section}
            <span style={{ marginLeft: 4, opacity: 0.6 }}>
              ({section === 'joins' ? schema.joins.length : schema[section].length})
            </span>
          </div>
        ))}
        <div style={{ flex: 1 }} />
        <button
          className="btn btn-sm btn-primary"
          style={{ margin: 4 }}
          onClick={() => setShowAddForm(!showAddForm)}
        >
          {showAddForm ? 'Cancel' : '+ Add'}
        </button>
      </div>

      {/* Add form */}
      {showAddForm && renderAddForm()}

      {/* List */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {renderList()}
      </div>
    </div>
  )
}
