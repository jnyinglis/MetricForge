import { useState } from 'react'
import { useWorkspaceStore } from '../../hooks/useWorkspaceStore'

type SchemaSection = 'facts' | 'dimensions' | 'attributes' | 'joins'
type FactDraft = { name: string; table: string }
type DimensionDraft = { name: string; table: string }
type AttributeDraft = { name: string; table: string; column: string }
type JoinDraft = { fact: string; dimension: string; factKey: string; dimensionKey: string }

const EMPTY_FACT: FactDraft = { name: '', table: '' }
const EMPTY_DIMENSION: DimensionDraft = { name: '', table: '' }
const EMPTY_ATTRIBUTE: AttributeDraft = { name: '', table: '', column: '' }
const EMPTY_JOIN: JoinDraft = { fact: '', dimension: '', factKey: '', dimensionKey: '' }

export function SchemaEditor() {
  const schema = useWorkspaceStore((state) => state.schema)
  const setSchema = useWorkspaceStore((state) => state.setSchema)
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
  const [editError, setEditError] = useState<string | null>(null)

  // Add-form states
  const [newFact, setNewFact] = useState<FactDraft>(EMPTY_FACT)
  const [newDimension, setNewDimension] = useState<DimensionDraft>(EMPTY_DIMENSION)
  const [newAttribute, setNewAttribute] = useState<AttributeDraft>(EMPTY_ATTRIBUTE)
  const [newJoin, setNewJoin] = useState<JoinDraft>(EMPTY_JOIN)

  // Edit states
  const [editingFactName, setEditingFactName] = useState<string | null>(null)
  const [factDraft, setFactDraft] = useState<FactDraft>(EMPTY_FACT)
  const [editingDimensionName, setEditingDimensionName] = useState<string | null>(null)
  const [dimensionDraft, setDimensionDraft] = useState<DimensionDraft>(EMPTY_DIMENSION)
  const [editingAttributeName, setEditingAttributeName] = useState<string | null>(null)
  const [attributeDraft, setAttributeDraft] = useState<AttributeDraft>(EMPTY_ATTRIBUTE)
  const [editingJoinIndex, setEditingJoinIndex] = useState<number | null>(null)
  const [joinDraft, setJoinDraft] = useState<JoinDraft>(EMPTY_JOIN)

  const tableNames = tables.map((t) => t.name)

  const isEditing =
    editingFactName !== null ||
    editingDimensionName !== null ||
    editingAttributeName !== null ||
    editingJoinIndex !== null

  const clearEditState = () => {
    setEditingFactName(null)
    setFactDraft(EMPTY_FACT)
    setEditingDimensionName(null)
    setDimensionDraft(EMPTY_DIMENSION)
    setEditingAttributeName(null)
    setAttributeDraft(EMPTY_ATTRIBUTE)
    setEditingJoinIndex(null)
    setJoinDraft(EMPTY_JOIN)
    setEditError(null)
  }

  const getColumnsForTable = (tableName: string): string[] => {
    const table = tables.find((t) => t.name === tableName)
    return table ? table.columns.map((c) => c.name) : []
  }

  const handleAddFact = () => {
    if (newFact.name && newFact.table) {
      addFact(newFact)
      setNewFact(EMPTY_FACT)
      setShowAddForm(false)
    }
  }

  const handleAddDimension = () => {
    if (newDimension.name && newDimension.table) {
      addDimension(newDimension)
      setNewDimension(EMPTY_DIMENSION)
      setShowAddForm(false)
    }
  }

  const handleAddAttribute = () => {
    if (newAttribute.name && newAttribute.table && newAttribute.column) {
      addAttribute(newAttribute)
      setNewAttribute(EMPTY_ATTRIBUTE)
      setShowAddForm(false)
    }
  }

  const handleAddJoin = () => {
    if (newJoin.fact && newJoin.dimension && newJoin.factKey && newJoin.dimensionKey) {
      addJoin(newJoin)
      setNewJoin(EMPTY_JOIN)
      setShowAddForm(false)
    }
  }

  const startEditFact = (name: string, table: string) => {
    clearEditState()
    setShowAddForm(false)
    setEditingFactName(name)
    setFactDraft({ name, table })
  }

  const saveFactEdit = () => {
    if (!editingFactName || !factDraft.name || !factDraft.table) return
    const duplicateName = schema.facts.some(
      (fact) => fact.name === factDraft.name && fact.name !== editingFactName
    )
    if (duplicateName) {
      setEditError(`Fact "${factDraft.name}" already exists.`)
      return
    }

    const renamed = editingFactName !== factDraft.name
    setSchema({
      ...schema,
      facts: schema.facts.map((fact) =>
        fact.name === editingFactName ? { ...fact, ...factDraft } : fact
      ),
      joins: renamed
        ? schema.joins.map((join) =>
            join.fact === editingFactName ? { ...join, fact: factDraft.name } : join
          )
        : schema.joins,
    })
    clearEditState()
  }

  const startEditDimension = (name: string, table: string) => {
    clearEditState()
    setShowAddForm(false)
    setEditingDimensionName(name)
    setDimensionDraft({ name, table })
  }

  const saveDimensionEdit = () => {
    if (!editingDimensionName || !dimensionDraft.name || !dimensionDraft.table) return
    const duplicateName = schema.dimensions.some(
      (dimension) =>
        dimension.name === dimensionDraft.name && dimension.name !== editingDimensionName
    )
    if (duplicateName) {
      setEditError(`Dimension "${dimensionDraft.name}" already exists.`)
      return
    }

    const renamed = editingDimensionName !== dimensionDraft.name
    setSchema({
      ...schema,
      dimensions: schema.dimensions.map((dimension) =>
        dimension.name === editingDimensionName ? { ...dimension, ...dimensionDraft } : dimension
      ),
      joins: renamed
        ? schema.joins.map((join) =>
            join.dimension === editingDimensionName
              ? { ...join, dimension: dimensionDraft.name }
              : join
          )
        : schema.joins,
    })
    clearEditState()
  }

  const startEditAttribute = (name: string, table: string, column: string) => {
    clearEditState()
    setShowAddForm(false)
    setEditingAttributeName(name)
    setAttributeDraft({ name, table, column })
  }

  const saveAttributeEdit = () => {
    if (!editingAttributeName || !attributeDraft.name || !attributeDraft.table || !attributeDraft.column) return
    const duplicateName = schema.attributes.some(
      (attribute) =>
        attribute.name === attributeDraft.name && attribute.name !== editingAttributeName
    )
    if (duplicateName) {
      setEditError(`Attribute "${attributeDraft.name}" already exists.`)
      return
    }

    setSchema({
      ...schema,
      attributes: schema.attributes.map((attribute) =>
        attribute.name === editingAttributeName ? { ...attribute, ...attributeDraft } : attribute
      ),
    })
    clearEditState()
  }

  const startEditJoin = (index: number, join: JoinDraft) => {
    clearEditState()
    setShowAddForm(false)
    setEditingJoinIndex(index)
    setJoinDraft(join)
  }

  const saveJoinEdit = () => {
    if (
      editingJoinIndex === null ||
      !joinDraft.fact ||
      !joinDraft.dimension ||
      !joinDraft.factKey ||
      !joinDraft.dimensionKey
    ) {
      return
    }

    setSchema({
      ...schema,
      joins: schema.joins.map((join, index) => (index === editingJoinIndex ? joinDraft : join)),
    })
    clearEditState()
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
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddFact}>
                Add
              </button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>
                Cancel
              </button>
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
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddDimension}>
                Add
              </button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>
                Cancel
              </button>
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
                onChange={(e) =>
                  setNewAttribute({ ...newAttribute, table: e.target.value, column: '' })
                }
              >
                <option value="">Select table...</option>
                {tableNames.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
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
                  <option key={col} value={col}>
                    {col}
                  </option>
                ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddAttribute}>
                Add
              </button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>
                Cancel
              </button>
            </div>
          </div>
        )

      case 'joins': {
        const factTable = schema.facts.find((fact) => fact.name === newJoin.fact)?.table
        const dimTable = schema.dimensions.find((dimension) => dimension.name === newJoin.dimension)?.table
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
                {schema.facts.map((fact) => (
                  <option key={fact.name} value={fact.name}>
                    {fact.name}
                  </option>
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
                {factTable &&
                  getColumnsForTable(factTable).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
              </select>
            </div>
            <div className="form-group">
              <label className="form-label">Dimension</label>
              <select
                className="form-select"
                value={newJoin.dimension}
                onChange={(e) =>
                  setNewJoin({ ...newJoin, dimension: e.target.value, dimensionKey: '' })
                }
              >
                <option value="">Select dimension...</option>
                {schema.dimensions.map((dimension) => (
                  <option key={dimension.name} value={dimension.name}>
                    {dimension.name}
                  </option>
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
                {dimTable &&
                  getColumnsForTable(dimTable).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
              </select>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={handleAddJoin}>
                Add
              </button>
              <button className="btn btn-sm" onClick={() => setShowAddForm(false)}>
                Cancel
              </button>
            </div>
          </div>
        )
      }
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
            {schema.facts.map((fact) =>
              editingFactName === fact.name ? (
                <div key={fact.name} style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}>
                  <div className="form-group">
                    <label className="form-label">Name</label>
                    <input
                      className="form-input"
                      value={factDraft.name}
                      onChange={(e) => setFactDraft({ ...factDraft, name: e.target.value })}
                    />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Table</label>
                    <select
                      className="form-select"
                      value={factDraft.table}
                      onChange={(e) => setFactDraft({ ...factDraft, table: e.target.value })}
                    >
                      <option value="">Select table...</option>
                      {tableNames.map((name) => (
                        <option key={name} value={name}>
                          {name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary btn-sm" onClick={saveFactEdit}>
                      Save
                    </button>
                    <button className="btn btn-sm" onClick={clearEditState}>
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                <div key={fact.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                  <span style={{ flex: 1 }}>
                    <strong>{fact.name}</strong>
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>→ {fact.table}</span>
                  </span>
                  <button className="btn btn-sm" onClick={() => startEditFact(fact.name, fact.table)}>
                    Edit
                  </button>
                  <button className="btn btn-sm" onClick={() => removeFact(fact.name)}>
                    Delete
                  </button>
                </div>
              )
            )}
          </div>
        )

      case 'dimensions':
        return schema.dimensions.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No dimensions defined. Dimensions represent your lookup/reference tables.
          </div>
        ) : (
          <div>
            {schema.dimensions.map((dimension) =>
              editingDimensionName === dimension.name ? (
                <div
                  key={dimension.name}
                  style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}
                >
                  <div className="form-group">
                    <label className="form-label">Name</label>
                    <input
                      className="form-input"
                      value={dimensionDraft.name}
                      onChange={(e) => setDimensionDraft({ ...dimensionDraft, name: e.target.value })}
                    />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Table</label>
                    <select
                      className="form-select"
                      value={dimensionDraft.table}
                      onChange={(e) => setDimensionDraft({ ...dimensionDraft, table: e.target.value })}
                    >
                      <option value="">Select table...</option>
                      {tableNames.map((name) => (
                        <option key={name} value={name}>
                          {name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary btn-sm" onClick={saveDimensionEdit}>
                      Save
                    </button>
                    <button className="btn btn-sm" onClick={clearEditState}>
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                <div key={dimension.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                  <span style={{ flex: 1 }}>
                    <strong>{dimension.name}</strong>
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>
                      → {dimension.table}
                    </span>
                  </span>
                  <button
                    className="btn btn-sm"
                    onClick={() => startEditDimension(dimension.name, dimension.table)}
                  >
                    Edit
                  </button>
                  <button className="btn btn-sm" onClick={() => removeDimension(dimension.name)}>
                    Delete
                  </button>
                </div>
              )
            )}
          </div>
        )

      case 'attributes':
        return schema.attributes.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No attributes defined. Attributes map logical names to physical columns.
          </div>
        ) : (
          <div>
            {schema.attributes.map((attribute) =>
              editingAttributeName === attribute.name ? (
                <div
                  key={attribute.name}
                  style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}
                >
                  <div className="form-group">
                    <label className="form-label">Logical Name</label>
                    <input
                      className="form-input"
                      value={attributeDraft.name}
                      onChange={(e) => setAttributeDraft({ ...attributeDraft, name: e.target.value })}
                    />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Table</label>
                    <select
                      className="form-select"
                      value={attributeDraft.table}
                      onChange={(e) =>
                        setAttributeDraft({ ...attributeDraft, table: e.target.value, column: '' })
                      }
                    >
                      <option value="">Select table...</option>
                      {tableNames.map((name) => (
                        <option key={name} value={name}>
                          {name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Column</label>
                    <select
                      className="form-select"
                      value={attributeDraft.column}
                      onChange={(e) => setAttributeDraft({ ...attributeDraft, column: e.target.value })}
                      disabled={!attributeDraft.table}
                    >
                      <option value="">Select column...</option>
                      {getColumnsForTable(attributeDraft.table).map((col) => (
                        <option key={col} value={col}>
                          {col}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary btn-sm" onClick={saveAttributeEdit}>
                      Save
                    </button>
                    <button className="btn btn-sm" onClick={clearEditState}>
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                <div key={attribute.name} className="sidebar-item" style={{ padding: '8px 12px' }}>
                  <span style={{ flex: 1 }}>
                    <strong>{attribute.name}</strong>
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>
                      → {attribute.table}.{attribute.column}
                    </span>
                  </span>
                  <button
                    className="btn btn-sm"
                    onClick={() => startEditAttribute(attribute.name, attribute.table, attribute.column)}
                  >
                    Edit
                  </button>
                  <button className="btn btn-sm" onClick={() => removeAttribute(attribute.name)}>
                    Delete
                  </button>
                </div>
              )
            )}
          </div>
        )

      case 'joins':
        return schema.joins.length === 0 ? (
          <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
            No joins defined. Joins connect facts to dimensions.
          </div>
        ) : (
          <div>
            {schema.joins.map((join, index) =>
              editingJoinIndex === index ? (
                <div
                  key={`${join.fact}:${join.dimension}:${index}`}
                  style={{ padding: 12, borderBottom: '1px solid var(--border-color)' }}
                >
                  <div className="form-group">
                    <label className="form-label">Fact</label>
                    <select
                      className="form-select"
                      value={joinDraft.fact}
                      onChange={(e) => setJoinDraft({ ...joinDraft, fact: e.target.value, factKey: '' })}
                    >
                      <option value="">Select fact...</option>
                      {schema.facts.map((fact) => (
                        <option key={fact.name} value={fact.name}>
                          {fact.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Fact Key Column</label>
                    <select
                      className="form-select"
                      value={joinDraft.factKey}
                      onChange={(e) => setJoinDraft({ ...joinDraft, factKey: e.target.value })}
                      disabled={!joinDraft.fact}
                    >
                      <option value="">Select column...</option>
                      {schema.facts
                        .find((fact) => fact.name === joinDraft.fact)
                        ?.table &&
                        getColumnsForTable(schema.facts.find((fact) => fact.name === joinDraft.fact)!.table).map(
                          (col) => (
                            <option key={col} value={col}>
                              {col}
                            </option>
                          )
                        )}
                    </select>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Dimension</label>
                    <select
                      className="form-select"
                      value={joinDraft.dimension}
                      onChange={(e) =>
                        setJoinDraft({ ...joinDraft, dimension: e.target.value, dimensionKey: '' })
                      }
                    >
                      <option value="">Select dimension...</option>
                      {schema.dimensions.map((dimension) => (
                        <option key={dimension.name} value={dimension.name}>
                          {dimension.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Dimension Key Column</label>
                    <select
                      className="form-select"
                      value={joinDraft.dimensionKey}
                      onChange={(e) => setJoinDraft({ ...joinDraft, dimensionKey: e.target.value })}
                      disabled={!joinDraft.dimension}
                    >
                      <option value="">Select column...</option>
                      {schema.dimensions
                        .find((dimension) => dimension.name === joinDraft.dimension)
                        ?.table &&
                        getColumnsForTable(
                          schema.dimensions.find((dimension) => dimension.name === joinDraft.dimension)!.table
                        ).map((col) => (
                          <option key={col} value={col}>
                            {col}
                          </option>
                        ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <button className="btn btn-primary btn-sm" onClick={saveJoinEdit}>
                      Save
                    </button>
                    <button className="btn btn-sm" onClick={clearEditState}>
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                <div key={`${join.fact}:${join.dimension}:${index}`} className="sidebar-item" style={{ padding: '8px 12px' }}>
                  <span style={{ flex: 1 }}>
                    <span style={{ color: 'var(--info)' }}>{join.fact}</span>
                    <span style={{ color: 'var(--text-muted)' }}>.{join.factKey}</span>
                    <span style={{ margin: '0 8px' }}>→</span>
                    <span style={{ color: 'var(--success)' }}>{join.dimension}</span>
                    <span style={{ color: 'var(--text-muted)' }}>.{join.dimensionKey}</span>
                  </span>
                  <button className="btn btn-sm" onClick={() => startEditJoin(index, join)}>
                    Edit
                  </button>
                  <button className="btn btn-sm" onClick={() => removeJoin(index)}>
                    Delete
                  </button>
                </div>
              )
            )}
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
            onClick={() => {
              setActiveSection(section)
              setShowAddForm(false)
              clearEditState()
            }}
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
          onClick={() => {
            clearEditState()
            setShowAddForm(!showAddForm)
          }}
          disabled={isEditing}
        >
          {showAddForm ? 'Cancel' : '+ Add'}
        </button>
      </div>

      {editError && (
        <div
          style={{
            padding: '8px 12px',
            borderBottom: '1px solid var(--border-color)',
            color: 'var(--error)',
            fontSize: 12,
          }}
        >
          {editError}
        </div>
      )}

      {/* Add form */}
      {showAddForm && renderAddForm()}

      {/* List */}
      <div style={{ flex: 1, overflow: 'auto' }}>{renderList()}</div>
    </div>
  )
}
