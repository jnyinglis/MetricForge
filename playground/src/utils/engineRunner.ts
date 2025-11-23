// Engine runner - executes queries client-side using the semantic engine logic

import type {
  SchemaDefinition,
  MetricDefinition,
  QueryResult,
  TableData,
} from '../types/workspace'
import type { MetricExpr, QuerySpecAst, FilterNode } from './parserAdapter'
import { parseDsl } from './parserAdapter'

// Row type
type Row = Record<string, unknown>

// Simple LINQ-like operations for browser
class Enumerable<T> {
  constructor(private items: T[] | (() => Generator<T>)) {}

  static from<T>(items: T[]): Enumerable<T> {
    return new Enumerable(items)
  }

  toArray(): T[] {
    if (Array.isArray(this.items)) {
      return [...this.items]
    }
    return [...this.items()]
  }

  where(predicate: (item: T) => boolean): Enumerable<T> {
    const items = this.toArray()
    return new Enumerable(items.filter(predicate))
  }

  select<U>(selector: (item: T) => U): Enumerable<U> {
    const items = this.toArray()
    return new Enumerable(items.map(selector))
  }

  groupBy<K>(keySelector: (item: T) => K): Map<K, T[]> {
    const items = this.toArray()
    const map = new Map<K, T[]>()
    for (const item of items) {
      const key = keySelector(item)
      const keyStr = JSON.stringify(key)
      let group = map.get(keyStr as unknown as K)
      if (!group) {
        group = []
        map.set(keyStr as unknown as K, group)
      }
      group.push(item)
    }
    return map
  }

  join<U, K, R>(
    inner: Enumerable<U>,
    outerKeySelector: (item: T) => K,
    innerKeySelector: (item: U) => K,
    resultSelector: (outer: T, inner: U) => R
  ): Enumerable<R> {
    const outerItems = this.toArray()
    const innerItems = inner.toArray()
    const results: R[] = []

    // Build lookup for inner items
    const innerLookup = new Map<string, U[]>()
    for (const innerItem of innerItems) {
      const key = JSON.stringify(innerKeySelector(innerItem))
      let items = innerLookup.get(key)
      if (!items) {
        items = []
        innerLookup.set(key, items)
      }
      items.push(innerItem)
    }

    // Join
    for (const outerItem of outerItems) {
      const key = JSON.stringify(outerKeySelector(outerItem))
      const matchingInner = innerLookup.get(key) || []
      for (const innerItem of matchingInner) {
        results.push(resultSelector(outerItem, innerItem))
      }
    }

    return new Enumerable(results)
  }

  count(): number {
    return this.toArray().length
  }

  sum(selector: (item: T) => number): number {
    return this.toArray().reduce((acc, item) => acc + (selector(item) || 0), 0)
  }

  avg(selector: (item: T) => number): number {
    const items = this.toArray()
    if (items.length === 0) return 0
    return this.sum(selector) / items.length
  }

  min(selector: (item: T) => number): number {
    const items = this.toArray()
    if (items.length === 0) return 0
    return Math.min(...items.map(selector))
  }

  max(selector: (item: T) => number): number {
    const items = this.toArray()
    if (items.length === 0) return 0
    return Math.max(...items.map(selector))
  }
}

// Build in-memory database from tables
interface InMemoryDb {
  tables: Record<string, Row[]>
}

function buildDb(tables: TableData[]): InMemoryDb {
  const db: InMemoryDb = { tables: {} }
  for (const table of tables) {
    db.tables[table.name] = table.rows as Row[]
  }
  return db
}

// Build semantic model from schema
interface SemanticModel {
  facts: Record<string, { table: string }>
  dimensions: Record<string, { table: string }>
  attributes: Record<string, { table: string; column: string }>
  joins: Array<{ fact: string; dimension: string; factKey: string; dimensionKey: string }>
  metrics: Record<string, CompiledMetric>
}

interface CompiledMetric {
  name: string
  baseFact?: string
  eval: (ctx: MetricContext) => number | undefined
}

interface MetricContext {
  rows: Row[]
  groupKey: Record<string, unknown>
  evalMetric: (name: string) => number | undefined
}

function buildModel(schema: SchemaDefinition, metrics: MetricDefinition[]): SemanticModel {
  const model: SemanticModel = {
    facts: {},
    dimensions: {},
    attributes: {},
    joins: [],
    metrics: {},
  }

  // Add facts
  for (const fact of schema.facts) {
    model.facts[fact.name] = { table: fact.table }
  }

  // Add dimensions
  for (const dim of schema.dimensions) {
    model.dimensions[dim.name] = { table: dim.table }
  }

  // Add attributes
  for (const attr of schema.attributes) {
    model.attributes[attr.name] = { table: attr.table, column: attr.column }
  }

  // Add joins
  model.joins = [...schema.joins]

  // Compile metrics
  for (const metric of metrics) {
    if (metric.dsl && metric.valid) {
      const compiled = compileMetric(metric.name, metric.dsl)
      if (compiled) {
        model.metrics[metric.name] = compiled
      }
    }
  }

  return model
}

function compileMetric(name: string, dsl: string): CompiledMetric | null {
  // Try parsing as a metric declaration
  const metricDeclMatch = dsl.match(/^metric\s+\w+\s+on\s+(\w+)\s*=\s*(.+)$/s)
  if (metricDeclMatch) {
    const [, baseFact, exprStr] = metricDeclMatch
    return compileMetricExpr(name, baseFact, exprStr.trim())
  }

  // Try parsing as just an expression
  return compileMetricExpr(name, undefined, dsl.trim())
}

function compileMetricExpr(
  name: string,
  baseFact: string | undefined,
  exprStr: string
): CompiledMetric | null {
  const { ast, errors } = parseDsl(`metric ${name} on ${baseFact || 'default'} = ${exprStr}`)
  if (errors.length > 0 || !ast || ast.metrics.length === 0) {
    // Try as raw expression
    const expr = parseSimpleExpr(exprStr)
    if (!expr) return null

    return {
      name,
      baseFact,
      eval: (ctx) => evalExpr(expr, ctx),
    }
  }

  const metricAst = ast.metrics[0]
  return {
    name,
    baseFact: metricAst.baseFact,
    eval: (ctx) => evalExpr(metricAst.expr, ctx),
  }
}

function parseSimpleExpr(text: string): MetricExpr | null {
  // Simple expression parsing for standalone expressions
  const trimmed = text.trim()

  // Number literal
  const numMatch = trimmed.match(/^-?\d+(?:\.\d+)?$/)
  if (numMatch) {
    return { kind: 'Literal', value: Number(trimmed) }
  }

  // Function call
  const funcMatch = trimmed.match(/^(\w+)\s*\(\s*(\*|\w+)\s*\)$/)
  if (funcMatch) {
    const [, fn, arg] = funcMatch
    return {
      kind: 'Call',
      fn,
      args: [{ kind: 'AttrRef', name: arg }],
    }
  }

  // Simple identifier
  const identMatch = trimmed.match(/^[A-Za-z_][A-Za-z0-9_]*$/)
  if (identMatch) {
    return { kind: 'AttrRef', name: trimmed }
  }

  // Binary expression (simple)
  const binMatch = trimmed.match(/^(.+?)\s*([\+\-\*\/])\s*(.+)$/)
  if (binMatch) {
    const [, left, op, right] = binMatch
    const leftExpr = parseSimpleExpr(left)
    const rightExpr = parseSimpleExpr(right)
    if (leftExpr && rightExpr) {
      return {
        kind: 'BinaryOp',
        op: op as '+' | '-' | '*' | '/',
        left: leftExpr,
        right: rightExpr,
      }
    }
  }

  return null
}

function evalExpr(expr: MetricExpr, ctx: MetricContext): number | undefined {
  switch (expr.kind) {
    case 'Literal':
      return expr.value

    case 'AttrRef': {
      // For aggregate context, this shouldn't happen directly
      // But for simple cases, return the first row's value
      if (ctx.rows.length > 0) {
        const val = ctx.rows[0][expr.name]
        return typeof val === 'number' ? val : undefined
      }
      return undefined
    }

    case 'MetricRef':
      return ctx.evalMetric(expr.name)

    case 'BinaryOp': {
      const left = evalExpr(expr.left, ctx)
      const right = evalExpr(expr.right, ctx)
      if (left === undefined || right === undefined) return undefined
      switch (expr.op) {
        case '+':
          return left + right
        case '-':
          return left - right
        case '*':
          return left * right
        case '/':
          return right !== 0 ? left / right : undefined
      }
      return undefined
    }

    case 'Call': {
      const fn = expr.fn.toLowerCase()
      const arg = expr.args[0]

      if (!arg) {
        if (fn === 'count') {
          return ctx.rows.length
        }
        return undefined
      }

      if (arg.kind === 'AttrRef') {
        const attrName = arg.name
        if (attrName === '*' && fn === 'count') {
          return ctx.rows.length
        }

        const values = ctx.rows.map((r) => r[attrName]).filter((v) => typeof v === 'number') as number[]

        switch (fn) {
          case 'sum':
            return values.reduce((a, b) => a + b, 0)
          case 'avg':
            return values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : undefined
          case 'min':
            return values.length > 0 ? Math.min(...values) : undefined
          case 'max':
            return values.length > 0 ? Math.max(...values) : undefined
          case 'count':
            return ctx.rows.length
        }
      }

      return undefined
    }
  }
}

// Filter evaluation
function evalFilter(filter: FilterNode | undefined, row: Row): boolean {
  if (!filter) return true

  switch (filter.kind) {
    case 'expression': {
      const value = row[filter.field]
      const target = filter.value

      switch (filter.op) {
        case '==':
        case 'eq':
          return value === target
        case '!=':
          return value !== target
        case '>':
        case 'gt':
          return typeof value === 'number' && typeof target === 'number' && value > target
        case '>=':
        case 'gte':
          return typeof value === 'number' && typeof target === 'number' && value >= target
        case '<':
        case 'lt':
          return typeof value === 'number' && typeof target === 'number' && value < target
        case '<=':
        case 'lte':
          return typeof value === 'number' && typeof target === 'number' && value <= target
        default:
          return true
      }
    }

    case 'and':
      return filter.filters.every((f) => evalFilter(f, row))

    case 'or':
      return filter.filters.some((f) => evalFilter(f, row))

    default:
      return true
  }
}

// Evaluate having clause
function evalHaving(
  having: QuerySpecAst['having'],
  metricValues: Record<string, number | undefined>
): boolean {
  if (!having) return true

  switch (having.kind) {
    case 'MetricCmp': {
      const value = metricValues[having.metric]
      if (value === undefined) return false

      switch (having.op) {
        case '>':
          return value > having.value
        case '>=':
          return value >= having.value
        case '<':
          return value < having.value
        case '<=':
          return value <= having.value
        case '==':
          return value === having.value
        case '!=':
          return value !== having.value
        default:
          return true
      }
    }

    case 'And':
      return having.items.every((item) => evalHaving(item, metricValues))

    case 'Or':
      return having.items.some((item) => evalHaving(item, metricValues))

    default:
      return true
  }
}

// Run query
export function runQuery(
  queryName: string,
  queryDsl: string,
  tables: TableData[],
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): QueryResult {
  const startTime = performance.now()

  try {
    // Parse query DSL
    const { ast, errors } = parseDsl(queryDsl)
    if (errors.length > 0 || !ast || ast.queries.length === 0) {
      return {
        queryName,
        rows: [],
        columns: [],
        executionTime: performance.now() - startTime,
        error: errors.length > 0 ? errors[0].message : 'Failed to parse query',
      }
    }

    const queryAst = ast.queries[0]
    const spec = queryAst.spec

    // Build database and model
    const db = buildDb(tables)
    const model = buildModel(schema, metrics)

    // Resolve dimensions to table columns
    const dimensionColumns: Array<{ name: string; table: string; column: string }> = []
    for (const dimName of spec.dimensions) {
      const attr = model.attributes[dimName]
      if (attr) {
        dimensionColumns.push({ name: dimName, table: attr.table, column: attr.column })
      }
    }

    // Find base fact for metrics
    let baseFact: string | undefined
    for (const metricName of spec.metrics) {
      const metric = model.metrics[metricName]
      if (metric?.baseFact && model.facts[metric.baseFact]) {
        baseFact = metric.baseFact
        break
      }
    }

    // Get base table
    let baseTable: string
    if (baseFact && model.facts[baseFact]) {
      baseTable = model.facts[baseFact].table
    } else if (Object.keys(model.facts).length > 0) {
      const firstFact = Object.values(model.facts)[0]
      baseTable = firstFact.table
      baseFact = Object.keys(model.facts)[0]
    } else if (tables.length > 0) {
      baseTable = tables[0].name
    } else {
      return {
        queryName,
        rows: [],
        columns: [],
        executionTime: performance.now() - startTime,
        error: 'No tables available',
      }
    }

    // Get base rows
    let rows = Enumerable.from(db.tables[baseTable] || [])

    // Join dimensions
    for (const join of model.joins) {
      if (join.fact === baseFact) {
        const dimDef = model.dimensions[join.dimension]
        if (dimDef) {
          const dimRows = Enumerable.from(db.tables[dimDef.table] || [])
          rows = rows.join(
            dimRows,
            (r) => r[join.factKey],
            (d) => d[join.dimensionKey],
            (fact, dim) => ({ ...fact, ...dim })
          )
        }
      }
    }

    // Apply where filter
    if (spec.where) {
      rows = rows.where((row) => evalFilter(spec.where, row))
    }

    // Group by dimensions
    const allRows = rows.toArray()
    const groups = new Map<string, Row[]>()

    for (const row of allRows) {
      const keyParts: Record<string, unknown> = {}
      for (const dim of dimensionColumns) {
        keyParts[dim.name] = row[dim.column]
      }
      const keyStr = JSON.stringify(keyParts)

      let group = groups.get(keyStr)
      if (!group) {
        group = []
        groups.set(keyStr, group)
      }
      group.push(row)
    }

    // Compute metrics for each group
    const resultRows: Row[] = []
    const columns = [...spec.dimensions, ...spec.metrics]

    for (const [keyStr, groupRows] of groups) {
      const groupKey = JSON.parse(keyStr) as Record<string, unknown>

      // Create metric evaluation context
      const metricCache: Record<string, number | undefined> = {}
      const evalMetric = (name: string): number | undefined => {
        if (name in metricCache) return metricCache[name]

        const metric = model.metrics[name]
        if (!metric) return undefined

        const ctx: MetricContext = {
          rows: groupRows,
          groupKey,
          evalMetric,
        }

        const value = metric.eval(ctx)
        metricCache[name] = value
        return value
      }

      // Build result row
      const resultRow: Row = { ...groupKey }

      // Compute each metric
      const metricValues: Record<string, number | undefined> = {}
      for (const metricName of spec.metrics) {
        const value = evalMetric(metricName)
        resultRow[metricName] = value
        metricValues[metricName] = value
      }

      // Apply having filter
      if (evalHaving(spec.having, metricValues)) {
        resultRows.push(resultRow)
      }
    }

    return {
      queryName,
      rows: resultRows,
      columns,
      executionTime: performance.now() - startTime,
    }
  } catch (e) {
    return {
      queryName,
      rows: [],
      columns: [],
      executionTime: performance.now() - startTime,
      error: e instanceof Error ? e.message : 'Unknown error',
    }
  }
}

// Run a simple query without parsing DSL
export function runSimpleQuery(
  tables: TableData[],
  _schema: SchemaDefinition,
  _metrics: MetricDefinition[],
  dimensions: string[],
  metricNames: string[],
  metricDefs: Record<string, string>
): QueryResult {
  const startTime = performance.now()

  try {
    if (tables.length === 0) {
      return {
        queryName: 'simple',
        rows: [],
        columns: [],
        executionTime: performance.now() - startTime,
        error: 'No tables available',
      }
    }

    // Use first table as source
    const sourceTable = tables[0]
    const rows = sourceTable.rows as Row[]

    // Group by dimensions
    const groups = new Map<string, Row[]>()

    for (const row of rows) {
      const keyParts: Record<string, unknown> = {}
      for (const dim of dimensions) {
        keyParts[dim] = row[dim]
      }
      const keyStr = JSON.stringify(keyParts)

      let group = groups.get(keyStr)
      if (!group) {
        group = []
        groups.set(keyStr, group)
      }
      group.push(row)
    }

    // Compute metrics for each group
    const resultRows: Row[] = []
    const columns = [...dimensions, ...metricNames]

    // Compile metric expressions
    const compiledMetrics: Record<string, CompiledMetric> = {}
    for (const name of metricNames) {
      const dsl = metricDefs[name]
      if (dsl) {
        const compiled = compileMetric(name, dsl)
        if (compiled) {
          compiledMetrics[name] = compiled
        }
      }
    }

    for (const [keyStr, groupRows] of groups) {
      const groupKey = JSON.parse(keyStr) as Record<string, unknown>

      const resultRow: Row = { ...groupKey }

      // Metric evaluation context
      const metricCache: Record<string, number | undefined> = {}
      const evalMetricFn = (name: string): number | undefined => {
        if (name in metricCache) return metricCache[name]

        const metric = compiledMetrics[name]
        if (!metric) return undefined

        const ctx: MetricContext = {
          rows: groupRows,
          groupKey,
          evalMetric: evalMetricFn,
        }

        const value = metric.eval(ctx)
        metricCache[name] = value
        return value
      }

      for (const metricName of metricNames) {
        resultRow[metricName] = evalMetricFn(metricName)
      }

      resultRows.push(resultRow)
    }

    return {
      queryName: 'simple',
      rows: resultRows,
      columns,
      executionTime: performance.now() - startTime,
    }
  } catch (e) {
    return {
      queryName: 'simple',
      rows: [],
      columns: [],
      executionTime: performance.now() - startTime,
      error: e instanceof Error ? e.message : 'Unknown error',
    }
  }
}
