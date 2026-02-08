import {
  buildLogicalPlan,
  compileDslToModel,
  parseDslWithDiagnostics,
  runSemanticQuery,
  type DslDiagnostic,
  type DslFileAst,
  type InMemoryDb,
  type LogicalQueryPlan,
  type SemanticModel,
} from '../../../src/browser'
import type {
  MetricDefinition,
  ParseError,
  QueryResult,
  SchemaDefinition,
  TableData,
} from '../types/workspace'

interface QuerySpecExtractionResult {
  ast: DslFileAst | null
  errors: ParseError[]
}

interface BuildModelResult {
  model: SemanticModel | null
  errors: ParseError[]
}

interface BuildPlanResult {
  plan: LogicalQueryPlan | null
  errors: ParseError[]
}

function mapDiagnostics(errors: DslDiagnostic[]): ParseError[] {
  return errors.map((error) => ({
    message: error.message,
    line: error.line,
    column: error.column,
    severity: error.severity,
  }))
}

function normalizeMetricDsl(metric: MetricDefinition, defaultFact: string): string {
  const trimmed = metric.dsl.trim()
  const fullDeclMatch = /^metric\s+\w+\s+on\s+(\w+)\s*=\s*([\s\S]+)$/i.exec(trimmed)
  if (fullDeclMatch) {
    const baseFact = fullDeclMatch[1]
    const expression = fullDeclMatch[2]
    return `metric ${metric.name} on ${baseFact} = ${expression}`
  }

  return `metric ${metric.name} on ${defaultFact} = ${trimmed}`
}

function buildBaseModel(schema: SchemaDefinition): SemanticModel {
  const facts: SemanticModel['facts'] = {}
  schema.facts.forEach((fact) => {
    facts[fact.name] = { table: fact.table }
  })

  const dimensions: SemanticModel['dimensions'] = {}
  schema.dimensions.forEach((dimension) => {
    dimensions[dimension.name] = { table: dimension.table }
  })

  const attributes: SemanticModel['attributes'] = {}
  schema.attributes.forEach((attribute) => {
    attributes[attribute.name] = { table: attribute.table, column: attribute.column }
  })

  return {
    facts,
    dimensions,
    attributes,
    joins: [...schema.joins],
    metrics: {},
  }
}

function toInMemoryDb(tables: TableData[]): InMemoryDb {
  const db: InMemoryDb = { tables: {} }
  tables.forEach((table) => {
    db.tables[table.name] = table.rows as Record<string, unknown>[]
  })
  return db
}

function resolveDefaultFact(schema: SchemaDefinition): string {
  return schema.facts[0]?.name ?? 'default'
}

function extractQuerySpec(queryDsl: string): QuerySpecExtractionResult {
  const parsed = parseDslWithDiagnostics(queryDsl)
  const errors = mapDiagnostics(parsed.errors)
  if (errors.length > 0 || !parsed.ast || parsed.ast.queries.length === 0) {
    return {
      ast: parsed.ast,
      errors:
        errors.length > 0
          ? errors
          : [{ message: 'No query declaration found', line: 1, column: 1, severity: 'error' }],
    }
  }

  return { ast: parsed.ast, errors: [] }
}

function buildModel(schema: SchemaDefinition, metrics: MetricDefinition[]): BuildModelResult {
  const baseModel = buildBaseModel(schema)
  const defaultFact = resolveDefaultFact(schema)
  const validMetrics = metrics.filter((metric) => metric.valid && metric.dsl.trim().length > 0)

  if (validMetrics.length === 0) {
    return { model: baseModel, errors: [] }
  }

  const dsl = validMetrics.map((metric) => normalizeMetricDsl(metric, defaultFact)).join('\n')
  try {
    const { model } = compileDslToModel(dsl, baseModel)
    return { model, errors: [] }
  } catch (error) {
    return {
      model: null,
      errors: [
        {
          message: error instanceof Error ? error.message : 'Failed to compile metric model',
          line: 1,
          column: 1,
          severity: 'error',
        },
      ],
    }
  }
}

export function runWorkspaceQuery(
  queryName: string,
  queryDsl: string,
  tables: TableData[],
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): QueryResult {
  const startTime = performance.now()

  try {
    const parsedQuery = extractQuerySpec(queryDsl)
    if (parsedQuery.errors.length > 0 || !parsedQuery.ast) {
      return {
        queryName,
        rows: [],
        columns: [],
        executionTime: performance.now() - startTime,
        error: parsedQuery.errors[0].message,
      }
    }

    const modelResult = buildModel(schema, metrics)
    if (modelResult.errors.length > 0 || !modelResult.model) {
      return {
        queryName,
        rows: [],
        columns: [],
        executionTime: performance.now() - startTime,
        error: modelResult.errors[0].message,
      }
    }

    const spec = parsedQuery.ast.queries[0].spec
    const rows = runSemanticQuery(
      {
        db: toInMemoryDb(tables),
        model: modelResult.model,
      },
      spec
    )

    return {
      queryName,
      rows,
      columns: [...spec.dimensions, ...spec.metrics],
      executionTime: performance.now() - startTime,
    }
  } catch (error) {
    return {
      queryName,
      rows: [],
      columns: [],
      executionTime: performance.now() - startTime,
      error: error instanceof Error ? error.message : 'Unknown query execution error',
    }
  }
}

export function buildWorkspacePlan(
  queryDsl: string,
  schema: SchemaDefinition,
  metrics: MetricDefinition[]
): BuildPlanResult {
  const parsedQuery = extractQuerySpec(queryDsl)
  if (parsedQuery.errors.length > 0 || !parsedQuery.ast) {
    return { plan: null, errors: parsedQuery.errors }
  }

  const modelResult = buildModel(schema, metrics)
  if (modelResult.errors.length > 0 || !modelResult.model) {
    return { plan: null, errors: modelResult.errors }
  }

  try {
    const plan = buildLogicalPlan(parsedQuery.ast.queries[0].spec, modelResult.model)
    return { plan, errors: [] }
  } catch (error) {
    return {
      plan: null,
      errors: [
        {
          message: error instanceof Error ? error.message : 'Failed to build logical plan',
          line: 1,
          column: 1,
          severity: 'error',
        },
      ],
    }
  }
}
