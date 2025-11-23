// Parser adapter for the playground - browser-compatible DSL parser

import type { ParseError } from '../types/workspace'

// Re-implement core parser types and functions for browser use
export type ParseResult<T> = { value: T; nextPos: number }
export type Parser<T> = (input: string, pos: number) => ParseResult<T> | null

// AST types
export type MetricExpr =
  | { kind: 'Literal'; value: number }
  | { kind: 'AttrRef'; name: string }
  | { kind: 'MetricRef'; name: string }
  | { kind: 'Call'; fn: string; args: MetricExpr[] }
  | { kind: 'BinaryOp'; op: '+' | '-' | '*' | '/'; left: MetricExpr; right: MetricExpr }

export interface MetricDeclAst {
  name: string
  baseFact?: string
  expr: MetricExpr
}

export interface QuerySpecAst {
  dimensions: string[]
  metrics: string[]
  where?: FilterNode
  having?: MetricHavingAst
}

export interface QueryAst {
  name: string
  spec: QuerySpecAst
}

export interface DslFileAst {
  metrics: MetricDeclAst[]
  queries: QueryAst[]
}

export type FilterNode =
  | { kind: 'expression'; field: string; op: string; value: unknown }
  | { kind: 'and' | 'or'; filters: FilterNode[] }

export type MetricHavingAst =
  | { kind: 'MetricCmp'; metric: string; op: string; value: number }
  | { kind: 'And' | 'Or'; items: MetricHavingAst[] }

// Parser combinators
function skipWs(input: string, pos: number): number {
  const match = /^\s*/.exec(input.slice(pos))
  return pos + (match ? match[0].length : 0)
}

function map<A, B>(parser: Parser<A>, fn: (value: A) => B): Parser<B> {
  return (input, pos) => {
    const result = parser(input, pos)
    if (!result) return null
    return { value: fn(result.value), nextPos: result.nextPos }
  }
}

function seq<T extends unknown[]>(...parsers: { [K in keyof T]: Parser<T[K]> }): Parser<T> {
  return (input, pos) => {
    const values: unknown[] = []
    let nextPos = pos
    for (const p of parsers) {
      const result = p(input, nextPos)
      if (!result) return null
      values.push(result.value)
      nextPos = result.nextPos
    }
    return { value: values as T, nextPos }
  }
}

function choice<T>(...parsers: Parser<T>[]): Parser<T> {
  return (input, pos) => {
    for (const p of parsers) {
      const result = p(input, pos)
      if (result) return result
    }
    return null
  }
}

function opt<T>(parser: Parser<T>): Parser<T | null> {
  return (input, pos) => {
    const result = parser(input, pos)
    if (!result) return { value: null, nextPos: pos }
    return result
  }
}

function regex(re: RegExp): Parser<string> {
  const anchored = new RegExp('^(?:' + re.source + ')', re.flags)
  return (input, pos) => {
    const start = skipWs(input, pos)
    const slice = input.slice(start)
    const match = anchored.exec(slice)
    if (!match) return null
    const nextPos = skipWs(input, start + match[0].length)
    return { value: match[0], nextPos }
  }
}

function token(text: string): Parser<string> {
  return (input, pos) => {
    const start = skipWs(input, pos)
    if (input.slice(start).startsWith(text)) {
      const nextPos = skipWs(input, start + text.length)
      return { value: text, nextPos }
    }
    return null
  }
}

function symbol(text: string): Parser<string> {
  return token(text)
}

function keyword(word: string): Parser<string> {
  const re = new RegExp(word + '(?![A-Za-z0-9_])')
  return regex(re)
}

function between<A>(left: Parser<unknown>, parser: Parser<A>, right: Parser<unknown>): Parser<A> {
  return map(seq(left, parser, right), ([, value]) => value as A)
}

function sepBy<T>(parser: Parser<T>, separator: Parser<unknown>): Parser<T[]> {
  return (input, pos) => {
    const first = parser(input, pos)
    if (!first) return { value: [], nextPos: pos }
    const values: T[] = [first.value]
    let nextPos = first.nextPos
    while (true) {
      const sep = separator(input, nextPos)
      if (!sep) break
      const next = parser(input, sep.nextPos)
      if (!next) break
      values.push(next.value)
      nextPos = next.nextPos
    }
    return { value: values, nextPos }
  }
}

function lazy<T>(fn: () => Parser<T>): Parser<T> {
  return (input, pos) => fn()(input, pos)
}

function chainLeft<T>(
  parser: Parser<T>,
  opParser: Parser<string>,
  combine: (left: T, op: string, right: T) => T
): Parser<T> {
  return (input, pos) => {
    let result = parser(input, pos)
    if (!result) return null
    let value = result.value
    let nextPos = result.nextPos
    while (true) {
      const op = opParser(input, nextPos)
      if (!op) break
      const right = parser(input, op.nextPos)
      if (!right) break
      value = combine(value, op.value, right.value)
      nextPos = right.nextPos
    }
    return { value, nextPos }
  }
}

function parens<T>(parser: Parser<T>): Parser<T> {
  return between(symbol('('), parser, symbol(')'))
}

// Lexer helpers
const identifier: Parser<string> = map(regex(/[A-Za-z_][A-Za-z0-9_]*/), (v) => v)
const numberLiteral: Parser<number> = map(regex(/-?\d+(?:\.\d+)?/), (v) => Number(v))
const stringLiteral: Parser<string> = (input, pos) => {
  const start = skipWs(input, pos)
  const slice = input.slice(start)
  const match = /^"([^"]*)"/.exec(slice) || /^'([^']*)'/.exec(slice)
  if (!match) return null
  const nextPos = skipWs(input, start + match[0].length)
  return { value: match[1], nextPos }
}

// Expression parser
const expr: Parser<MetricExpr> = lazy(() => additive)

const argList: Parser<MetricExpr[]> = sepBy(lazy(() => expr), symbol(','))

const functionCall: Parser<MetricExpr> = (input, pos) => {
  const nameRes = identifier(input, pos)
  if (!nameRes) return null
  const lp = symbol('(')(input, nameRes.nextPos)
  if (!lp) return null

  const fn = nameRes.value
  let nextPos = lp.nextPos
  let args: MetricExpr[] = []

  if (fn === 'last_year') {
    const metricArg = identifier(input, nextPos)
    if (!metricArg) return null
    args.push({ kind: 'MetricRef', name: metricArg.value })
    nextPos = metricArg.nextPos

    const comma = opt(symbol(','))(input, nextPos)
    if (comma) nextPos = comma.nextPos

    const byKw = keyword('by')(input, nextPos)
    if (!byKw) return null
    const anchor = identifier(input, byKw.nextPos)
    if (!anchor) return null
    args.push({ kind: 'AttrRef', name: anchor.value })
    nextPos = anchor.nextPos
  } else {
    const starArg = symbol('*')(input, nextPos)
    if (starArg) {
      args = [{ kind: 'AttrRef', name: '*' }]
      nextPos = starArg.nextPos
    } else {
      const argRes = opt(argList)(input, nextPos)
      if (!argRes) return null
      args = argRes.value ?? []
      nextPos = argRes.nextPos
    }
  }

  const rp = symbol(')')(input, nextPos)
  if (!rp) return null

  return { value: { kind: 'Call', fn, args }, nextPos: rp.nextPos }
}

const primary: Parser<MetricExpr> = choice(
  parens(expr),
  functionCall,
  map(numberLiteral, (n) => ({ kind: 'Literal', value: n }) as MetricExpr),
  map(identifier, (name) => ({ kind: 'AttrRef', name }) as MetricExpr)
)

const multiplicative: Parser<MetricExpr> = chainLeft(
  primary,
  choice(symbol('*'), symbol('/')),
  (left, op, right) => ({ kind: 'BinaryOp', op: op as '+' | '-' | '*' | '/', left, right })
)

const additive: Parser<MetricExpr> = chainLeft(
  multiplicative,
  choice(symbol('+'), symbol('-')),
  (left, op, right) => ({ kind: 'BinaryOp', op: op as '+' | '-' | '*' | '/', left, right })
)

// Metric declaration parser
const metricDecl: Parser<MetricDeclAst> = map(
  seq(keyword('metric'), identifier, keyword('on'), identifier, symbol('='), expr),
  ([, name, , baseFact, , expression]) => ({ name, baseFact, expr: expression })
)

// Filter and Having parsers
const comparator = choice(
  symbol('>='),
  symbol('<='),
  symbol('>'),
  symbol('<'),
  symbol('=='),
  symbol('!=')
)

const filterLiteral: Parser<unknown> = choice<unknown>(
  map(numberLiteral, (n) => n as unknown),
  map(stringLiteral, (s) => s as unknown),
  map(keyword('true'), () => true as unknown),
  map(keyword('false'), () => false as unknown)
)

const filterExpression: Parser<FilterNode> = map(
  seq(identifier, comparator, filterLiteral),
  ([field, op, value]) => ({ kind: 'expression', field, op, value })
)

const boolExpr: Parser<FilterNode> = lazy(() => boolOr)
const boolTerm: Parser<FilterNode> = choice(parens(boolExpr), filterExpression)

const boolAnd: Parser<FilterNode> = (input, pos) => {
  let result = boolTerm(input, pos)
  if (!result) return null
  let value = result.value
  let nextPos = result.nextPos

  while (true) {
    const andKw = keyword('and')(input, nextPos)
    if (!andKw) break
    const right = boolTerm(input, andKw.nextPos)
    if (!right) break

    if (value.kind === 'and') {
      value = { kind: 'and', filters: [...value.filters, right.value] }
    } else {
      value = { kind: 'and', filters: [value, right.value] }
    }
    nextPos = right.nextPos
  }

  return { value, nextPos }
}

const boolOr: Parser<FilterNode> = (input, pos) => {
  let result = boolAnd(input, pos)
  if (!result) return null
  let value = result.value
  let nextPos = result.nextPos

  while (true) {
    const orKw = keyword('or')(input, nextPos)
    if (!orKw) break
    const right = boolAnd(input, orKw.nextPos)
    if (!right) break

    if (value.kind === 'or') {
      value = { kind: 'or', filters: [...value.filters, right.value] }
    } else {
      value = { kind: 'or', filters: [value, right.value] }
    }
    nextPos = right.nextPos
  }

  return { value, nextPos }
}

const havingTerm: Parser<MetricHavingAst> = map(
  seq(identifier, comparator, numberLiteral),
  ([metric, op, value]) => ({ kind: 'MetricCmp', metric, op, value })
)

const havingExpr: Parser<MetricHavingAst> = lazy(() => havingOr)

const havingAnd: Parser<MetricHavingAst> = (input, pos) => {
  let result = havingTerm(input, pos)
  if (!result) return null
  let value = result.value
  let nextPos = result.nextPos

  while (true) {
    const andKw = keyword('and')(input, nextPos)
    if (!andKw) break
    const right = havingTerm(input, andKw.nextPos)
    if (!right) break

    if (value.kind === 'And') {
      value = { kind: 'And', items: [...value.items, right.value] }
    } else {
      value = { kind: 'And', items: [value, right.value] }
    }
    nextPos = right.nextPos
  }

  return { value, nextPos }
}

const havingOr: Parser<MetricHavingAst> = (input, pos) => {
  let result = havingAnd(input, pos)
  if (!result) return null
  let value = result.value
  let nextPos = result.nextPos

  while (true) {
    const orKw = keyword('or')(input, nextPos)
    if (!orKw) break
    const right = havingAnd(input, orKw.nextPos)
    if (!right) break

    if (value.kind === 'Or') {
      value = { kind: 'Or', items: [...value.items, right.value] }
    } else {
      value = { kind: 'Or', items: [value, right.value] }
    }
    nextPos = right.nextPos
  }

  return { value, nextPos }
}

// Query parser
const identList = sepBy(identifier, symbol(','))

const dimensionsLine = map(seq(keyword('dimensions'), symbol(':'), identList), ([, , dims]) => ({
  kind: 'dimensions' as const,
  values: dims,
}))

const metricsLine = map(seq(keyword('metrics'), symbol(':'), identList), ([, , metrics]) => ({
  kind: 'metrics' as const,
  values: metrics,
}))

const whereLine = map(seq(keyword('where'), symbol(':'), boolExpr), ([, , expr]) => ({
  kind: 'where' as const,
  value: expr,
}))

const havingLine = map(seq(keyword('having'), symbol(':'), havingExpr), ([, , expr]) => ({
  kind: 'having' as const,
  value: expr,
}))

type QueryLineResult =
  | { kind: 'dimensions'; values: string[] }
  | { kind: 'metrics'; values: string[] }
  | { kind: 'where'; value: FilterNode }
  | { kind: 'having'; value: MetricHavingAst }

const queryLine: Parser<QueryLineResult> = choice<QueryLineResult>(
  dimensionsLine,
  metricsLine,
  whereLine as Parser<QueryLineResult>,
  havingLine as Parser<QueryLineResult>
)

const queryDecl: Parser<QueryAst> = (input, pos) => {
  const header = seq(keyword('query'), identifier, symbol('{'))
  const headResult = header(input, pos)
  if (!headResult) return null
  const [, name] = headResult.value
  let nextPos = headResult.nextPos
  const lines: QueryLineResult[] = []

  while (true) {
    const close = symbol('}')(input, nextPos)
    if (close) {
      nextPos = close.nextPos
      break
    }
    const line = queryLine(input, nextPos)
    if (!line) return null
    lines.push(line.value)
    nextPos = line.nextPos
  }

  const spec: QuerySpecAst = { dimensions: [], metrics: [] }
  lines.forEach((line) => {
    switch (line.kind) {
      case 'dimensions':
        spec.dimensions = line.values
        break
      case 'metrics':
        spec.metrics = line.values
        break
      case 'where':
        spec.where = line.value
        break
      case 'having':
        spec.having = line.value
        break
    }
  })

  return { value: { name, spec }, nextPos }
}

// File parser
const fileParser: Parser<DslFileAst> = (input, pos) => {
  let nextPos = skipWs(input, pos)
  const metrics: MetricDeclAst[] = []
  const queries: QueryAst[] = []

  while (nextPos < input.length) {
    const metric = metricDecl(input, nextPos)
    if (metric) {
      metrics.push(metric.value)
      nextPos = metric.nextPos
      continue
    }
    const query = queryDecl(input, nextPos)
    if (query) {
      queries.push(query.value)
      nextPos = query.nextPos
      continue
    }
    break
  }

  nextPos = skipWs(input, nextPos)
  return { value: { metrics, queries }, nextPos }
}

// Get line and column from position
function getLineCol(input: string, pos: number): { line: number; column: number } {
  const lines = input.slice(0, pos).split('\n')
  return {
    line: lines.length,
    column: lines[lines.length - 1].length + 1,
  }
}

// Parse functions with error reporting
export function parseDsl(text: string): { ast: DslFileAst | null; errors: ParseError[] } {
  try {
    const result = fileParser(text, 0)
    if (!result) {
      return {
        ast: null,
        errors: [{ message: 'Failed to parse DSL', line: 1, column: 1, severity: 'error' }],
      }
    }

    const remaining = skipWs(text, result.nextPos)
    if (remaining !== text.length) {
      const { line, column } = getLineCol(text, result.nextPos)
      return {
        ast: result.value,
        errors: [
          {
            message: `Unexpected content at position ${result.nextPos}`,
            line,
            column,
            severity: 'error',
          },
        ],
      }
    }

    return { ast: result.value, errors: [] }
  } catch (e) {
    return {
      ast: null,
      errors: [
        {
          message: e instanceof Error ? e.message : 'Unknown parse error',
          line: 1,
          column: 1,
          severity: 'error',
        },
      ],
    }
  }
}

export function parseMetricExpression(text: string): {
  expr: MetricExpr | null
  errors: ParseError[]
} {
  try {
    const result = expr(text, 0)
    if (!result) {
      return {
        expr: null,
        errors: [
          { message: 'Failed to parse metric expression', line: 1, column: 1, severity: 'error' },
        ],
      }
    }

    const remaining = skipWs(text, result.nextPos)
    if (remaining !== text.length) {
      const { line, column } = getLineCol(text, result.nextPos)
      return {
        expr: result.value,
        errors: [
          {
            message: `Unexpected content at position ${result.nextPos}`,
            line,
            column,
            severity: 'error',
          },
        ],
      }
    }

    return { expr: result.value, errors: [] }
  } catch (e) {
    return {
      expr: null,
      errors: [
        {
          message: e instanceof Error ? e.message : 'Unknown parse error',
          line: 1,
          column: 1,
          severity: 'error',
        },
      ],
    }
  }
}

// Validate metric references
export function validateMetricExpr(
  expr: MetricExpr,
  knownAttributes: Set<string>,
  knownMetrics: Set<string>
): ParseError[] {
  const errors: ParseError[] = []

  function validate(e: MetricExpr): void {
    switch (e.kind) {
      case 'AttrRef':
        if (e.name !== '*' && !knownAttributes.has(e.name) && !knownMetrics.has(e.name)) {
          errors.push({
            message: `Unknown attribute or metric: ${e.name}`,
            severity: 'warning',
          })
        }
        break
      case 'MetricRef':
        if (!knownMetrics.has(e.name)) {
          errors.push({
            message: `Unknown metric: ${e.name}`,
            severity: 'error',
          })
        }
        break
      case 'BinaryOp':
        validate(e.left)
        validate(e.right)
        break
      case 'Call':
        e.args.forEach(validate)
        break
    }
  }

  validate(expr)
  return errors
}

// Get completions for DSL
export function getDslCompletions(
  context: {
    attributes: string[]
    metrics: string[]
    facts: string[]
    dimensions: string[]
  },
  _position: { line: number; column: number },
  _text: string
): Array<{ label: string; kind: string; detail?: string }> {
  const completions: Array<{ label: string; kind: string; detail?: string }> = []

  // Keywords
  const keywords = ['metric', 'on', 'query', 'dimensions', 'metrics', 'where', 'having', 'and', 'or']
  keywords.forEach((kw) => {
    completions.push({ label: kw, kind: 'keyword' })
  })

  // Functions
  const functions = ['sum', 'avg', 'min', 'max', 'count', 'last_year']
  functions.forEach((fn) => {
    completions.push({ label: fn, kind: 'function', detail: `${fn}()` })
  })

  // Attributes
  context.attributes.forEach((attr) => {
    completions.push({ label: attr, kind: 'attribute', detail: 'Attribute' })
  })

  // Metrics
  context.metrics.forEach((metric) => {
    completions.push({ label: metric, kind: 'metric', detail: 'Metric' })
  })

  // Facts
  context.facts.forEach((fact) => {
    completions.push({ label: fact, kind: 'fact', detail: 'Fact' })
  })

  return completions
}
