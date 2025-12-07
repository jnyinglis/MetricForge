# Semantic Engine Playground Technical Guide

This document provides technical details for developers who want to understand, modify, or extend the Semantic Engine Playground.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Technology Stack](#technology-stack)
4. [State Management](#state-management)
5. [Parser Implementation](#parser-implementation)
6. [Query Execution Engine](#query-execution-engine)
7. [Monaco Editor Integration](#monaco-editor-integration)
8. [Component Reference](#component-reference)
9. [Type Definitions](#type-definitions)
10. [Development Guide](#development-guide)
11. [Deployment](#deployment)
12. [Extension Points](#extension-points)

---

## Architecture Overview

The playground is a self-contained React SPA with no backend dependencies. All processing happens client-side.

```
┌─────────────────────────────────────────────────────────────────┐
│                        React Application                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Monaco    │  │  Zustand    │  │    Parser Adapter       │  │
│  │   Editor    │  │   Store     │  │  (Parser Combinators)   │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                      │                │
│         └────────────────┼──────────────────────┘                │
│                          │                                       │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │   Engine Runner       │                          │
│              │ (Query Execution)     │                          │
│              └───────────────────────┘                          │
│                          │                                       │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │   LocalStorage        │                          │
│              │   (Persistence)       │                          │
│              └───────────────────────┘                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **User Input** → Monaco Editor or Form UI
2. **Validation** → Parser Adapter validates DSL syntax
3. **State Update** → Zustand store updates workspace state
4. **Persistence** → State auto-saved to LocalStorage
5. **Execution** → Engine Runner processes queries client-side
6. **Results** → Displayed in Results panel

---

## Project Structure

```
playground/
├── index.html                 # Entry HTML
├── package.json               # Dependencies and scripts
├── tsconfig.json              # TypeScript configuration
├── tsconfig.node.json         # Node-specific TS config (for Vite)
├── vite.config.ts             # Vite build configuration
│
├── public/
│   └── vite.svg               # Favicon
│
└── src/
    ├── main.tsx               # React entry point
    ├── App.tsx                # Root component
    │
    ├── components/
    │   ├── Header.tsx         # Top bar with import/export
    │   ├── Sidebar.tsx        # Left navigation panel
    │   ├── Workspace.tsx      # Center content router
    │   ├── RightPanel.tsx     # Right info panel
    │   ├── WelcomeScreen.tsx  # Initial landing page
    │   │
    │   └── editors/
    │       ├── DataEditor.tsx    # Table data viewer
    │       ├── SchemaEditor.tsx  # Schema form editor
    │       ├── MetricEditor.tsx  # Metric DSL editor
    │       └── QueryEditor.tsx   # Query DSL editor
    │
    ├── hooks/
    │   └── useWorkspaceStore.ts  # Zustand state management
    │
    ├── types/
    │   └── workspace.ts       # TypeScript type definitions
    │
    ├── utils/
    │   ├── parserAdapter.ts   # DSL parser implementation
    │   └── engineRunner.ts    # Query execution engine
    │
    └── styles/
        └── index.css          # Global styles
```

---

## Technology Stack

| Technology | Purpose | Version |
|------------|---------|---------|
| React | UI framework | 18.2.0 |
| Vite | Build tool | 5.0.10 |
| TypeScript | Type safety | 5.3.3 |
| Zustand | State management | 4.4.7 |
| Monaco Editor | Code editor | 0.45.0 |
| @monaco-editor/react | React wrapper | 4.6.0 |

### Why These Choices?

- **Vite**: Fast HMR, ESM-native, simple configuration
- **Zustand**: Minimal boilerplate, built-in persistence middleware
- **Monaco**: Same editor as VS Code, excellent for DSL editing

---

## State Management

### Zustand Store

The entire application state is managed by a single Zustand store with persistence.

**Location**: `src/hooks/useWorkspaceStore.ts`

### State Shape

```typescript
interface WorkspaceState {
  // Data Layer
  tables: TableData[]

  // Schema Layer
  schema: SchemaDefinition

  // Metrics
  metrics: MetricDefinition[]

  // Queries
  queries: QueryDefinition[]

  // Query Results
  queryResults: QueryResult[]

  // UI State
  activeTab: EditorTab | null
  rightPanelTab: RightPanelTab
  sidebarExpanded: {
    data: boolean
    schema: boolean
    metrics: boolean
    queries: boolean
  }
}
```

### Actions

| Action | Description |
|--------|-------------|
| `addTable(name, data)` | Add a new data table |
| `removeTable(name)` | Remove a table |
| `setSchema(schema)` | Replace entire schema |
| `addFact/Dimension/Attribute/Join` | Add schema elements |
| `removeFact/Dimension/Attribute/Join` | Remove schema elements |
| `addMetric(name, dsl?)` | Create a new metric |
| `updateMetric(name, dsl, valid, errors)` | Update metric state |
| `addQuery(name, dsl?)` | Create a new query |
| `updateQuery(name, dsl, valid, errors)` | Update query state |
| `addQueryResult(result)` | Store query execution result |
| `setActiveTab(tab)` | Change active editor tab |
| `exportWorkspace()` | Generate export JSON |
| `importWorkspace(workspace)` | Restore from JSON |
| `resetWorkspace()` | Clear all state |

### Persistence

The store uses Zustand's `persist` middleware with LocalStorage:

```typescript
export const useWorkspaceStore = create<WorkspaceState & WorkspaceActions>()(
  persist(
    (set, get) => ({
      // ... state and actions
    }),
    {
      name: 'semantic-engine-playground',
      version: 1,
    }
  )
)
```

---

## Parser Implementation

### Parser Combinators

The DSL parser uses functional parser combinators, a recursive descent parsing technique.

**Location**: `src/utils/parserAdapter.ts`

### Core Types

```typescript
type ParseResult<T> = { value: T; nextPos: number }
type Parser<T> = (input: string, pos: number) => ParseResult<T> | null
```

### Combinator Functions

| Combinator | Description |
|------------|-------------|
| `map(p, fn)` | Transform parser result |
| `seq(...parsers)` | Sequential composition |
| `choice(...parsers)` | Try alternatives |
| `opt(p)` | Optional (returns null if no match) |
| `many(p)` | Zero or more repetitions |
| `sepBy(p, sep)` | List with separator |
| `chainLeft(p, op, fn)` | Left-associative operators |
| `lazy(fn)` | Deferred parser (for recursion) |
| `between(l, p, r)` | Bracketed content |

### AST Types

```typescript
type MetricExpr =
  | { kind: 'Literal'; value: number }
  | { kind: 'AttrRef'; name: string }
  | { kind: 'MetricRef'; name: string }
  | { kind: 'Call'; fn: string; args: MetricExpr[] }
  | { kind: 'BinaryOp'; op: '+' | '-' | '*' | '/'; left: MetricExpr; right: MetricExpr }

interface MetricDeclAst {
  name: string
  baseFact?: string
  expr: MetricExpr
}

interface QueryAst {
  name: string
  spec: QuerySpecAst
}
```

### Expression Grammar

```
expr        := additive
additive    := multiplicative (("+" | "-") multiplicative)*
multiplicative := primary (("*" | "/") primary)*
primary     := "(" expr ")" | function_call | number | identifier
function_call := identifier "(" arg_list? ")"
```

### Error Reporting

```typescript
interface ParseError {
  message: string
  line?: number
  column?: number
  severity: 'error' | 'warning' | 'info'
}

function parseDsl(text: string): { ast: DslFileAst | null; errors: ParseError[] }
```

---

## Query Execution Engine

### Overview

The engine executes queries entirely client-side using a simplified LINQ-like approach.

**Location**: `src/utils/engineRunner.ts`

### Execution Pipeline

```
1. Parse Query DSL
       ↓
2. Build In-Memory Database
       ↓
3. Build Semantic Model (schema + compiled metrics)
       ↓
4. Resolve Base Fact
       ↓
5. Join Dimensions
       ↓
6. Apply WHERE Filters
       ↓
7. Group by Dimensions
       ↓
8. Evaluate Metrics per Group
       ↓
9. Apply HAVING Filters
       ↓
10. Return Results
```

### Key Components

#### InMemoryDb

```typescript
interface InMemoryDb {
  tables: Record<string, Row[]>
}
```

#### SemanticModel

```typescript
interface SemanticModel {
  facts: Record<string, { table: string }>
  dimensions: Record<string, { table: string }>
  attributes: Record<string, { table: string; column: string }>
  joins: JoinEdge[]
  metrics: Record<string, CompiledMetric>
}
```

#### CompiledMetric

```typescript
interface CompiledMetric {
  name: string
  baseFact?: string
  eval: (ctx: MetricContext) => number | undefined
}

interface MetricContext {
  rows: Row[]                        // Grouped rows
  groupKey: Record<string, unknown>  // Dimension values
  evalMetric: (name: string) => number | undefined  // Recursive eval
}
```

### Enumerable Operations

A minimal LINQ-like implementation:

```typescript
class Enumerable<T> {
  where(predicate: (item: T) => boolean): Enumerable<T>
  select<U>(selector: (item: T) => U): Enumerable<U>
  groupBy<K>(keySelector: (item: T) => K): Map<K, T[]>
  join<U, K, R>(...): Enumerable<R>
  sum(selector): number
  avg(selector): number
  // etc.
}
```

---

## Monaco Editor Integration

### Language Registration

Each editor registers a custom language with Monaco:

```typescript
monaco.languages.register({ id: 'metric-dsl' })

monaco.languages.setMonarchTokensProvider('metric-dsl', {
  keywords: ['metric', 'on', ...],
  functions: ['sum', 'avg', ...],
  tokenizer: {
    root: [
      [/\b(metric|on|...)\b/, 'keyword'],
      [/\b(sum|avg|...)\b/, 'function'],
      // ...
    ],
  },
})
```

### Custom Theme

```typescript
monaco.editor.defineTheme('metric-dsl-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '569cd6', fontStyle: 'bold' },
    { token: 'function', foreground: 'dcdcaa' },
    // ...
  ],
  colors: {
    'editor.background': '#1e1e1e',
  },
})
```

### Completion Provider

```typescript
monaco.languages.registerCompletionItemProvider('metric-dsl', {
  provideCompletionItems: (model, position) => {
    // Return context-aware suggestions
    return {
      suggestions: [
        { label: 'sum', kind: CompletionItemKind.Function, ... },
        { label: 'total_revenue', kind: CompletionItemKind.Variable, ... },
      ],
    }
  },
})
```

### Error Markers

```typescript
monaco.editor.setModelMarkers(model, 'metric-dsl', [
  {
    severity: MarkerSeverity.Error,
    message: 'Unknown attribute: xyz',
    startLineNumber: 1,
    startColumn: 10,
    endLineNumber: 1,
    endColumn: 13,
  },
])
```

---

## Component Reference

### Header.tsx

**Props**: None

**Responsibilities**:
- Export workspace to JSON file
- Import workspace from JSON file
- Reset workspace with confirmation

### Sidebar.tsx

**Props**: None

**State**: `showNewMetricInput`, `showNewQueryInput`

**Responsibilities**:
- Display collapsible sections
- Navigate between editors
- Create/delete metrics and queries

### Workspace.tsx

**Props**: None

**Responsibilities**:
- Route to appropriate editor based on `activeTab`
- Display welcome screen when no tab selected

### RightPanel.tsx

**Props**: None

**Tabs**:
- Preview: Context metadata
- AST: Parsed syntax tree
- Errors: All validation errors
- Results: Query results table

### DataEditor.tsx

**Props**: `{ table: TableData }`

**Responsibilities**:
- Display table rows with pagination
- Show column types
- Format cell values by type

### SchemaEditor.tsx

**Props**: None

**State**: `activeSection`, `showAddForm`, form field states

**Sections**: Facts, Dimensions, Attributes, Joins

### MetricEditor.tsx

**Props**: `{ metric: MetricDefinition }`

**Features**:
- Monaco editor with DSL language
- Real-time validation
- Autocomplete

### QueryEditor.tsx

**Props**: `{ query: QueryDefinition }`

**Features**:
- Monaco editor with DSL language
- Real-time validation
- Autocomplete
- Run Query button

---

## Type Definitions

**Location**: `src/types/workspace.ts`

### Core Types

```typescript
interface TableData {
  name: string
  rows: Record<string, unknown>[]
  columns: ColumnInfo[]
}

interface ColumnInfo {
  name: string
  type: 'string' | 'number' | 'boolean' | 'date' | 'unknown'
  sampleValues: unknown[]
}

interface SchemaDefinition {
  facts: SchemaFact[]
  dimensions: SchemaDimension[]
  attributes: SchemaAttribute[]
  joins: SchemaJoin[]
}

interface MetricDefinition {
  name: string
  dsl: string
  valid: boolean
  errors: ParseError[]
}

interface QueryDefinition {
  name: string
  dsl: string
  valid: boolean
  errors: ParseError[]
}

interface QueryResult {
  queryName: string
  rows: Record<string, unknown>[]
  columns: string[]
  executionTime: number
  error?: string
}
```

### UI Types

```typescript
type EditorTab =
  | { type: 'data'; tableName: string }
  | { type: 'schema' }
  | { type: 'metric'; metricName: string }
  | { type: 'query'; queryName: string }

type RightPanelTab = 'preview' | 'ast' | 'errors' | 'results'
```

---

## Development Guide

### Prerequisites

- Node.js 18+
- npm 9+

### Setup

```bash
cd playground
npm install
```

### Development Server

```bash
npm run dev
```

Opens at `http://localhost:3000` with hot module replacement.

### Building

```bash
npm run build
```

Outputs to `playground/dist/`.

### Type Checking

```bash
npx tsc --noEmit
```

### Adding a New Aggregate Function

1. **Parser** (`src/utils/parserAdapter.ts`):
   - Add to `functions` array in Monarch tokenizer

2. **Engine** (`src/utils/engineRunner.ts`):
   - Add case in `evalExpr` function's `Call` handler

3. **Autocomplete** (`src/utils/parserAdapter.ts`):
   - Add to `getDslCompletions` function

### Adding a New Schema Element

1. **Types** (`src/types/workspace.ts`):
   - Add interface for the new element
   - Update `SchemaDefinition`

2. **Store** (`src/hooks/useWorkspaceStore.ts`):
   - Add state field
   - Add add/remove actions

3. **UI** (`src/components/editors/SchemaEditor.tsx`):
   - Add tab and form UI

---

## Deployment

### GitHub Pages

The repository includes a GitHub Actions workflow for automatic deployment.

**Workflow**: `.github/workflows/deploy.yml`

**Triggers**:
- Push to `main` branch
- Manual dispatch

**Process**:
1. Checkout code
2. Install dependencies
3. Build playground
4. Upload to GitHub Pages

### Hosting both `main` and `develop`

GitHub Pages can only serve one site per repository, so hosting two branches side-by-side requires publishing both builds into the **same** `gh-pages` branch under different folders.

Recommended approach:

1. **Build with distinct base paths**
   - `main`: keep the current Vite base of `/MetricForge/`.
   - `develop`: override the base, e.g. `npm run build -- --base=/MetricForge/develop/`, so assets resolve when served from `.../MetricForge/develop/`.
   - If you prefer not to pass CLI flags, set `VITE_BASE=/MetricForge/develop/` and read it inside `vite.config.ts`.

2. **Publish into separate folders**
   - Use a deployment action that can write to subdirectories (e.g. `peaceiris/actions-gh-pages`).
   - Main workflow publishes the `playground/dist` output to the root of the `gh-pages` branch.
   - A second workflow, triggered by pushes to `develop`, publishes its build to `gh-pages` under `develop/` (use `destination_dir: develop`).
   - Both workflows should set `keep_files: true` to avoid one job deleting the other folder when pushing to `gh-pages`.

3. **Example dual-workflow setup**
   - **Main** (`.github/workflows/deploy-main.yml`): trigger on `push` to `main`; run `npm ci`, `npm run build`; deploy with `destination_dir: .` and `publish_dir: playground/dist`.
   - **Develop** (`.github/workflows/deploy-develop.yml`): trigger on `push` to `develop`; run `npm ci`, `npm run build -- --base=/MetricForge/develop/`; deploy with `destination_dir: develop` and `publish_dir: playground/dist`.
   - Use the same `deploy` secret (token) for both; `peaceiris/actions-gh-pages` handles creating/updating the `gh-pages` branch.

4. **Keep both versions live**
   - Main site stays at `https://<username>.github.io/MetricForge/`.
   - Develop preview is reachable at `https://<username>.github.io/MetricForge/develop/` without overwriting the main build.

5. **Optional hardening**
   - Gate the develop deployment behind branch protection or environment approvals.
   - Add cache-busting headers or suffixes if you expect frequent force-pushes on `develop`.

### Manual Deployment

```bash
cd playground
npm run build
# Deploy contents of dist/ to any static host
```

### Configuration

The `base` path in `vite.config.ts` must match your deployment path:

```typescript
export default defineConfig({
  base: '/MetricForge/',  // For github.io/username/MetricForge/
  // ...
})
```

---

## Extension Points

### Adding a New Editor Type

1. Create component in `src/components/editors/`
2. Add tab type to `EditorTab` union
3. Add case to `Workspace.tsx` router
4. Add sidebar entry in `Sidebar.tsx`

### Custom DSL Syntax

1. Extend parser in `src/utils/parserAdapter.ts`
2. Add AST types
3. Update Monaco tokenizer
4. Handle in engine runner

### New Panel Tab

1. Add to `RightPanelTab` type
2. Add tab button in `RightPanel.tsx`
3. Add render function

### Integration with External Engine

Replace `src/utils/engineRunner.ts` with calls to the parent engine:

```typescript
import { SemanticEngine } from '@engine/semanticEngine'

export function runQuery(...) {
  const engine = SemanticEngine.fromSchema(schema, db)
  engine.useDslFile(metricsDsl)
  return engine.runQuery(queryName)
}
```

Note: This requires configuring Vite aliases and ensuring the engine is browser-compatible.

---

## Performance Considerations

### Data Size Limits

- Recommended: < 10MB total data
- Tables: < 100,000 rows
- LocalStorage limit: ~5MB (varies by browser)

### Optimization Strategies

1. **Lazy Loading**: Only parse DSL on change
2. **Memoization**: Cache compiled metrics
3. **Web Workers**: Move execution off main thread (future)
4. **IndexedDB**: For larger datasets (future)

### Profiling

Use React DevTools and browser performance tools to identify bottlenecks.

---

## Security Considerations

### No Code Execution

The DSL is parsed to an AST and evaluated in a controlled manner. No `eval()` or `Function()` calls are used.

### Data Isolation

All data stays in the browser. No external API calls are made.

### LocalStorage

Data persisted to LocalStorage is accessible to any script on the same origin. Avoid storing sensitive data.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

### Code Style

- Use TypeScript strictly
- Follow existing patterns
- Keep components focused
- Document public APIs
