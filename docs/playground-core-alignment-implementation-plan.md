# Playground/Core Alignment Implementation Plan

Date: 2026-02-08  
Repo: `MetricForge`

## Goal

Align the web playground with the post-`cfdf58b` core runtime architecture so the playground is no longer running a parallel/legacy execution stack.

## Latest Commit Review (`cfdf58b`)

The latest commit ("Implement legacy runtime removal") made these architecture changes in core:

1. Removed legacy execution fallback from `compileMetricExpr` and moved plan-level expressions (`Window`, `Transform`, `last_year`) into explicit runtime compilers in `src/semanticEngine.ts`.
2. Enforced AST-backed metrics in planning (`buildLogicalPlan` now throws when `exprAst` is missing) in `src/planBuilder.ts`.
3. Made `last_year()` plan-level only by rejecting expression-level transformation in `src/syntaxToLogical.ts`.
4. Removed compatibility re-exports from `planBuilder` and tightened import boundaries to `logicalExprCompiler`.
5. Avoided unconditional plan build in `runSemanticQuery` unless `explain/includePlan` is requested.

This means core now has a single strict execution path and assumes metric definitions are AST-native.

## Findings: Playground Legacy Usage

The playground is still running a legacy/parallel implementation and is not aligned with the core architecture.

### 1) Parallel parser implementation

`playground/src/utils/parserAdapter.ts` duplicates parser combinators and AST types instead of using `src/dsl.ts`.

Legacy symptoms:
- Separate `MetricExpr` type that does not include `Window` or `Transform`.
- Local parse APIs with custom error behavior diverging from core parser behavior.

### 2) Parallel runtime evaluator

`playground/src/utils/engineRunner.ts` has a custom evaluator and query executor.

Legacy symptoms:
- Recursive expression evaluation (`evalExpr`) instead of plan/logical-expression compilation path.
- `parseSimpleExpr` fallback parsing path.
- No enforcement of `exprAst`-backed metrics.
- Runtime behavior can drift from `runSemanticQuery`.

### 3) Parallel logical plan builder

`playground/src/utils/logicalPlanBuilder.ts` builds a simplified plan with regex-based metric extraction.

Legacy symptoms:
- Does not use `src/planBuilder.ts` or `src/logicalAst.ts`.
- Cannot represent full plan features (window/transform/table transform/phase semantics).
- Plan view can misrepresent actual core execution semantics.

### 4) UI is wired to legacy adapters

- `playground/src/components/editors/MetricEditor.tsx` and `playground/src/components/editors/QueryEditor.tsx` validate using `parserAdapter`.
- `playground/src/components/editors/QueryEditor.tsx` executes queries via `engineRunner`.
- `playground/src/components/RightPanel.tsx` builds plans via playground-specific builder.

## Playground Functionality Worth Promoting to Core

The playground has useful capabilities that should become core APIs:

1. Parser diagnostics API
- Playground supports non-throwing parse results with `line/column/severity`.
- Core `parseDsl` currently throws generic errors (`TODO: better error reporting`).

2. DSL completion metadata API
- Playground provides `getDslCompletions(...)` for Monaco.
- This should be derived from core grammar/semantic context to avoid keyword/function drift.

3. Browser-facing adapter layer
- Playground contains ad hoc conversion from workspace schema/metrics/tables to runtime model.
- Core should expose a small adapter module for browser clients instead of forcing custom translation.

## Required Changes

## Phase 1: Add core diagnostics/completions APIs

Core changes:
- Add `parseDslWithDiagnostics(text)` returning `{ ast, errors }` with line/column/severity.
- Add `parseMetricExprWithDiagnostics(text)`.
- Add `getDslCompletions(context)` in core (or `dslLanguageService.ts`).
- Keep existing throwing APIs for backward compatibility.

Playground changes:
- Replace `parserAdapter` usage in editors/right panel with core diagnostics/completions APIs.

## Phase 2: Add browser-safe core entry points

Core changes:
- Add an ESM/browser build target and explicit export map for reusable modules:
  - DSL parser/language service
  - semantic engine query runtime
  - plan builder + explain formatting
- Ensure no Node-only imports leak into browser entry points.

Playground changes:
- Import parser/runtime/plan APIs directly from core browser entry.

## Phase 3: Replace playground execution engine with core runtime

Playground changes:
- Remove query execution via `playground/src/utils/engineRunner.ts`.
- Add adapter: workspace state -> `SemanticModel`, `InMemoryDb`, and query spec.
- Execute with `runSemanticQuery(...)`.
- Use `compileDslToModel(...)` for metric registration instead of local compilation.

Core changes:
- Add optional helper(s) if needed for common browser workspace-to-model conversion.

## Phase 4: Replace playground plan builder with core planner

Playground changes:
- Remove `playground/src/utils/logicalPlanBuilder.ts` from plan rendering path.
- Build plan with `buildLogicalPlan(...)`.
- Render either:
  - `LogicalQueryPlan` directly in `PlanVisualizer`, or
  - `explainPlan(...)` textual output for fallback.

## Phase 5: Cleanup legacy playground modules

Remove (or reduce to thin compatibility shims):
- `playground/src/utils/parserAdapter.ts`
- `playground/src/utils/engineRunner.ts`
- `playground/src/utils/logicalPlanBuilder.ts`

Keep only UI-specific helpers that do not duplicate core logic.

## Validation Strategy

1. Parity fixtures
- Create shared fixture DSL/schema/data cases and assert playground results match `runSemanticQuery` outputs.
- Include `last_year`, `Window`, `Transform`, metric dependencies, and having filters.

2. Regression coverage
- Core tests for diagnostics/completions APIs.
- Playground integration tests for:
  - editor diagnostics wiring
  - query execution through core
  - plan panel output from core planner

3. Manual smoke checks
- Run representative queries from playground and core CLI/test harness and compare rows/columns/errors.

## Definition of Done

1. Playground executes queries using core runtime APIs only.
2. Playground plan panel reflects core `buildLogicalPlan` output.
3. Parser/completion behavior comes from core language services.
4. Legacy playground parser/engine/plan duplicates are removed or reduced to thin adapters.
5. Feature parity includes modern plan-level behavior (`Window`, `Transform`, `last_year`) introduced by the post-legacy runtime architecture.

