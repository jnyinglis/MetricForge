# Legacy Runtime Removal Implementation Plan

This document defines the implementation steps to remove legacy/compatibility execution paths and make the logical-plan pipeline the only supported runtime.

## Scope

- Remove legacy metric-expression evaluation fallback in `semanticEngine`.
- Remove compatibility behaviors that keep old metric definitions alive without AST-backed expressions.
- Remove compatibility re-export shims from `planBuilder`.
- Keep behavior correct for Window/Transform/`last_year` via plan-driven execution and tests.

## Non-goals

- Changing public query semantics.
- Introducing new metric syntax.
- Refactoring unrelated planner/executor modules.

## Current Legacy Retention Points

1. `compileMetricExpr` uses `requiresLegacyEvaluation`, `legacyEvaluator`, and dynamic fallback on `TransformationError`.
2. `buildLogicalPlan` allows metrics without `exprAst` by injecting `Constant(0)`.
3. `planBuilder` re-exports compiler symbols from `logicalExprCompiler` for back-compat imports.

## Implementation Steps

### Step 1: Add a migration safety gate (temporary)

1. Add an internal feature flag (for one PR only), e.g. `STRICT_LOGICAL_EXECUTION = true`, near metric compilation/runtime wiring.
2. Route compile-time decisions through that flag so tests can be migrated incrementally.
3. Add a TODO marker in code that the flag must be deleted in Step 6.

Acceptance criteria:
- Build and tests pass with the flag enabled.
- No production behavior changes except code-path selection.

### Step 2: Remove legacy fallback in `compileMetricExpr`

Files:
- `src/semanticEngine.ts`

Tasks:
1. Delete `requiresLegacyEvaluation`.
2. Delete `legacyEvaluator`.
3. Remove mutable `useLegacy` flow and `TransformationError` fallback.
4. Compile `MetricExpr -> LogicalExpr` through `syntaxToLogical(expr, runtimeModel, baseFact)` and always evaluate with `compileLogicalExpr`.
5. Keep aggregate callback handling in `LogicalExprEvalContext.aggregate` so aggregate semantics remain correct.
6. Preserve existing error quality by wrapping/annotating failures only if needed (do not silently downgrade to legacy execution).

Acceptance criteria:
- No code path in `compileMetricExpr` can evaluate Window/Transform/`last_year` using legacy evaluator helpers.
- Errors are explicit when unsupported expressions are encountered.

### Step 3: Make Window/Transform/`last_year` execute through plan semantics only

Files:
- `src/semanticEngine.ts`
- `src/planBuilder.ts`
- `src/syntaxToLogical.ts`

Tasks:
1. Ensure runtime evaluation for Window/Transform uses plan nodes and plan-ordered metric execution, not recursive legacy AST evaluation.
2. Ensure `last_year` execution is represented as transform plan behavior only.
3. Remove dead helpers that exist only for legacy AST fallback if no remaining callers:
   - `evaluateWindowNode`
   - legacy-only rowset/table transform invocation branches
4. Keep `syntaxToLogical` strict for Window/Transform at expression level (already throwing) and verify all callers respect that contract.

Acceptance criteria:
- Window/Transform/`last_year` metrics pass through plan-driven code paths.
- No legacy AST recursive evaluation remains in runtime metric evaluation.

### Step 4: Enforce AST-backed metric definitions in plan builder

Files:
- `src/planBuilder.ts`

Tasks:
1. Remove `exprAst` missing fallback that injects `Constant(0)`.
2. Replace with explicit error: metric definitions must include `exprAst`.
3. Update any builders/helpers that construct metrics to always provide `exprAst`.
4. Audit tests/fixtures for old-style metric definitions and migrate them.

Acceptance criteria:
- Plan building fails fast for metrics without `exprAst`.
- No test relies on implicit zero-valued placeholder metrics.

### Step 5: Remove compatibility re-export shim

Files:
- `src/planBuilder.ts`
- any importing files

Tasks:
1. Delete re-export block at end of `planBuilder.ts`:
   - `compileLogicalExpr`
   - `compileLogicalExprToSql`
   - `LogicalExprEvalContext`
   - `CompiledLogicalExpr`
2. Update all imports to consume these directly from `logicalExprCompiler`.
3. Run TypeScript compile and tests to confirm no stale imports.

Acceptance criteria:
- `planBuilder` no longer exposes compiler APIs for backward compatibility.
- All modules import from canonical compiler module.

### Step 6: Delete temporary migration gate and dead code

Files:
- `src/semanticEngine.ts`
- related tests

Tasks:
1. Remove temporary feature flag from Step 1.
2. Remove any dead code paths, stale comments, and fallback wording.
3. Ensure comments/docs do not mention legacy fallback behavior.

Acceptance criteria:
- No feature flag remains.
- Runtime code reflects a single execution architecture.

### Step 7: Test migration and coverage hardening

Files:
- `test/semanticEngine.test.ts`
- `test/planBuilder.test.ts`
- `test/logicalAst.test.ts`

Tasks:
1. Replace legacy-evaluator-centric tests with plan-execution assertions.
2. Add/adjust tests for:
   - Window node emission order and result correctness.
   - Transform node emission order and result correctness.
   - `last_year` correctness through transform plan path.
   - failure when metric lacks `exprAst`.
3. Add one regression test proving no runtime fallback occurs on `TransformationError` (error is surfaced).
4. Keep EXPLAIN/includePlan tests intact and expand where useful.

Acceptance criteria:
- Tests validate only the new architecture.
- Removed legacy behavior is covered by negative tests.

### Step 8: Verification checklist before merge

1. Run unit tests:
   - `npm test`
2. Run type checks:
   - `npm run build` (or project’s TypeScript build command)
3. Smoke test planner output for representative queries:
   - basic aggregate
   - window metric
   - transform metric
   - `last_year` metric
4. Confirm no code search hits for removed symbols:
   - `requiresLegacyEvaluation`
   - `legacyEvaluator`
   - compatibility re-export block in `planBuilder`

Acceptance criteria:
- Green tests/build.
- No references to removed legacy symbols.

## Rollout Strategy

1. Land as 2-3 focused PRs to reduce risk:
   - PR A: remove `compileMetricExpr` fallback + tests
   - PR B: enforce `exprAst` + fixture migration
   - PR C: remove re-export shim + final cleanup
2. Require passing CI and reviewer sign-off per PR.
3. Do not merge partial cleanup that leaves mixed legacy/non-legacy execution paths.

## Final Definition of Done

- Runtime metric evaluation has a single, plan-first architecture.
- No legacy fallback evaluator exists.
- No placeholder metric fallback for missing `exprAst`.
- No compatibility compiler re-exports in `planBuilder`.
- Tests and docs reflect the final architecture.
