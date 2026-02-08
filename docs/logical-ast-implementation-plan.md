# Logical AST Implementation Plan

This plan translates the Logical AST proposal status review into concrete, step-by-step implementation tasks. It focuses on finishing the remaining Phase 3 and Phase 5 gaps, plus any supporting execution wiring.

## Goals

- Route Window/Transform expressions through the logical plan (plan nodes), not placeholder scalar functions.
- Update runtime execution to build and use logical plans.
- Move metric compilation from syntax AST to logical IR, while preserving backward compatibility.

## Step-by-step Plan

### Step 1: Implement plan-level Window/Transform emission

1. **Plan builder integration**
   - Detect Window/Transform expressions in metric ASTs and emit `WindowNode`/`TransformNode` during plan construction instead of allowing placeholder scalar functions.
   - Replace `strictMode: false` in `buildLogicalPlan` with strict handling for these nodes once plan emission is in place.
2. **Syntax-to-plan wiring**
   - Use existing extract helpers to convert MetricExpr window/transform info into plan nodes, and connect them to the plan DAG.
   - Ensure all attribute references required by window/transform inputs are included in required columns for upstream scans.
3. **Tests**
   - Add integration tests covering window and transform metrics that assert plan node presence and correct DAG order.

### Step 2: Wire window/transform evaluation through the plan

1. **Execution adapter**
   - Decide how window and transform nodes map into runtime evaluation (e.g., extend logical plan compilation or reuse existing runtime helpers).
2. **Remove placeholder fallbacks**
   - Delete the placeholder scalar-function fallbacks in `syntaxToLogical` for Window/Transform now that plan-level handling exists.
3. **Tests**
   - Add runtime tests verifying window/transform results match the legacy evaluator for representative cases.

### Step 3: Update `compileMetricExpr` to use logical IR

1. **Internal reroute**
   - Convert `MetricExpr → LogicalExpr` via `syntaxToLogical`, then compile via `compileLogicalExpr`.
   - Keep existing API signature for backward compatibility.
2. **Validation alignment**
   - Move or replicate validation rules to the logical compilation pipeline so error messages remain as clear as today.
3. **Tests**
   - Ensure existing metric expression tests pass and add a regression test for new logical compilation path.

### Step 4: Integrate logical plan into query execution

1. **Plan-first execution**
   - Update `runSemanticQuery` to build a logical plan before execution.
   - Provide an EXPLAIN/plan option in the public path (or delegate through `buildQueryPlan` if that becomes the main entry point).
2. **Plan execution path**
   - Implement or connect a `compileLogicalPlan` execution pipeline that can produce runtime results equivalent to the legacy executor.
   - Decide whether to fully replace legacy execution or run the plan builder in parallel for validation in a transitional phase.
3. **Tests**
   - Add end-to-end tests that compare plan-based execution to legacy results.

### Step 5: Cleanup & documentation

1. **Remove legacy-only branches**
   - Delete unused placeholders and deprecated helpers once plan execution is primary.
2. **Documentation updates**
   - Update migration notes to reflect the final architecture and public entry points.
3. **Regression checks**
   - Run relevant unit/integration tests for the plan builder and semantic engine.

## Suggested Implementation Order

1. Step 1 (plan-level window/transform emission) so plan structure is correct.
2. Step 2 (window/transform execution) so results are correct.
3. Step 3 (compileMetricExpr via logical IR) to unify expression compilation.
4. Step 4 (runSemanticQuery integration) to make logical plan the primary runtime path.
5. Step 5 (cleanup & docs).
