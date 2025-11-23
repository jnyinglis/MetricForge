/**
 * Logical Plan Builder
 *
 * Utilities for constructing LogicalQueryPlan DAGs from query specifications.
 * This module handles:
 * - Plan node ID generation
 * - Scan node construction (fact and dimension)
 * - Join inference from semantic model
 * - Filter node construction
 * - Aggregate node construction
 * - Plan DAG assembly
 *
 * @see docs/logical-ast-proposal.md
 */

import type {
  SemanticModel,
  JoinEdge,
  QuerySpecV2,
  FilterNode as FilterSpec,
  FilterExpression,
  FilterConjunction,
} from "./semanticEngine";
import {
  PlanNodeId,
  LogicalPlanNode,
  FactScanNode,
  DimensionScanNode,
  JoinNode,
  FilterNode,
  AggregateNode,
  LogicalQueryPlan,
  LogicalMetricPlan,
  ResolvedGrain,
  LogicalAttributeRef,
  LogicalPredicate,
  LogicalExpr,
  LogicalAggregate,
  DataTypes,
  computeGrainId,
} from "./logicalAst";
import { syntaxToLogical, collectAttributeRefs, collectMetricRefs } from "./syntaxToLogical";

// ---------------------------------------------------------------------------
// PLAN NODE ID GENERATION
// ---------------------------------------------------------------------------

let nodeIdCounter = 0;

/**
 * Generate a unique plan node ID.
 */
export function generateNodeId(prefix: string = "node"): PlanNodeId {
  return `${prefix}_${++nodeIdCounter}`;
}

/**
 * Reset the node ID counter (for testing).
 */
export function resetNodeIdCounter(): void {
  nodeIdCounter = 0;
}

// ---------------------------------------------------------------------------
// PLAN DAG CLASS
// ---------------------------------------------------------------------------

/**
 * Mutable plan DAG builder.
 * Accumulates nodes and provides utilities for construction.
 */
export class PlanDag {
  private nodes: Map<PlanNodeId, LogicalPlanNode> = new Map();
  private rootId: PlanNodeId | null = null;

  /**
   * Add a node to the DAG.
   */
  addNode(node: LogicalPlanNode): PlanNodeId {
    this.nodes.set(node.id, node);
    return node.id;
  }

  /**
   * Set the root node ID.
   */
  setRoot(id: PlanNodeId): void {
    if (!this.nodes.has(id)) {
      throw new Error(`Cannot set root: node ${id} not found in DAG`);
    }
    this.rootId = id;
  }

  /**
   * Get a node by ID.
   */
  getNode(id: PlanNodeId): LogicalPlanNode | undefined {
    return this.nodes.get(id);
  }

  /**
   * Get all nodes.
   */
  getAllNodes(): Map<PlanNodeId, LogicalPlanNode> {
    return new Map(this.nodes);
  }

  /**
   * Get the root node ID.
   */
  getRootId(): PlanNodeId | null {
    return this.rootId;
  }

  /**
   * Get the number of nodes.
   */
  size(): number {
    return this.nodes.size;
  }

  /**
   * Check if a node exists.
   */
  hasNode(id: PlanNodeId): boolean {
    return this.nodes.has(id);
  }

  /**
   * Get all node IDs.
   */
  getNodeIds(): PlanNodeId[] {
    return Array.from(this.nodes.keys());
  }

  /**
   * Find nodes by kind.
   */
  findNodesByKind<K extends LogicalPlanNode["kind"]>(
    kind: K
  ): Array<Extract<LogicalPlanNode, { kind: K }>> {
    const result: Array<Extract<LogicalPlanNode, { kind: K }>> = [];
    for (const node of this.nodes.values()) {
      if (node.kind === kind) {
        result.push(node as Extract<LogicalPlanNode, { kind: K }>);
      }
    }
    return result;
  }
}

// ---------------------------------------------------------------------------
// SCAN NODE BUILDERS
// ---------------------------------------------------------------------------

/**
 * Create a fact scan node.
 */
export function createFactScan(
  tableName: string,
  requiredColumns: LogicalAttributeRef[],
  inlineFilters: LogicalPredicate[] = []
): FactScanNode {
  return {
    id: generateNodeId("fact_scan"),
    kind: "FactScan",
    tableName,
    requiredColumns,
    inlineFilters,
  };
}

/**
 * Create a dimension scan node.
 */
export function createDimensionScan(
  tableName: string,
  requiredColumns: LogicalAttributeRef[],
  inlineFilters: LogicalPredicate[] = []
): DimensionScanNode {
  return {
    id: generateNodeId("dim_scan"),
    kind: "DimensionScan",
    tableName,
    requiredColumns,
    inlineFilters,
  };
}

// ---------------------------------------------------------------------------
// JOIN NODE BUILDERS
// ---------------------------------------------------------------------------

/**
 * Create a join node.
 */
export function createJoin(
  leftInputId: PlanNodeId,
  rightInputId: PlanNodeId,
  joinKeys: Array<{ leftAttr: LogicalAttributeRef; rightAttr: LogicalAttributeRef }>,
  joinType: "inner" | "left" | "right" | "full" = "inner",
  cardinality: "1:1" | "1:N" | "N:1" | "N:M" = "N:1"
): JoinNode {
  return {
    id: generateNodeId("join"),
    kind: "Join",
    joinType,
    leftInputId,
    rightInputId,
    joinKeys,
    cardinality,
  };
}

/**
 * Infer join keys from the semantic model for a fact-dimension join.
 */
export function inferJoinKeys(
  factTable: string,
  dimensionTable: string,
  model: SemanticModel
): Array<{ leftAttr: LogicalAttributeRef; rightAttr: LogicalAttributeRef }> | null {
  // Find the join edge in the model
  const joinEdge = model.joins.find((edge) => {
    const factTableName = model.facts[edge.fact]?.table ?? edge.fact;
    const dimTableName = model.dimensions[edge.dimension]?.table ?? edge.dimension;
    return factTableName === factTable && dimTableName === dimensionTable;
  });

  if (!joinEdge) {
    return null;
  }

  // Create attribute refs for the join keys
  const leftAttr: LogicalAttributeRef = {
    kind: "AttributeRef",
    attributeId: joinEdge.factKey,
    logicalName: joinEdge.factKey,
    physicalTable: factTable,
    physicalColumn: joinEdge.factKey,
    dataType: DataTypes.unknown,
    sourceKind: "fact",
  };

  const rightAttr: LogicalAttributeRef = {
    kind: "AttributeRef",
    attributeId: joinEdge.dimensionKey,
    logicalName: joinEdge.dimensionKey,
    physicalTable: dimensionTable,
    physicalColumn: joinEdge.dimensionKey,
    dataType: DataTypes.unknown,
    sourceKind: "dimension",
  };

  return [{ leftAttr, rightAttr }];
}

// ---------------------------------------------------------------------------
// FILTER NODE BUILDERS
// ---------------------------------------------------------------------------

/**
 * Create a filter node.
 */
export function createFilter(
  inputId: PlanNodeId,
  predicate: LogicalPredicate
): FilterNode {
  return {
    id: generateNodeId("filter"),
    kind: "Filter",
    inputId,
    predicate,
  };
}

// ---------------------------------------------------------------------------
// AGGREGATE NODE BUILDERS
// ---------------------------------------------------------------------------

/**
 * Create an aggregate node.
 */
export function createAggregate(
  inputId: PlanNodeId,
  groupBy: LogicalAttributeRef[],
  aggregates: Array<{ outputName: string; expr: LogicalAggregate }>
): AggregateNode {
  return {
    id: generateNodeId("agg"),
    kind: "Aggregate",
    inputId,
    groupBy,
    aggregates,
  };
}

// ---------------------------------------------------------------------------
// ATTRIBUTE RESOLUTION
// ---------------------------------------------------------------------------

/**
 * Resolve an attribute name to a LogicalAttributeRef using the semantic model.
 */
export function resolveAttributeRef(
  attrName: string,
  model: SemanticModel
): LogicalAttributeRef | null {
  const attr = model.attributes[attrName];
  if (!attr) {
    return null;
  }

  const physicalTable = attr.table;
  const physicalColumn = attr.column ?? attrName;

  // Determine source kind
  let sourceKind: "fact" | "dimension" = "dimension";
  for (const [factId, factDef] of Object.entries(model.facts)) {
    const factTable = factDef.table ?? factId;
    if (factTable === physicalTable) {
      sourceKind = "fact";
      break;
    }
  }

  return {
    kind: "AttributeRef",
    attributeId: attrName,
    logicalName: attrName,
    physicalTable,
    physicalColumn,
    dataType: DataTypes.unknown,
    sourceKind,
  };
}

/**
 * Resolve multiple attribute names to LogicalAttributeRefs.
 */
export function resolveAttributeRefs(
  attrNames: string[],
  model: SemanticModel
): LogicalAttributeRef[] {
  const refs: LogicalAttributeRef[] = [];
  for (const name of attrNames) {
    const ref = resolveAttributeRef(name, model);
    if (ref) {
      refs.push(ref);
    }
  }
  return refs;
}

// ---------------------------------------------------------------------------
// TABLE GROUPING
// ---------------------------------------------------------------------------

/**
 * Group attributes by their source table.
 */
export function groupAttributesByTable(
  attrs: LogicalAttributeRef[]
): Map<string, LogicalAttributeRef[]> {
  const groups = new Map<string, LogicalAttributeRef[]>();
  for (const attr of attrs) {
    const existing = groups.get(attr.physicalTable) ?? [];
    existing.push(attr);
    groups.set(attr.physicalTable, existing);
  }
  return groups;
}

/**
 * Determine which tables are needed for a set of attributes.
 */
export function getRequiredTables(attrs: LogicalAttributeRef[]): Set<string> {
  return new Set(attrs.map((a) => a.physicalTable));
}

// ---------------------------------------------------------------------------
// JOIN PATH INFERENCE
// ---------------------------------------------------------------------------

/**
 * Find a join path from a fact table to all required dimension tables.
 * Returns the dimension tables in the order they should be joined.
 */
export function inferJoinPath(
  factTable: string,
  dimensionTables: Set<string>,
  model: SemanticModel
): string[] {
  const joinableDimensions: string[] = [];

  for (const dimTable of dimensionTables) {
    // Check if there's a join edge from the fact to this dimension
    const hasJoin = model.joins.some((edge) => {
      const factTableName = model.facts[edge.fact]?.table ?? edge.fact;
      const dimTableName = model.dimensions[edge.dimension]?.table ?? edge.dimension;
      return factTableName === factTable && dimTableName === dimTable;
    });

    if (hasJoin) {
      joinableDimensions.push(dimTable);
    }
  }

  return joinableDimensions;
}

// ---------------------------------------------------------------------------
// PLAN DAG CONSTRUCTION
// ---------------------------------------------------------------------------

/**
 * Options for plan building.
 */
export interface PlanBuilderOptions {
  /** Whether to inline filters into scan nodes when possible */
  pushDownFilters?: boolean;
}

/**
 * Context for plan building.
 */
export interface PlanBuilderContext {
  model: SemanticModel;
  options: PlanBuilderOptions;
  dag: PlanDag;
}

/**
 * Build a basic scan + join plan for a fact table and its dimensions.
 * Returns the ID of the final joined node.
 */
export function buildJoinedScanPlan(
  factTable: string,
  factColumns: LogicalAttributeRef[],
  dimensionColumns: Map<string, LogicalAttributeRef[]>,
  model: SemanticModel,
  dag: PlanDag
): PlanNodeId {
  // Create fact scan
  const factScan = createFactScan(factTable, factColumns);
  dag.addNode(factScan);
  let currentId = factScan.id;

  // Join each dimension
  for (const [dimTable, dimCols] of dimensionColumns) {
    // Create dimension scan
    const dimScan = createDimensionScan(dimTable, dimCols);
    dag.addNode(dimScan);

    // Infer join keys
    const joinKeys = inferJoinKeys(factTable, dimTable, model);
    if (joinKeys) {
      const join = createJoin(currentId, dimScan.id, joinKeys, "inner", "N:1");
      dag.addNode(join);
      currentId = join.id;
    }
  }

  return currentId;
}

// ---------------------------------------------------------------------------
// PLAN VISUALIZATION
// ---------------------------------------------------------------------------

/**
 * Generate a simple text representation of a plan DAG.
 */
export function formatPlanDag(dag: PlanDag, rootId?: PlanNodeId): string {
  const root = rootId ?? dag.getRootId();
  if (!root) {
    return "(empty plan)";
  }

  const lines: string[] = [];
  const visited = new Set<PlanNodeId>();

  function formatNode(id: PlanNodeId, indent: number): void {
    if (visited.has(id)) {
      lines.push(`${"  ".repeat(indent)}[${id}] (already shown)`);
      return;
    }
    visited.add(id);

    const node = dag.getNode(id);
    if (!node) {
      lines.push(`${"  ".repeat(indent)}[${id}] (missing)`);
      return;
    }

    const prefix = "  ".repeat(indent);

    switch (node.kind) {
      case "FactScan":
        lines.push(`${prefix}[${node.id}] FactScan ${node.tableName}`);
        if (node.requiredColumns.length > 0) {
          lines.push(`${prefix}  columns: ${node.requiredColumns.map((c) => c.attributeId).join(", ")}`);
        }
        break;

      case "DimensionScan":
        lines.push(`${prefix}[${node.id}] DimensionScan ${node.tableName}`);
        if (node.requiredColumns.length > 0) {
          lines.push(`${prefix}  columns: ${node.requiredColumns.map((c) => c.attributeId).join(", ")}`);
        }
        break;

      case "Join":
        lines.push(`${prefix}[${node.id}] Join (${node.joinType}, ${node.cardinality})`);
        lines.push(`${prefix}  on: ${node.joinKeys.map((k) => `${k.leftAttr.attributeId}=${k.rightAttr.attributeId}`).join(", ")}`);
        lines.push(`${prefix}  ↳ left:`);
        formatNode(node.leftInputId, indent + 2);
        lines.push(`${prefix}  ↳ right:`);
        formatNode(node.rightInputId, indent + 2);
        break;

      case "Filter":
        lines.push(`${prefix}[${node.id}] Filter`);
        formatNode(node.inputId, indent + 1);
        break;

      case "Aggregate":
        lines.push(`${prefix}[${node.id}] Aggregate`);
        if (node.groupBy.length > 0) {
          lines.push(`${prefix}  groupBy: ${node.groupBy.map((g) => g.attributeId).join(", ")}`);
        }
        if (node.aggregates.length > 0) {
          lines.push(`${prefix}  aggregates: ${node.aggregates.map((a) => `${a.outputName}=${a.expr.op}(...)`).join(", ")}`);
        }
        formatNode(node.inputId, indent + 1);
        break;

      case "Window":
        lines.push(`${prefix}[${node.id}] Window`);
        formatNode(node.inputId, indent + 1);
        break;

      case "Transform":
        lines.push(`${prefix}[${node.id}] Transform ${node.transformId}`);
        formatNode(node.inputId, indent + 1);
        break;

      case "Project":
        lines.push(`${prefix}[${node.id}] Project`);
        formatNode(node.inputId, indent + 1);
        break;
    }
  }

  formatNode(root, 0);
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// LOGICAL QUERY PLAN ASSEMBLY
// ---------------------------------------------------------------------------

/**
 * Assemble a LogicalQueryPlan from a PlanDag and metric information.
 */
export function assemblePlan(
  dag: PlanDag,
  outputGrain: ResolvedGrain,
  outputMetrics: LogicalMetricPlan[],
  metricEvalOrder: string[]
): LogicalQueryPlan {
  const rootId = dag.getRootId();
  if (!rootId) {
    throw new Error("Cannot assemble plan: no root node set");
  }

  return {
    rootNodeId: rootId,
    nodes: dag.getAllNodes(),
    outputGrain,
    outputMetrics,
    metricEvalOrder,
  };
}

/**
 * Create a resolved grain from dimension attribute refs.
 */
export function createResolvedGrain(
  dimensions: LogicalAttributeRef[]
): ResolvedGrain {
  return {
    dimensions,
    grainId: computeGrainId(dimensions),
  };
}
