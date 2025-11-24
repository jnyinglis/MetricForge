import { expect } from "chai";
import {
  LogicalAttribute,
  MetricDefinition,
  SemanticModel,
  MetricExpr,
  aggregateMetric,
  buildMetricFromExpr,
  Expr,
} from "../src/semanticEngine";
import {
  LogicalAttributeRef,
  LogicalAggregate,
  DataTypes,
  FactScanNode,
  DimensionScanNode,
  JoinNode,
  AggregateNode,
  LogicalExpr,
  LogicalConstant,
  LogicalScalarOp,
  LogicalComparison,
  LogicalConditional,
  LogicalCoalesce,
  LogicalInList,
  LogicalBetween,
  LogicalIsNull,
  LogicalLogicalOp,
  LogicalScalarFunction,
} from "../src/logicalAst";
import {
  generateNodeId,
  resetNodeIdCounter,
  PlanDag,
  createFactScan,
  createDimensionScan,
  createJoin,
  createFilter,
  createAggregate,
  createWindow,
  createRollingWindow,
  createCumulativeWindow,
  createOffsetWindow,
  createTransform,
  createTableTransform,
  createRowsetTransform,
  createProject,
  inferJoinKeys,
  resolveAttributeRef,
  resolveAttributeRefs,
  groupAttributesByTable,
  getRequiredTables,
  inferJoinPath,
  buildJoinedScanPlan,
  formatPlanDag,
  assemblePlan,
  createResolvedGrain,
  isWindowExpr,
  isTransformExpr,
  extractWindowInfo,
  extractTransformInfo,
  findWindowExprs,
  findTransformExprs,
  windowInfoToPlanNode,
  transformInfoToPlanNode,
  // Phase 4 exports
  MetricCycleError,
  analyzeMetricDependencies,
  buildDependencyGraph,
  detectCycle,
  topologicalSortMetrics,
  classifyFilter,
  buildLogicalPlan,
  // Phase 5 exports
  ExplainOptions,
  explainPlan,
  formatLogicalExpr,
  compileLogicalExpr,
  compileLogicalExprToSql,
  LogicalExprEvalContext,
  buildQueryPlan,
  planToSql,
} from "../src/planBuilder";

// ---------------------------------------------------------------------------
// TEST FIXTURES
// ---------------------------------------------------------------------------

const attributes: Record<string, LogicalAttribute> = {
  storeId: { table: "dim_store", column: "id" },
  storeName: { table: "dim_store", column: "name" },
  weekId: { table: "dim_week", column: "id" },
  weekName: { table: "dim_week", column: "name" },
  salesAmount: { table: "fact_sales" },
  orderId: { table: "fact_orders" },
  quantity: { table: "fact_orders" },
};

const totalSales = aggregateMetric(
  "totalSales",
  "fact_sales",
  "salesAmount",
  "sum"
);

const model: SemanticModel = {
  facts: {
    fact_sales: { table: "fact_sales" },
    fact_orders: { table: "fact_orders" },
  },
  dimensions: {
    dim_store: { table: "dim_store" },
    dim_week: { table: "dim_week" },
  },
  attributes,
  joins: [
    {
      fact: "fact_sales",
      dimension: "dim_store",
      factKey: "storeId",
      dimensionKey: "id",
    },
    {
      fact: "fact_sales",
      dimension: "dim_week",
      factKey: "weekId",
      dimensionKey: "id",
    },
    {
      fact: "fact_orders",
      dimension: "dim_store",
      factKey: "storeId",
      dimensionKey: "id",
    },
  ],
  metrics: {
    totalSales,
  },
};

// Helper to create a LogicalAttributeRef
function makeAttrRef(
  attributeId: string,
  physicalTable: string,
  physicalColumn: string,
  sourceKind: "fact" | "dimension"
): LogicalAttributeRef {
  return {
    kind: "AttributeRef",
    attributeId,
    logicalName: attributeId,
    physicalTable,
    physicalColumn,
    dataType: DataTypes.unknown,
    sourceKind,
  };
}

// ---------------------------------------------------------------------------
// NODE ID GENERATION TESTS
// ---------------------------------------------------------------------------

describe("Node ID Generation", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  it("should generate unique IDs with prefix", () => {
    const id1 = generateNodeId("test");
    const id2 = generateNodeId("test");
    const id3 = generateNodeId("other");

    expect(id1).to.equal("test_1");
    expect(id2).to.equal("test_2");
    expect(id3).to.equal("other_3");
  });

  it("should reset counter", () => {
    generateNodeId("test");
    generateNodeId("test");
    resetNodeIdCounter();
    const id = generateNodeId("test");
    expect(id).to.equal("test_1");
  });
});

// ---------------------------------------------------------------------------
// PLAN DAG TESTS
// ---------------------------------------------------------------------------

describe("PlanDag", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  it("should add and retrieve nodes", () => {
    const dag = new PlanDag();
    const node = createFactScan("fact_sales", []);

    dag.addNode(node);

    expect(dag.hasNode(node.id)).to.be.true;
    expect(dag.getNode(node.id)).to.deep.equal(node);
    expect(dag.size()).to.equal(1);
  });

  it("should set and get root", () => {
    const dag = new PlanDag();
    const node = createFactScan("fact_sales", []);
    dag.addNode(node);

    dag.setRoot(node.id);

    expect(dag.getRootId()).to.equal(node.id);
  });

  it("should throw when setting root to non-existent node", () => {
    const dag = new PlanDag();

    expect(() => dag.setRoot("non_existent")).to.throw("not found in DAG");
  });

  it("should find nodes by kind", () => {
    const dag = new PlanDag();
    const fact1 = createFactScan("fact_sales", []);
    const fact2 = createFactScan("fact_orders", []);
    const dim1 = createDimensionScan("dim_store", []);

    dag.addNode(fact1);
    dag.addNode(fact2);
    dag.addNode(dim1);

    const factScans = dag.findNodesByKind("FactScan");
    const dimScans = dag.findNodesByKind("DimensionScan");

    expect(factScans).to.have.lengthOf(2);
    expect(dimScans).to.have.lengthOf(1);
  });

  it("should get all node IDs", () => {
    const dag = new PlanDag();
    const node1 = createFactScan("fact_sales", []);
    const node2 = createDimensionScan("dim_store", []);

    dag.addNode(node1);
    dag.addNode(node2);

    const ids = dag.getNodeIds();
    expect(ids).to.include(node1.id);
    expect(ids).to.include(node2.id);
  });
});

// ---------------------------------------------------------------------------
// SCAN NODE BUILDER TESTS
// ---------------------------------------------------------------------------

describe("Scan Node Builders", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createFactScan", () => {
    it("should create fact scan with columns", () => {
      const cols = [makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact")];
      const node = createFactScan("fact_sales", cols);

      expect(node.kind).to.equal("FactScan");
      expect(node.tableName).to.equal("fact_sales");
      expect(node.requiredColumns).to.have.lengthOf(1);
      expect(node.inlineFilters).to.be.empty;
      expect(node.id).to.match(/^fact_scan_/);
    });

    it("should create fact scan with filters", () => {
      const filter: any = {
        kind: "Comparison",
        left: { kind: "Constant", value: 1, dataType: DataTypes.number },
        op: ">",
        right: { kind: "Constant", value: 0, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };
      const node = createFactScan("fact_sales", [], [filter]);

      expect(node.inlineFilters).to.have.lengthOf(1);
    });
  });

  describe("createDimensionScan", () => {
    it("should create dimension scan", () => {
      const cols = [makeAttrRef("storeName", "dim_store", "name", "dimension")];
      const node = createDimensionScan("dim_store", cols);

      expect(node.kind).to.equal("DimensionScan");
      expect(node.tableName).to.equal("dim_store");
      expect(node.id).to.match(/^dim_scan_/);
    });
  });
});

// ---------------------------------------------------------------------------
// JOIN NODE BUILDER TESTS
// ---------------------------------------------------------------------------

describe("Join Node Builders", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createJoin", () => {
    it("should create inner join with N:1 cardinality", () => {
      const leftAttr = makeAttrRef("storeId", "fact_sales", "storeId", "fact");
      const rightAttr = makeAttrRef("id", "dim_store", "id", "dimension");

      const node = createJoin(
        "fact_scan_1",
        "dim_scan_1",
        [{ leftAttr, rightAttr }],
        "inner",
        "N:1"
      );

      expect(node.kind).to.equal("Join");
      expect(node.joinType).to.equal("inner");
      expect(node.cardinality).to.equal("N:1");
      expect(node.joinKeys).to.have.lengthOf(1);
      expect(node.leftInputId).to.equal("fact_scan_1");
      expect(node.rightInputId).to.equal("dim_scan_1");
    });

    it("should default to inner join and N:1", () => {
      const node = createJoin("left", "right", []);

      expect(node.joinType).to.equal("inner");
      expect(node.cardinality).to.equal("N:1");
    });
  });

  describe("inferJoinKeys", () => {
    it("should infer join keys from model", () => {
      const keys = inferJoinKeys("fact_sales", "dim_store", model);

      expect(keys).to.not.be.null;
      expect(keys).to.have.lengthOf(1);
      expect(keys![0].leftAttr.attributeId).to.equal("storeId");
      expect(keys![0].rightAttr.attributeId).to.equal("id");
    });

    it("should return null for non-existent join", () => {
      const keys = inferJoinKeys("fact_orders", "dim_week", model);
      expect(keys).to.be.null;
    });
  });
});

// ---------------------------------------------------------------------------
// FILTER AND AGGREGATE NODE TESTS
// ---------------------------------------------------------------------------

describe("Filter and Aggregate Node Builders", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createFilter", () => {
    it("should create filter node", () => {
      const predicate: any = {
        kind: "Comparison",
        left: { kind: "Constant", value: 10, dataType: DataTypes.number },
        op: ">",
        right: { kind: "Constant", value: 5, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };

      const node = createFilter("input_1", predicate);

      expect(node.kind).to.equal("Filter");
      expect(node.inputId).to.equal("input_1");
      expect(node.predicate).to.deep.equal(predicate);
    });
  });

  describe("createAggregate", () => {
    it("should create aggregate node", () => {
      const groupBy = [makeAttrRef("storeName", "dim_store", "name", "dimension")];
      const agg: LogicalAggregate = {
        kind: "Aggregate",
        op: "sum",
        input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
        distinct: false,
        resultType: DataTypes.number,
      };

      const node = createAggregate("input_1", groupBy, [
        { outputName: "total_sales", expr: agg },
      ]);

      expect(node.kind).to.equal("Aggregate");
      expect(node.inputId).to.equal("input_1");
      expect(node.groupBy).to.have.lengthOf(1);
      expect(node.aggregates).to.have.lengthOf(1);
      expect(node.aggregates[0].outputName).to.equal("total_sales");
    });
  });
});

// ---------------------------------------------------------------------------
// ATTRIBUTE RESOLUTION TESTS
// ---------------------------------------------------------------------------

describe("Attribute Resolution", () => {
  describe("resolveAttributeRef", () => {
    it("should resolve fact attribute", () => {
      const ref = resolveAttributeRef("salesAmount", model);

      expect(ref).to.not.be.null;
      expect(ref!.attributeId).to.equal("salesAmount");
      expect(ref!.physicalTable).to.equal("fact_sales");
      expect(ref!.sourceKind).to.equal("fact");
    });

    it("should resolve dimension attribute", () => {
      const ref = resolveAttributeRef("storeName", model);

      expect(ref).to.not.be.null;
      expect(ref!.physicalTable).to.equal("dim_store");
      expect(ref!.physicalColumn).to.equal("name");
      expect(ref!.sourceKind).to.equal("dimension");
    });

    it("should return null for unknown attribute", () => {
      const ref = resolveAttributeRef("unknown", model);
      expect(ref).to.be.null;
    });
  });

  describe("resolveAttributeRefs", () => {
    it("should resolve multiple attributes", () => {
      const refs = resolveAttributeRefs(["salesAmount", "storeName"], model);

      expect(refs).to.have.lengthOf(2);
      expect(refs[0].attributeId).to.equal("salesAmount");
      expect(refs[1].attributeId).to.equal("storeName");
    });

    it("should skip unknown attributes", () => {
      const refs = resolveAttributeRefs(["salesAmount", "unknown"], model);
      expect(refs).to.have.lengthOf(1);
    });
  });
});

// ---------------------------------------------------------------------------
// TABLE GROUPING TESTS
// ---------------------------------------------------------------------------

describe("Table Grouping", () => {
  describe("groupAttributesByTable", () => {
    it("should group attributes by table", () => {
      const attrs = [
        makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
        makeAttrRef("orderId", "fact_sales", "orderId", "fact"),
        makeAttrRef("storeName", "dim_store", "name", "dimension"),
      ];

      const groups = groupAttributesByTable(attrs);

      expect(groups.size).to.equal(2);
      expect(groups.get("fact_sales")).to.have.lengthOf(2);
      expect(groups.get("dim_store")).to.have.lengthOf(1);
    });
  });

  describe("getRequiredTables", () => {
    it("should return unique tables", () => {
      const attrs = [
        makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
        makeAttrRef("orderId", "fact_sales", "orderId", "fact"),
        makeAttrRef("storeName", "dim_store", "name", "dimension"),
      ];

      const tables = getRequiredTables(attrs);

      expect(tables.size).to.equal(2);
      expect(tables.has("fact_sales")).to.be.true;
      expect(tables.has("dim_store")).to.be.true;
    });
  });
});

// ---------------------------------------------------------------------------
// JOIN PATH INFERENCE TESTS
// ---------------------------------------------------------------------------

describe("Join Path Inference", () => {
  describe("inferJoinPath", () => {
    it("should find joinable dimensions", () => {
      const dims = new Set(["dim_store", "dim_week"]);
      const path = inferJoinPath("fact_sales", dims, model);

      expect(path).to.have.lengthOf(2);
      expect(path).to.include("dim_store");
      expect(path).to.include("dim_week");
    });

    it("should exclude non-joinable dimensions", () => {
      const dims = new Set(["dim_store", "dim_week"]);
      const path = inferJoinPath("fact_orders", dims, model);

      // fact_orders only joins to dim_store, not dim_week
      expect(path).to.have.lengthOf(1);
      expect(path).to.include("dim_store");
    });
  });
});

// ---------------------------------------------------------------------------
// PLAN DAG CONSTRUCTION TESTS
// ---------------------------------------------------------------------------

describe("Plan DAG Construction", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("buildJoinedScanPlan", () => {
    it("should build fact + dimension join plan", () => {
      const dag = new PlanDag();
      const factCols = [makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact")];
      const dimCols = new Map<string, LogicalAttributeRef[]>([
        ["dim_store", [makeAttrRef("storeName", "dim_store", "name", "dimension")]],
      ]);

      const rootId = buildJoinedScanPlan("fact_sales", factCols, dimCols, model, dag);

      expect(dag.size()).to.equal(3); // fact scan + dim scan + join

      const factScans = dag.findNodesByKind("FactScan");
      const dimScans = dag.findNodesByKind("DimensionScan");
      const joins = dag.findNodesByKind("Join");

      expect(factScans).to.have.lengthOf(1);
      expect(dimScans).to.have.lengthOf(1);
      expect(joins).to.have.lengthOf(1);

      // Root should be the join node
      expect(joins[0].id).to.equal(rootId);
    });

    it("should build multi-dimension join plan", () => {
      const dag = new PlanDag();
      const factCols = [makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact")];
      const dimCols = new Map<string, LogicalAttributeRef[]>([
        ["dim_store", [makeAttrRef("storeName", "dim_store", "name", "dimension")]],
        ["dim_week", [makeAttrRef("weekName", "dim_week", "name", "dimension")]],
      ]);

      const rootId = buildJoinedScanPlan("fact_sales", factCols, dimCols, model, dag);

      // fact scan + 2 dim scans + 2 joins
      expect(dag.size()).to.equal(5);

      const joins = dag.findNodesByKind("Join");
      expect(joins).to.have.lengthOf(2);
    });
  });
});

// ---------------------------------------------------------------------------
// PLAN VISUALIZATION TESTS
// ---------------------------------------------------------------------------

describe("Plan Visualization", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("formatPlanDag", () => {
    it("should format simple fact scan", () => {
      const dag = new PlanDag();
      const factScan = createFactScan("fact_sales", [
        makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
      ]);
      dag.addNode(factScan);
      dag.setRoot(factScan.id);

      const output = formatPlanDag(dag);

      expect(output).to.include("FactScan");
      expect(output).to.include("fact_sales");
      expect(output).to.include("salesAmount");
    });

    it("should format join plan", () => {
      const dag = new PlanDag();
      const factScan = createFactScan("fact_sales", []);
      const dimScan = createDimensionScan("dim_store", []);
      const join = createJoin(
        factScan.id,
        dimScan.id,
        [{
          leftAttr: makeAttrRef("storeId", "fact_sales", "storeId", "fact"),
          rightAttr: makeAttrRef("id", "dim_store", "id", "dimension"),
        }],
        "inner",
        "N:1"
      );

      dag.addNode(factScan);
      dag.addNode(dimScan);
      dag.addNode(join);
      dag.setRoot(join.id);

      const output = formatPlanDag(dag);

      expect(output).to.include("Join");
      expect(output).to.include("inner");
      expect(output).to.include("N:1");
      expect(output).to.include("FactScan");
      expect(output).to.include("DimensionScan");
    });

    it("should return (empty plan) for empty DAG", () => {
      const dag = new PlanDag();
      const output = formatPlanDag(dag);
      expect(output).to.equal("(empty plan)");
    });
  });
});

// ---------------------------------------------------------------------------
// PLAN ASSEMBLY TESTS
// ---------------------------------------------------------------------------

describe("Plan Assembly", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createResolvedGrain", () => {
    it("should create grain with sorted ID", () => {
      const dims = [
        makeAttrRef("storeName", "dim_store", "name", "dimension"),
        makeAttrRef("month", "fact_sales", "month", "fact"),
      ];

      const grain = createResolvedGrain(dims);

      expect(grain.dimensions).to.have.lengthOf(2);
      expect(grain.grainId).to.equal("month,storeName"); // sorted alphabetically
    });
  });

  describe("assemblePlan", () => {
    it("should assemble complete plan", () => {
      const dag = new PlanDag();
      const factScan = createFactScan("fact_sales", []);
      dag.addNode(factScan);
      dag.setRoot(factScan.id);

      const grain = createResolvedGrain([
        makeAttrRef("storeName", "dim_store", "name", "dimension"),
      ]);
      const metrics: any[] = [];
      const evalOrder: string[] = [];

      const plan = assemblePlan(dag, grain, metrics, evalOrder);

      expect(plan.rootNodeId).to.equal(factScan.id);
      expect(plan.nodes.size).to.equal(1);
      expect(plan.outputGrain.grainId).to.equal("storeName");
    });

    it("should throw if no root set", () => {
      const dag = new PlanDag();
      const factScan = createFactScan("fact_sales", []);
      dag.addNode(factScan);
      // Don't set root

      expect(() => assemblePlan(dag, { dimensions: [], grainId: "" }, [], [])).to.throw(
        "no root node set"
      );
    });
  });
});

// ---------------------------------------------------------------------------
// WINDOW NODE BUILDER TESTS
// ---------------------------------------------------------------------------

describe("Window Node Builders", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createWindow", () => {
    it("should create window node with partition and order", () => {
      const partitionBy = [makeAttrRef("storeId", "fact_sales", "storeId", "fact")];
      const orderBy = [{ attr: makeAttrRef("weekId", "dim_week", "id", "dimension"), direction: "asc" as const }];
      const windowFunctions = [{
        outputName: "rolling_sum",
        op: "sum" as const,
        input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
      }];

      const node = createWindow("input_1", partitionBy, orderBy, { kind: "rolling", count: 3 }, windowFunctions);

      expect(node.kind).to.equal("Window");
      expect(node.inputId).to.equal("input_1");
      expect(node.partitionBy).to.have.lengthOf(1);
      expect(node.orderBy).to.have.lengthOf(1);
      expect(node.frame.kind).to.equal("rolling");
      expect((node.frame as any).count).to.equal(3);
      expect(node.windowFunctions).to.have.lengthOf(1);
      expect(node.id).to.match(/^window_/);
    });

    it("should create window with cumulative frame", () => {
      const node = createWindow("input_1", [], [], { kind: "cumulative" }, []);

      expect(node.frame.kind).to.equal("cumulative");
    });

    it("should create window with offset frame", () => {
      const node = createWindow("input_1", [], [], { kind: "offset", offset: -1 }, []);

      expect(node.frame.kind).to.equal("offset");
      expect((node.frame as any).offset).to.equal(-1);
    });
  });

  describe("createRollingWindow", () => {
    it("should create rolling window with count", () => {
      const partitionBy = [makeAttrRef("storeId", "fact_sales", "storeId", "fact")];
      const orderBy = makeAttrRef("weekId", "dim_week", "id", "dimension");
      const windowFunctions = [{
        outputName: "rolling_avg",
        op: "avg" as const,
        input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
      }];

      const node = createRollingWindow("input_1", partitionBy, orderBy, 7, windowFunctions);

      expect(node.kind).to.equal("Window");
      expect(node.frame.kind).to.equal("rolling");
      expect((node.frame as any).count).to.equal(7);
      expect(node.orderBy).to.have.lengthOf(1);
      expect(node.orderBy[0].direction).to.equal("asc");
    });
  });

  describe("createCumulativeWindow", () => {
    it("should create cumulative window", () => {
      const partitionBy = [makeAttrRef("storeId", "fact_sales", "storeId", "fact")];
      const orderBy = makeAttrRef("weekId", "dim_week", "id", "dimension");

      const node = createCumulativeWindow("input_1", partitionBy, orderBy, []);

      expect(node.frame.kind).to.equal("cumulative");
    });
  });

  describe("createOffsetWindow", () => {
    it("should create offset window (e.g., LAG)", () => {
      const partitionBy = [makeAttrRef("storeId", "fact_sales", "storeId", "fact")];
      const orderBy = makeAttrRef("weekId", "dim_week", "id", "dimension");

      const node = createOffsetWindow("input_1", partitionBy, orderBy, -1, []);

      expect(node.frame.kind).to.equal("offset");
      expect((node.frame as any).offset).to.equal(-1);
    });

    it("should create offset window for LEAD", () => {
      const orderBy = makeAttrRef("weekId", "dim_week", "id", "dimension");
      const node = createOffsetWindow("input_1", [], orderBy, 1, []);

      expect((node.frame as any).offset).to.equal(1);
    });
  });
});

// ---------------------------------------------------------------------------
// TRANSFORM NODE BUILDER TESTS
// ---------------------------------------------------------------------------

describe("Transform Node Builders", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createTransform", () => {
    it("should create table transform node", () => {
      const inputAttr = makeAttrRef("weekId", "fact_sales", "weekId", "fact");
      const outputAttr = makeAttrRef("lyWeekId", "dim_week", "ly_week_id", "dimension");

      const node = createTransform("input_1", "table", "week_to_ly_week", inputAttr, outputAttr);

      expect(node.kind).to.equal("Transform");
      expect(node.inputId).to.equal("input_1");
      expect(node.transformKind).to.equal("table");
      expect(node.transformId).to.equal("week_to_ly_week");
      expect(node.inputAttr.attributeId).to.equal("weekId");
      expect(node.outputAttr.attributeId).to.equal("lyWeekId");
      expect(node.id).to.match(/^transform_/);
    });

    it("should create rowset transform node", () => {
      const inputAttr = makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact");
      const outputAttr = makeAttrRef("lySalesAmount", "fact_sales", "ly_sales_amount", "fact");

      const node = createTransform("input_1", "rowset", "last_year", inputAttr, outputAttr);

      expect(node.transformKind).to.equal("rowset");
      expect(node.transformId).to.equal("last_year");
    });

    it("should include transform definition when provided", () => {
      const inputAttr = makeAttrRef("weekId", "fact_sales", "weekId", "fact");
      const outputAttr = makeAttrRef("lyWeekId", "dim_week", "ly_week_id", "dimension");
      const transformDef = { kind: "table" as const, sourceAttr: "weekId", lookupTable: "dim_week_mapping" };

      const node = createTransform("input_1", "table", "week_mapping", inputAttr, outputAttr, transformDef as any);

      expect(node.transformDef).to.deep.equal(transformDef);
    });
  });

  describe("createTableTransform", () => {
    it("should create table transform shorthand", () => {
      const inputAttr = makeAttrRef("weekId", "fact_sales", "weekId", "fact");
      const outputAttr = makeAttrRef("lyWeekId", "dim_week", "ly_week_id", "dimension");

      const node = createTableTransform("input_1", "week_to_ly", inputAttr, outputAttr);

      expect(node.transformKind).to.equal("table");
      expect(node.transformId).to.equal("week_to_ly");
    });
  });

  describe("createRowsetTransform", () => {
    it("should create rowset transform shorthand", () => {
      const inputAttr = makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact");
      const outputAttr = makeAttrRef("lySales", "fact_sales", "ly_sales", "fact");

      const node = createRowsetTransform("input_1", "last_year", inputAttr, outputAttr);

      expect(node.transformKind).to.equal("rowset");
      expect(node.transformId).to.equal("last_year");
    });
  });
});

// ---------------------------------------------------------------------------
// PROJECT NODE BUILDER TESTS
// ---------------------------------------------------------------------------

describe("Project Node Builder", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("createProject", () => {
    it("should create project node with outputs", () => {
      const outputs = [
        { name: "total_sales", expr: { kind: "Constant" as const, value: 100, dataType: DataTypes.number } },
        { name: "store_name", expr: makeAttrRef("storeName", "dim_store", "name", "dimension") },
      ];

      const node = createProject("input_1", outputs);

      expect(node.kind).to.equal("Project");
      expect(node.inputId).to.equal("input_1");
      expect(node.outputs).to.have.lengthOf(2);
      expect(node.outputs[0].name).to.equal("total_sales");
      expect(node.outputs[1].name).to.equal("store_name");
      expect(node.id).to.match(/^project_/);
    });

    it("should create empty project node", () => {
      const node = createProject("input_1", []);

      expect(node.outputs).to.be.empty;
    });
  });
});

// ---------------------------------------------------------------------------
// WINDOW/TRANSFORM EXTRACTION TESTS
// ---------------------------------------------------------------------------

describe("Window/Transform Extraction", () => {
  describe("isWindowExpr", () => {
    it("should return true for Window expression", () => {
      const expr: MetricExpr = {
        kind: "Window",
        base: { kind: "AttrRef", name: "salesAmount" },
        partitionBy: ["storeId"],
        orderBy: "weekId",
        frame: { kind: "rolling", count: 3 },
        aggregate: "sum",
      };

      expect(isWindowExpr(expr)).to.be.true;
    });

    it("should return false for non-Window expression", () => {
      const expr: MetricExpr = { kind: "AttrRef", name: "salesAmount" };
      expect(isWindowExpr(expr)).to.be.false;
    });
  });

  describe("isTransformExpr", () => {
    it("should return true for Transform expression", () => {
      const expr: MetricExpr = {
        kind: "Transform",
        base: { kind: "MetricRef", name: "totalSales" },
        transformId: "last_year",
        transformKind: "table",
      };

      expect(isTransformExpr(expr)).to.be.true;
    });

    it("should return false for non-Transform expression", () => {
      const expr: MetricExpr = { kind: "Literal", value: 42 };
      expect(isTransformExpr(expr)).to.be.false;
    });
  });

  describe("extractWindowInfo", () => {
    it("should extract window info from Window expression", () => {
      const expr: MetricExpr = {
        kind: "Window",
        base: { kind: "AttrRef", name: "salesAmount" },
        partitionBy: ["storeId", "regionId"],
        orderBy: "weekId",
        frame: { kind: "cumulative" },
        aggregate: "avg",
      };

      const info = extractWindowInfo(expr);

      expect(info).to.not.be.null;
      expect(info!.kind).to.equal("window");
      expect(info!.partitionBy).to.deep.equal(["storeId", "regionId"]);
      expect(info!.orderBy).to.equal("weekId");
      expect(info!.frame.kind).to.equal("cumulative");
      expect(info!.aggregate).to.equal("avg");
      expect(info!.baseExpr.kind).to.equal("AttrRef");
    });

    it("should return null for non-Window expression", () => {
      const expr: MetricExpr = { kind: "Literal", value: 100 };
      expect(extractWindowInfo(expr)).to.be.null;
    });
  });

  describe("extractTransformInfo", () => {
    it("should extract transform info from Transform expression", () => {
      const expr: MetricExpr = {
        kind: "Transform",
        base: { kind: "MetricRef", name: "totalSales" },
        transformId: "last_year",
        transformKind: "table",
        inputAttr: "weekId",
        outputAttr: "lyWeekId",
      };

      const info = extractTransformInfo(expr);

      expect(info).to.not.be.null;
      expect(info!.kind).to.equal("transform");
      expect(info!.transformId).to.equal("last_year");
      expect(info!.transformKind).to.equal("table");
      expect(info!.inputAttr).to.equal("weekId");
      expect(info!.outputAttr).to.equal("lyWeekId");
      expect(info!.baseExpr.kind).to.equal("MetricRef");
    });

    it("should return null for non-Transform expression", () => {
      const expr: MetricExpr = { kind: "AttrRef", name: "salesAmount" };
      expect(extractTransformInfo(expr)).to.be.null;
    });
  });

  describe("findWindowExprs", () => {
    it("should find all Window expressions in tree", () => {
      const expr: MetricExpr = {
        kind: "BinaryOp",
        op: "+",
        left: {
          kind: "Window",
          base: { kind: "AttrRef", name: "salesAmount" },
          partitionBy: ["storeId"],
          orderBy: "weekId",
          frame: { kind: "rolling", count: 3 },
          aggregate: "sum",
        },
        right: {
          kind: "Window",
          base: { kind: "AttrRef", name: "quantity" },
          partitionBy: ["storeId"],
          orderBy: "weekId",
          frame: { kind: "cumulative" },
          aggregate: "avg",
        },
      };

      const windows = findWindowExprs(expr);

      expect(windows).to.have.lengthOf(2);
      expect(windows[0].aggregate).to.equal("sum");
      expect(windows[1].aggregate).to.equal("avg");
    });

    it("should find nested Window expressions", () => {
      const expr: MetricExpr = {
        kind: "Window",
        base: {
          kind: "Window",
          base: { kind: "AttrRef", name: "salesAmount" },
          partitionBy: ["storeId"],
          orderBy: "weekId",
          frame: { kind: "rolling", count: 3 },
          aggregate: "sum",
        },
        partitionBy: ["regionId"],
        orderBy: "monthId",
        frame: { kind: "cumulative" },
        aggregate: "avg",
      };

      const windows = findWindowExprs(expr);
      expect(windows).to.have.lengthOf(2);
    });

    it("should return empty array for expression without windows", () => {
      const expr: MetricExpr = { kind: "Literal", value: 42 };
      expect(findWindowExprs(expr)).to.be.empty;
    });
  });

  describe("findTransformExprs", () => {
    it("should find all Transform expressions in tree", () => {
      const expr: MetricExpr = {
        kind: "BinaryOp",
        op: "-",
        left: { kind: "MetricRef", name: "currentSales" },
        right: {
          kind: "Transform",
          base: { kind: "MetricRef", name: "currentSales" },
          transformId: "last_year",
          transformKind: "table",
        },
      };

      const transforms = findTransformExprs(expr);

      expect(transforms).to.have.lengthOf(1);
      expect(transforms[0].transformId).to.equal("last_year");
    });

    it("should find nested Transform expressions", () => {
      const expr: MetricExpr = {
        kind: "Transform",
        base: {
          kind: "Transform",
          base: { kind: "MetricRef", name: "sales" },
          transformId: "last_week",
          transformKind: "table",
        },
        transformId: "last_year",
        transformKind: "table",
      };

      const transforms = findTransformExprs(expr);
      expect(transforms).to.have.lengthOf(2);
    });

    it("should return empty array for expression without transforms", () => {
      const expr: MetricExpr = { kind: "AttrRef", name: "salesAmount" };
      expect(findTransformExprs(expr)).to.be.empty;
    });
  });
});

// ---------------------------------------------------------------------------
// WINDOW/TRANSFORM PLAN NODE CONVERSION TESTS
// ---------------------------------------------------------------------------

describe("Window/Transform Plan Node Conversion", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  describe("windowInfoToPlanNode", () => {
    it("should convert window info to plan node", () => {
      const info = {
        kind: "window" as const,
        partitionBy: ["storeId"],
        orderBy: "weekId",
        frame: { kind: "rolling" as const, count: 7 },
        aggregate: "sum" as const,
        baseExpr: { kind: "AttrRef" as const, name: "salesAmount" },
      };

      const node = windowInfoToPlanNode(info, "input_1", model, "rolling_sales");

      expect(node.kind).to.equal("Window");
      expect(node.inputId).to.equal("input_1");
      expect(node.partitionBy).to.have.lengthOf(1);
      expect(node.partitionBy[0].attributeId).to.equal("storeId");
      expect(node.orderBy).to.have.lengthOf(1);
      expect(node.orderBy[0].attr.attributeId).to.equal("weekId");
      expect(node.frame.kind).to.equal("rolling");
      expect(node.windowFunctions[0].outputName).to.equal("rolling_sales");
      expect(node.windowFunctions[0].op).to.equal("sum");
    });

    it("should handle empty partition/order attributes", () => {
      const info = {
        kind: "window" as const,
        partitionBy: [],
        orderBy: "unknownAttr", // not in model
        frame: { kind: "cumulative" as const },
        aggregate: "avg" as const,
        baseExpr: { kind: "Literal" as const, value: 1 },
      };

      const node = windowInfoToPlanNode(info, "input_1", model, "cumulative_avg");

      expect(node.partitionBy).to.be.empty;
      expect(node.orderBy).to.be.empty; // unknown attr resolves to empty
    });
  });

  describe("transformInfoToPlanNode", () => {
    it("should convert transform info to plan node", () => {
      const info = {
        kind: "transform" as const,
        transformId: "week_to_ly",
        transformKind: "table" as const,
        inputAttr: "weekId",
        outputAttr: "weekName",
        baseExpr: { kind: "MetricRef" as const, name: "totalSales" },
      };

      const node = transformInfoToPlanNode(info, "input_1", model);

      expect(node).to.not.be.null;
      expect(node!.kind).to.equal("Transform");
      expect(node!.inputId).to.equal("input_1");
      expect(node!.transformKind).to.equal("table");
      expect(node!.transformId).to.equal("week_to_ly");
      expect(node!.inputAttr.attributeId).to.equal("weekId");
      expect(node!.outputAttr.attributeId).to.equal("weekName");
    });

    it("should return null if input attribute not found", () => {
      const info = {
        kind: "transform" as const,
        transformId: "test",
        transformKind: "rowset" as const,
        inputAttr: "unknownAttr",
        outputAttr: "weekId",
        baseExpr: { kind: "Literal" as const, value: 1 },
      };

      const node = transformInfoToPlanNode(info, "input_1", model);
      expect(node).to.be.null;
    });

    it("should return null if output attribute not found", () => {
      const info = {
        kind: "transform" as const,
        transformId: "test",
        transformKind: "rowset" as const,
        inputAttr: "weekId",
        outputAttr: "unknownOutput",
        baseExpr: { kind: "Literal" as const, value: 1 },
      };

      const node = transformInfoToPlanNode(info, "input_1", model);
      expect(node).to.be.null;
    });

    it("should return null if no input/output attrs specified", () => {
      const info = {
        kind: "transform" as const,
        transformId: "test",
        transformKind: "rowset" as const,
        inputAttr: undefined,
        outputAttr: undefined,
        baseExpr: { kind: "Literal" as const, value: 1 },
      };

      const node = transformInfoToPlanNode(info, "input_1", model);
      expect(node).to.be.null;
    });
  });
});

// ---------------------------------------------------------------------------
// PLAN VISUALIZATION WITH WINDOW/TRANSFORM TESTS
// ---------------------------------------------------------------------------

describe("Plan Visualization with Window/Transform", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  it("should format window node in plan", () => {
    const dag = new PlanDag();
    const factScan = createFactScan("fact_sales", []);
    const window = createRollingWindow(
      factScan.id,
      [makeAttrRef("storeId", "fact_sales", "storeId", "fact")],
      makeAttrRef("weekId", "dim_week", "id", "dimension"),
      7,
      [{ outputName: "rolling_sum", op: "sum", input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact") }]
    );

    dag.addNode(factScan);
    dag.addNode(window);
    dag.setRoot(window.id);

    const output = formatPlanDag(dag);

    expect(output).to.include("Window");
    expect(output).to.include("FactScan");
  });

  it("should format transform node in plan", () => {
    const dag = new PlanDag();
    const factScan = createFactScan("fact_sales", []);
    const transform = createRowsetTransform(
      factScan.id,
      "last_year",
      makeAttrRef("weekId", "fact_sales", "weekId", "fact"),
      makeAttrRef("lyWeekId", "dim_week", "ly_id", "dimension")
    );

    dag.addNode(factScan);
    dag.addNode(transform);
    dag.setRoot(transform.id);

    const output = formatPlanDag(dag);

    expect(output).to.include("Transform");
    expect(output).to.include("last_year");
    expect(output).to.include("FactScan");
  });

  it("should format project node in plan", () => {
    const dag = new PlanDag();
    const factScan = createFactScan("fact_sales", []);
    const project = createProject(factScan.id, [
      { name: "sales", expr: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact") }
    ]);

    dag.addNode(factScan);
    dag.addNode(project);
    dag.setRoot(project.id);

    const output = formatPlanDag(dag);

    expect(output).to.include("Project");
    expect(output).to.include("FactScan");
  });
});

// ---------------------------------------------------------------------------
// METRIC DEPENDENCY ANALYSIS TESTS (Phase 4)
// ---------------------------------------------------------------------------

describe("Metric Dependency Analysis", () => {
  describe("analyzeMetricDependencies", () => {
    it("should find no dependencies in simple aggregate", () => {
      const expr = {
        kind: "Aggregate" as const,
        op: "sum" as const,
        input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
        distinct: false,
        resultType: DataTypes.number,
      };

      const deps = analyzeMetricDependencies(expr);
      expect(deps.size).to.equal(0);
    });

    it("should find metric reference dependencies", () => {
      const expr = {
        kind: "ScalarOp" as const,
        op: "/" as const,
        left: {
          kind: "MetricRef" as const,
          metricName: "totalSales",
          baseFact: "fact_sales",
          resultType: DataTypes.number,
        },
        right: {
          kind: "MetricRef" as const,
          metricName: "orderCount",
          baseFact: "fact_sales",
          resultType: DataTypes.number,
        },
        resultType: DataTypes.number,
      };

      const deps = analyzeMetricDependencies(expr);
      expect(deps.size).to.equal(2);
      expect(deps.has("totalSales")).to.be.true;
      expect(deps.has("orderCount")).to.be.true;
    });

    it("should find dependencies in nested expressions", () => {
      const expr = {
        kind: "ScalarFunction" as const,
        fn: "abs",
        args: [
          {
            kind: "ScalarOp" as const,
            op: "-" as const,
            left: {
              kind: "MetricRef" as const,
              metricName: "currentSales",
              baseFact: null,
              resultType: DataTypes.number,
            },
            right: {
              kind: "MetricRef" as const,
              metricName: "lastYearSales",
              baseFact: null,
              resultType: DataTypes.number,
            },
            resultType: DataTypes.number,
          },
        ],
        resultType: DataTypes.number,
      };

      const deps = analyzeMetricDependencies(expr);
      expect(deps.size).to.equal(2);
      expect(deps.has("currentSales")).to.be.true;
      expect(deps.has("lastYearSales")).to.be.true;
    });
  });

  describe("buildDependencyGraph", () => {
    it("should build graph from metric expressions", () => {
      const metricNames = ["totalSales", "orderCount", "avgTicket"];
      const metricExprs = new Map<string, any>([
        ["totalSales", {
          kind: "Aggregate",
          op: "sum",
          input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
          distinct: false,
          resultType: DataTypes.number,
        }],
        ["orderCount", {
          kind: "Aggregate",
          op: "count",
          input: makeAttrRef("orderId", "fact_sales", "orderId", "fact"),
          distinct: false,
          resultType: DataTypes.number,
        }],
        ["avgTicket", {
          kind: "ScalarOp",
          op: "/",
          left: { kind: "MetricRef", metricName: "totalSales", baseFact: null, resultType: DataTypes.number },
          right: { kind: "MetricRef", metricName: "orderCount", baseFact: null, resultType: DataTypes.number },
          resultType: DataTypes.number,
        }],
      ]);

      const graph = buildDependencyGraph(metricNames, metricExprs);

      expect(graph.get("totalSales")?.size).to.equal(0);
      expect(graph.get("orderCount")?.size).to.equal(0);
      expect(graph.get("avgTicket")?.size).to.equal(2);
      expect(graph.get("avgTicket")?.has("totalSales")).to.be.true;
      expect(graph.get("avgTicket")?.has("orderCount")).to.be.true;
    });
  });

  describe("detectCycle", () => {
    it("should return null for acyclic graph", () => {
      const graph = new Map<string, Set<string>>([
        ["a", new Set(["b", "c"])],
        ["b", new Set(["c"])],
        ["c", new Set()],
      ]);

      expect(detectCycle(graph)).to.be.null;
    });

    it("should detect simple cycle", () => {
      const graph = new Map<string, Set<string>>([
        ["a", new Set(["b"])],
        ["b", new Set(["a"])],
      ]);

      const cycle = detectCycle(graph);
      expect(cycle).to.not.be.null;
      expect(cycle).to.include("a");
      expect(cycle).to.include("b");
    });

    it("should detect longer cycle", () => {
      const graph = new Map<string, Set<string>>([
        ["a", new Set(["b"])],
        ["b", new Set(["c"])],
        ["c", new Set(["a"])],
      ]);

      const cycle = detectCycle(graph);
      expect(cycle).to.not.be.null;
      expect(cycle!.length).to.be.greaterThanOrEqual(3);
    });

    it("should detect self-reference cycle", () => {
      const graph = new Map<string, Set<string>>([
        ["a", new Set(["a"])],
      ]);

      const cycle = detectCycle(graph);
      expect(cycle).to.not.be.null;
    });
  });

  describe("topologicalSortMetrics", () => {
    it("should sort metrics with no dependencies first", () => {
      const metricNames = ["avgTicket", "totalSales", "orderCount"];
      const graph = new Map<string, Set<string>>([
        ["totalSales", new Set()],
        ["orderCount", new Set()],
        ["avgTicket", new Set(["totalSales", "orderCount"])],
      ]);

      const { order, phases } = topologicalSortMetrics(metricNames, graph);

      expect(order.indexOf("totalSales")).to.be.lessThan(order.indexOf("avgTicket"));
      expect(order.indexOf("orderCount")).to.be.lessThan(order.indexOf("avgTicket"));
      expect(phases.get("totalSales")).to.equal(0);
      expect(phases.get("orderCount")).to.equal(0);
      expect(phases.get("avgTicket")).to.equal(1);
    });

    it("should handle multi-level dependencies", () => {
      const metricNames = ["base", "derived1", "derived2"];
      const graph = new Map<string, Set<string>>([
        ["base", new Set()],
        ["derived1", new Set(["base"])],
        ["derived2", new Set(["derived1"])],
      ]);

      const { order, phases } = topologicalSortMetrics(metricNames, graph);

      expect(order).to.deep.equal(["base", "derived1", "derived2"]);
      expect(phases.get("base")).to.equal(0);
      expect(phases.get("derived1")).to.equal(1);
      expect(phases.get("derived2")).to.equal(2);
    });

    it("should throw MetricCycleError for circular dependencies", () => {
      const metricNames = ["a", "b"];
      const graph = new Map<string, Set<string>>([
        ["a", new Set(["b"])],
        ["b", new Set(["a"])],
      ]);

      expect(() => topologicalSortMetrics(metricNames, graph)).to.throw(MetricCycleError);
    });
  });
});

// ---------------------------------------------------------------------------
// FILTER CLASSIFICATION TESTS (Phase 4)
// ---------------------------------------------------------------------------

describe("Filter Classification", () => {
  describe("classifyFilter", () => {
    it("should classify attribute-only filter as pre-aggregate", () => {
      const predicate: any = {
        kind: "Comparison",
        left: makeAttrRef("storeId", "dim_store", "id", "dimension"),
        op: "=",
        right: { kind: "Constant", value: 1, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };

      const metricNames = new Set(["totalSales", "orderCount"]);
      expect(classifyFilter(predicate, metricNames)).to.equal("pre");
    });

    it("should classify metric reference filter as post-aggregate", () => {
      const predicate: any = {
        kind: "Comparison",
        left: { kind: "MetricRef", metricName: "totalSales", baseFact: null, resultType: DataTypes.number },
        op: ">",
        right: { kind: "Constant", value: 1000, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };

      const metricNames = new Set(["totalSales", "orderCount"]);
      expect(classifyFilter(predicate, metricNames)).to.equal("post");
    });

    it("should classify aggregate function filter as post-aggregate", () => {
      const predicate: any = {
        kind: "Comparison",
        left: {
          kind: "Aggregate",
          op: "sum",
          input: makeAttrRef("salesAmount", "fact_sales", "salesAmount", "fact"),
          distinct: false,
          resultType: DataTypes.number,
        },
        op: ">",
        right: { kind: "Constant", value: 500, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };

      const metricNames = new Set<string>();
      expect(classifyFilter(predicate, metricNames)).to.equal("post");
    });
  });
});

// ---------------------------------------------------------------------------
// BUILD LOGICAL PLAN TESTS (Phase 4)
// ---------------------------------------------------------------------------

describe("buildLogicalPlan", () => {
  beforeEach(() => {
    resetNodeIdCounter();
  });

  it("should build plan for simple aggregate query", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);

    expect(plan.rootNodeId).to.not.be.null;
    expect(plan.outputGrain.dimensions).to.have.lengthOf(1);
    expect(plan.outputGrain.dimensions[0].attributeId).to.equal("storeName");
    expect(plan.outputMetrics).to.have.lengthOf(1);
    expect(plan.outputMetrics[0].name).to.equal("totalSales");
    expect(plan.metricEvalOrder).to.deep.equal(["totalSales"]);
  });

  it("should compute correct execution phases for derived metrics", () => {
    // Add derived metric to model using proper builders
    const extendedModel: SemanticModel = {
      ...model,
      metrics: {
        ...model.metrics,
        orderCount: aggregateMetric("orderCount", "fact_sales", "salesAmount", "count"),
        avgTicket: buildMetricFromExpr({
          name: "avgTicket",
          baseFact: "fact_sales",
          expr: Expr.div(Expr.metric("totalSales"), Expr.metric("orderCount")),
        }),
      },
    };

    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales", "orderCount", "avgTicket"],
    };

    const plan = buildLogicalPlan(query, extendedModel);

    // Base metrics should be phase 0, derived should be phase 1
    const totalSalesPlan = plan.outputMetrics.find((m) => m.name === "totalSales");
    const orderCountPlan = plan.outputMetrics.find((m) => m.name === "orderCount");
    const avgTicketPlan = plan.outputMetrics.find((m) => m.name === "avgTicket");

    expect(totalSalesPlan?.executionPhase).to.equal(0);
    expect(orderCountPlan?.executionPhase).to.equal(0);
    expect(avgTicketPlan?.executionPhase).to.equal(1);
  });

  it("should include aggregate nodes in plan", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);

    const aggNodes = [...plan.nodes.values()].filter((n) => n.kind === "Aggregate");
    expect(aggNodes.length).to.be.greaterThan(0);
  });

  it("should build join plan for multi-table queries", () => {
    const query = {
      dimensions: ["storeName", "weekName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);

    const joinNodes = [...plan.nodes.values()].filter((n) => n.kind === "Join");
    expect(joinNodes.length).to.be.greaterThanOrEqual(1);
  });

  it("should throw for unknown metric", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["unknownMetric"],
    };

    expect(() => buildLogicalPlan(query, model)).to.throw("Unknown metric");
  });

  it("should throw for circular metric dependencies", () => {
    const circularModel: SemanticModel = {
      ...model,
      metrics: {
        metricA: buildMetricFromExpr({
          name: "metricA",
          baseFact: "fact_sales",
          expr: Expr.add(Expr.metric("metricB"), Expr.lit(1)),
        }),
        metricB: buildMetricFromExpr({
          name: "metricB",
          baseFact: "fact_sales",
          expr: Expr.add(Expr.metric("metricA"), Expr.lit(1)),
        }),
      },
    };

    const query = {
      dimensions: ["storeName"],
      metrics: ["metricA", "metricB"],
    };

    expect(() => buildLogicalPlan(query, circularModel)).to.throw(MetricCycleError);
  });

  it("should compute correct grainId", () => {
    const query = {
      dimensions: ["weekName", "storeName"], // Note: unsorted
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);

    // grainId should be sorted alphabetically
    expect(plan.outputGrain.grainId).to.equal("storeName,weekName");
  });

  it("should collect required attributes from metrics", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);

    const totalSalesPlan = plan.outputMetrics.find((m) => m.name === "totalSales");
    expect(totalSalesPlan?.requiredAttrs.length).to.be.greaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// PHASE 5: EXPLAIN AND INTEGRATION TESTS
// ---------------------------------------------------------------------------

// Helper functions to create LogicalExpr nodes with proper type definitions
const mkConst = (value: number | string | boolean | null): LogicalConstant => ({
  kind: "Constant",
  value,
  dataType: typeof value === "number" ? DataTypes.number :
            typeof value === "string" ? DataTypes.string :
            typeof value === "boolean" ? DataTypes.boolean : DataTypes.null,
});

const mkAttr = (attributeId: string): LogicalAttributeRef => ({
  kind: "AttributeRef",
  attributeId,
  logicalName: attributeId,
  physicalTable: "test_table",
  physicalColumn: attributeId,
  dataType: DataTypes.string,
  sourceKind: "fact",
});

const mkNumAttr = (attributeId: string): LogicalAttributeRef => ({
  kind: "AttributeRef",
  attributeId,
  logicalName: attributeId,
  physicalTable: "test_table",
  physicalColumn: attributeId,
  dataType: DataTypes.number,
  sourceKind: "fact",
});

const mkMetricRef = (metricName: string): LogicalExpr => ({
  kind: "MetricRef",
  metricName,
  baseFact: "fact_sales",
  resultType: DataTypes.number,
});

const mkScalarOp = (op: "+" | "-" | "*" | "/" | "%", left: LogicalExpr, right: LogicalExpr): LogicalScalarOp => ({
  kind: "ScalarOp",
  op,
  left,
  right,
  resultType: DataTypes.number,
});

const mkComparison = (op: "=" | "!=" | "<" | "<=" | ">" | ">=", left: LogicalExpr, right: LogicalExpr): LogicalComparison => ({
  kind: "Comparison",
  op,
  left,
  right,
  resultType: { kind: "boolean" },
});

const mkAggregate = (op: "sum" | "avg" | "min" | "max" | "count", input: LogicalExpr): LogicalAggregate => ({
  kind: "Aggregate",
  op,
  input,
  distinct: false,
  resultType: DataTypes.number,
});

const mkCoalesce = (...exprs: LogicalExpr[]): LogicalCoalesce => ({
  kind: "Coalesce",
  exprs,
  resultType: DataTypes.number,
});

const mkInList = (expr: LogicalExpr, values: LogicalConstant[], negated: boolean = false): LogicalInList => ({
  kind: "InList",
  expr,
  values,
  negated,
  resultType: { kind: "boolean" },
});

const mkBetween = (expr: LogicalExpr, low: LogicalExpr, high: LogicalExpr): LogicalBetween => ({
  kind: "Between",
  expr,
  low,
  high,
  resultType: { kind: "boolean" },
});

const mkIsNull = (expr: LogicalExpr, negated: boolean = false): LogicalIsNull => ({
  kind: "IsNull",
  expr,
  negated,
  resultType: { kind: "boolean" },
});

const mkScalarFn = (fn: string, ...args: LogicalExpr[]): LogicalScalarFunction => ({
  kind: "ScalarFunction",
  fn,
  args,
  resultType: DataTypes.number,
});

const mkLogicalOp = (op: "and" | "or" | "not", ...operands: LogicalComparison[]): LogicalLogicalOp => ({
  kind: "LogicalOp",
  op,
  operands,
  resultType: { kind: "boolean" },
});

const mkConditional = (condition: LogicalExpr, thenExpr: LogicalExpr, elseExpr: LogicalExpr): LogicalConditional => ({
  kind: "Conditional",
  condition,
  thenExpr,
  elseExpr,
  resultType: DataTypes.string,
});

describe("formatLogicalExpr", () => {
  it("should format Constant expressions", () => {
    expect(formatLogicalExpr(mkConst(42))).to.equal("42");
    expect(formatLogicalExpr(mkConst("hello"))).to.equal("hello");
  });

  it("should format AttributeRef expressions", () => {
    expect(formatLogicalExpr(mkAttr("storeName"))).to.equal("storeName");
  });

  it("should format MetricRef expressions", () => {
    expect(formatLogicalExpr(mkMetricRef("totalSales"))).to.equal("@totalSales");
  });

  it("should format Aggregate expressions", () => {
    expect(formatLogicalExpr(mkAggregate("sum", mkNumAttr("amount")))).to.equal("sum(amount)");
  });

  it("should format ScalarOp expressions", () => {
    expect(formatLogicalExpr(mkScalarOp("+", mkConst(10), mkConst(5)))).to.equal("(10 + 5)");
  });

  it("should format Comparison expressions", () => {
    expect(formatLogicalExpr(mkComparison(">", mkNumAttr("amount"), mkConst(100)))).to.equal("(amount > 100)");
  });

  it("should format Conditional expressions", () => {
    const cond = mkConditional(mkComparison(">", mkConst(10), mkConst(5)), mkConst("yes"), mkConst("no"));
    expect(formatLogicalExpr(cond)).to.equal("IF((10 > 5), yes, no)");
  });

  it("should format Coalesce expressions", () => {
    expect(formatLogicalExpr(mkCoalesce(mkNumAttr("val1"), mkConst(0)))).to.equal("COALESCE(val1, 0)");
  });

  it("should format LogicalOp expressions", () => {
    const logicalAnd = mkLogicalOp("and", mkComparison(">", mkConst(10), mkConst(5)), mkComparison("<", mkConst(20), mkConst(30)));
    expect(formatLogicalExpr(logicalAnd)).to.equal("((10 > 5) AND (20 < 30))");

    const logicalNot = mkLogicalOp("not", mkComparison("=", mkConst(1), mkConst(1)));
    expect(formatLogicalExpr(logicalNot)).to.equal("NOT((1 = 1))");
  });

  it("should format InList expressions", () => {
    const inList = mkInList(mkAttr("status"), [mkConst("active"), mkConst("pending")]);
    expect(formatLogicalExpr(inList)).to.equal("(status IN (active, pending))");

    const notInList = mkInList(mkAttr("status"), [mkConst("active"), mkConst("pending")], true);
    expect(formatLogicalExpr(notInList)).to.equal("(status NOT IN (active, pending))");
  });

  it("should format Between expressions", () => {
    expect(formatLogicalExpr(mkBetween(mkNumAttr("amount"), mkConst(10), mkConst(100)))).to.equal("(amount BETWEEN 10 AND 100)");
  });

  it("should format IsNull expressions", () => {
    expect(formatLogicalExpr(mkIsNull(mkNumAttr("value")))).to.equal("(value IS NULL)");
    expect(formatLogicalExpr(mkIsNull(mkNumAttr("value"), true))).to.equal("(value IS NOT NULL)");
  });
});

describe("compileLogicalExpr", () => {
  const mockContext: LogicalExprEvalContext = {
    row: { amount: 100, status: "active" },
    getMetric: (name) => (name === "totalSales" ? 1000 : undefined),
    getAttribute: (id) => (mockContext.row as Record<string, unknown>)[id],
  };

  it("should evaluate Constant expressions", () => {
    expect(compileLogicalExpr(mkConst(42))(mockContext)).to.equal(42);
  });

  it("should evaluate AttributeRef expressions", () => {
    expect(compileLogicalExpr(mkNumAttr("amount"))(mockContext)).to.equal(100);
  });

  it("should evaluate MetricRef expressions", () => {
    expect(compileLogicalExpr(mkMetricRef("totalSales"))(mockContext)).to.equal(1000);
  });

  it("should evaluate ScalarOp expressions", () => {
    expect(compileLogicalExpr(mkScalarOp("+", mkConst(10), mkConst(5)))(mockContext)).to.equal(15);
    expect(compileLogicalExpr(mkScalarOp("-", mkConst(10), mkConst(5)))(mockContext)).to.equal(5);
    expect(compileLogicalExpr(mkScalarOp("*", mkConst(10), mkConst(5)))(mockContext)).to.equal(50);
    expect(compileLogicalExpr(mkScalarOp("/", mkConst(10), mkConst(5)))(mockContext)).to.equal(2);
  });

  it("should handle division by zero", () => {
    expect(compileLogicalExpr(mkScalarOp("/", mkConst(10), mkConst(0)))(mockContext)).to.be.undefined;
  });

  it("should evaluate Comparison expressions", () => {
    expect(compileLogicalExpr(mkComparison(">", mkConst(10), mkConst(5)))(mockContext)).to.be.true;
    expect(compileLogicalExpr(mkComparison("<", mkConst(10), mkConst(5)))(mockContext)).to.be.false;
    expect(compileLogicalExpr(mkComparison("=", mkConst(10), mkConst(10)))(mockContext)).to.be.true;
  });

  it("should evaluate LogicalOp expressions", () => {
    const andOp = mkLogicalOp("and", mkComparison(">", mkConst(10), mkConst(5)), mkComparison("<", mkConst(3), mkConst(8)));
    expect(compileLogicalExpr(andOp)(mockContext)).to.be.true;

    const orOp = mkLogicalOp("or", mkComparison("<", mkConst(10), mkConst(5)), mkComparison(">", mkConst(10), mkConst(5)));
    expect(compileLogicalExpr(orOp)(mockContext)).to.be.true;

    const notOp = mkLogicalOp("not", mkComparison(">", mkConst(5), mkConst(10)));
    expect(compileLogicalExpr(notOp)(mockContext)).to.be.true;
  });

  it("should evaluate Conditional expressions", () => {
    const condTrue = mkConditional(mkComparison(">", mkConst(10), mkConst(5)), mkConst("yes"), mkConst("no"));
    expect(compileLogicalExpr(condTrue)(mockContext)).to.equal("yes");

    const condFalse = mkConditional(mkComparison("<", mkConst(10), mkConst(5)), mkConst("yes"), mkConst("no"));
    expect(compileLogicalExpr(condFalse)(mockContext)).to.equal("no");
  });

  it("should evaluate Coalesce expressions", () => {
    expect(compileLogicalExpr(mkCoalesce(mkConst(null), mkConst(42)))(mockContext)).to.equal(42);
  });

  it("should evaluate InList expressions", () => {
    const inList = mkInList(mkConst("active"), [mkConst("active"), mkConst("pending")]);
    expect(compileLogicalExpr(inList)(mockContext)).to.be.true;

    const notInList = mkInList(mkConst("active"), [mkConst("active"), mkConst("pending")], true);
    expect(compileLogicalExpr(notInList)(mockContext)).to.be.false;
  });

  it("should evaluate Between expressions", () => {
    expect(compileLogicalExpr(mkBetween(mkConst(50), mkConst(10), mkConst(100)))(mockContext)).to.be.true;
    expect(compileLogicalExpr(mkBetween(mkConst(5), mkConst(10), mkConst(100)))(mockContext)).to.be.false;
  });

  it("should evaluate IsNull expressions", () => {
    expect(compileLogicalExpr(mkIsNull(mkConst(null)))(mockContext)).to.be.true;
    expect(compileLogicalExpr(mkIsNull(mkConst(null), true))(mockContext)).to.be.false;
  });

  it("should evaluate ScalarFunction expressions", () => {
    expect(compileLogicalExpr(mkScalarFn("abs", mkConst(-42)))(mockContext)).to.equal(42);
    expect(compileLogicalExpr(mkScalarFn("upper", mkConst("hello")))(mockContext)).to.equal("HELLO");
    expect(compileLogicalExpr(mkScalarFn("concat", mkConst("hello"), mkConst(" "), mkConst("world")))(mockContext)).to.equal("hello world");
  });
});

describe("compileLogicalExprToSql", () => {
  it("should compile Constant expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkConst(42))).to.equal("42");
    expect(compileLogicalExprToSql(mkConst("hello"))).to.equal("'hello'");
    expect(compileLogicalExprToSql(mkConst(null))).to.equal("NULL");
    expect(compileLogicalExprToSql(mkConst(true))).to.equal("TRUE");
  });

  it("should escape single quotes in strings", () => {
    expect(compileLogicalExprToSql(mkConst("it's"))).to.equal("'it''s'");
  });

  it("should compile AttributeRef expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkAttr("storeName"))).to.equal("storeName");

    // With alias map
    const aliasMap = new Map([["storeName", "s.name"]]);
    expect(compileLogicalExprToSql(mkAttr("storeName"), aliasMap)).to.equal("s.name");
  });

  it("should compile MetricRef expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkMetricRef("totalSales"))).to.equal('"totalSales"');
  });

  it("should compile Aggregate expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkAggregate("sum", mkNumAttr("amount")))).to.equal("SUM(amount)");
  });

  it("should compile ScalarOp expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkScalarOp("+", mkConst(10), mkConst(5)))).to.equal("(10 + 5)");
  });

  it("should compile Comparison expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkComparison("!=", mkAttr("status"), mkConst("inactive")))).to.equal("(status <> 'inactive')");
  });

  it("should compile Conditional expressions to SQL CASE", () => {
    const cond = mkConditional(mkComparison(">", mkConst(10), mkConst(5)), mkConst("yes"), mkConst("no"));
    expect(compileLogicalExprToSql(cond)).to.equal("CASE WHEN (10 > 5) THEN 'yes' ELSE 'no' END");
  });

  it("should compile Coalesce expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkCoalesce(mkNumAttr("val1"), mkConst(0)))).to.equal("COALESCE(val1, 0)");
  });

  it("should compile LogicalOp expressions to SQL", () => {
    const andOp = mkLogicalOp("and", mkComparison(">", mkConst(10), mkConst(5)), mkComparison("<", mkConst(20), mkConst(30)));
    expect(compileLogicalExprToSql(andOp)).to.equal("((10 > 5) AND (20 < 30))");

    const notOp = mkLogicalOp("not", mkComparison("=", mkConst(1), mkConst(1)));
    expect(compileLogicalExprToSql(notOp)).to.equal("NOT ((1 = 1))");
  });

  it("should compile InList expressions to SQL", () => {
    const inList = mkInList(mkAttr("status"), [mkConst("active"), mkConst("pending")]);
    expect(compileLogicalExprToSql(inList)).to.equal("(status IN ('active', 'pending'))");

    const notInList = mkInList(mkAttr("status"), [mkConst("active"), mkConst("pending")], true);
    expect(compileLogicalExprToSql(notInList)).to.equal("(status NOT IN ('active', 'pending'))");
  });

  it("should compile Between expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkBetween(mkNumAttr("amount"), mkConst(10), mkConst(100)))).to.equal("(amount BETWEEN 10 AND 100)");
  });

  it("should compile IsNull expressions to SQL", () => {
    expect(compileLogicalExprToSql(mkIsNull(mkNumAttr("value")))).to.equal("(value IS NULL)");
    expect(compileLogicalExprToSql(mkIsNull(mkNumAttr("value"), true))).to.equal("(value IS NOT NULL)");
  });
});

describe("explainPlan", () => {
  it("should generate EXPLAIN output for a simple plan", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);
    const output = explainPlan(plan);

    expect(output).to.include("EXPLAIN:");
    expect(output).to.include("Plan DAG:");
    expect(output).to.include("Output Grain:");
    expect(output).to.include("Metrics (evaluation order):");
    expect(output).to.include("totalSales");
  });

  it("should show verbose information when requested", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);
    const output = explainPlan(plan, { verbose: true });

    expect(output).to.include("columns:");
  });

  it("should show expressions when requested", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);
    const output = explainPlan(plan, { showExpressions: true });

    expect(output).to.include("Phase 0:");
  });

  it("should show metrics grouped by execution phase", () => {
    // Create a model with derived metric
    const derivedModel: SemanticModel = {
      ...model,
      metrics: {
        totalSales,
        avgSales: buildMetricFromExpr({
          name: "avgSales",
          baseFact: "fact_sales",
          expr: Expr.div(Expr.metric("totalSales"), Expr.lit(10)),
        }),
      },
    };

    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales", "avgSales"],
    };

    const plan = buildLogicalPlan(query, derivedModel);
    const output = explainPlan(plan, { showExpressions: true });

    expect(output).to.include("Phase 0:");
    expect(output).to.include("Phase 1:");
  });
});

describe("buildQueryPlan", () => {
  it("should build a plan and return it", () => {
    const spec = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const result = buildQueryPlan(model, spec, { includePlan: true });

    expect(result.plan).to.exist;
    expect(result.metadata?.planBuildTimeMs).to.be.a("number");
  });

  it("should return EXPLAIN output when explain=true", () => {
    const spec = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const result = buildQueryPlan(model, spec, { explain: true });

    expect(result.data).to.be.a("string");
    expect(result.data).to.include("EXPLAIN:");
    expect(result.plan).to.exist;
  });

  it("should pass explainOptions to explainPlan", () => {
    const spec = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const result = buildQueryPlan(model, spec, {
      explain: true,
      explainOptions: { verbose: true },
    });

    expect(result.data).to.include("columns:");
  });
});

describe("planToSql", () => {
  it("should generate SQL from a plan", () => {
    const query = {
      dimensions: ["storeName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);
    const sql = planToSql(plan);

    expect(sql).to.include("SELECT");
    expect(sql).to.include("storeName");
    expect(sql).to.include("totalSales");
    expect(sql).to.include("FROM");
    expect(sql).to.include("GROUP BY");
  });

  it("should generate SQL with multiple dimensions", () => {
    const query = {
      dimensions: ["storeName", "weekName"],
      metrics: ["totalSales"],
    };

    const plan = buildLogicalPlan(query, model);
    const sql = planToSql(plan);

    expect(sql).to.include("storeName");
    expect(sql).to.include("weekName");
    expect(sql).to.include("GROUP BY storeName, weekName");
  });
});
