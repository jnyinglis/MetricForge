import { expect } from "chai";
import {
  LogicalAttribute,
  MetricDefinition,
  SemanticModel,
  MetricExpr,
  aggregateMetric,
} from "../src/semanticEngine";
import {
  LogicalAttributeRef,
  LogicalAggregate,
  DataTypes,
  FactScanNode,
  DimensionScanNode,
  JoinNode,
  AggregateNode,
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
