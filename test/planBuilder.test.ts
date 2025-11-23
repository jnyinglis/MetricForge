import { expect } from "chai";
import {
  LogicalAttribute,
  MetricDefinition,
  SemanticModel,
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
