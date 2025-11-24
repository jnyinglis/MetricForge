import { expect } from "chai";
import {
  LogicalAttribute,
  MetricDefinition,
  SemanticModel,
  Expr,
  MetricExpr,
  aggregateMetric,
} from "../src/semanticEngine";
import {
  LogicalExpr,
  LogicalConstant,
  LogicalAttributeRef,
  LogicalMetricRef,
  LogicalAggregate,
  LogicalScalarOp,
  DataTypes,
  isLogicalPredicate,
  LogicalExprBuilder,
  computeGrainId,
} from "../src/logicalAst";
import {
  syntaxToLogical,
  TransformationError,
  resolveAttribute,
  collectAttributeRefs,
  collectMetricRefs,
} from "../src/syntaxToLogical";

// ---------------------------------------------------------------------------
// TEST FIXTURES
// ---------------------------------------------------------------------------

const attributes: Record<string, LogicalAttribute> = {
  storeId: { table: "dim_store", column: "id" },
  storeName: { table: "dim_store", column: "name" },
  month: { table: "fact_sales" },
  salesAmount: { table: "fact_sales" },
  orderId: { table: "fact_orders" },
  quantity: { table: "fact_orders" },
  week: { table: "dim_week", column: "id" },
};

const totalSales = aggregateMetric(
  "totalSales",
  "fact_sales",
  "salesAmount",
  "sum"
);
const orderCount: MetricDefinition = {
  name: "orderCount",
  baseFact: "fact_orders",
  attributes: ["orderId"],
  eval: () => 0,
};

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
      fact: "fact_orders",
      dimension: "dim_store",
      factKey: "storeId",
      dimensionKey: "id",
    },
  ],
  metrics: {
    totalSales,
    orderCount,
  },
};

// ---------------------------------------------------------------------------
// TYPE DEFINITIONS TESTS
// ---------------------------------------------------------------------------

describe("LogicalAst Type Definitions", () => {
  describe("DataTypes", () => {
    it("should have correct constant types", () => {
      expect(DataTypes.number).to.deep.equal({ kind: "number" });
      expect(DataTypes.integer).to.deep.equal({
        kind: "number",
        precision: "integer",
      });
      expect(DataTypes.decimal).to.deep.equal({
        kind: "number",
        precision: "decimal",
      });
      expect(DataTypes.string).to.deep.equal({ kind: "string" });
      expect(DataTypes.boolean).to.deep.equal({ kind: "boolean" });
      expect(DataTypes.null).to.deep.equal({ kind: "null" });
      expect(DataTypes.unknown).to.deep.equal({ kind: "unknown" });
    });
  });

  describe("isLogicalPredicate", () => {
    it("should return true for boolean expressions", () => {
      const comparison: LogicalExpr = {
        kind: "Comparison",
        left: { kind: "Constant", value: 1, dataType: DataTypes.number },
        op: ">",
        right: { kind: "Constant", value: 0, dataType: DataTypes.number },
        resultType: { kind: "boolean" },
      };
      expect(isLogicalPredicate(comparison)).to.be.true;
    });

    it("should return false for non-boolean expressions", () => {
      const constant: LogicalExpr = {
        kind: "Constant",
        value: 42,
        dataType: DataTypes.number,
      };
      expect(isLogicalPredicate(constant)).to.be.false;
    });
  });

  describe("computeGrainId", () => {
    it("should compute canonical grain ID from dimensions", () => {
      const dims: LogicalAttributeRef[] = [
        {
          kind: "AttributeRef",
          attributeId: "storeName",
          logicalName: "storeName",
          physicalTable: "dim_store",
          physicalColumn: "name",
          dataType: DataTypes.string,
          sourceKind: "dimension",
        },
        {
          kind: "AttributeRef",
          attributeId: "month",
          logicalName: "month",
          physicalTable: "fact_sales",
          physicalColumn: "month",
          dataType: DataTypes.number,
          sourceKind: "fact",
        },
      ];

      // Should be sorted alphabetically
      expect(computeGrainId(dims)).to.equal("month,storeName");
    });

    it("should return empty string for no dimensions", () => {
      expect(computeGrainId([])).to.equal("");
    });
  });
});

// ---------------------------------------------------------------------------
// EXPRESSION BUILDER TESTS
// ---------------------------------------------------------------------------

describe("LogicalExprBuilder", () => {
  describe("constant", () => {
    it("should create number constant with inferred type", () => {
      const expr = LogicalExprBuilder.constant(42);
      expect(expr.kind).to.equal("Constant");
      expect(expr.value).to.equal(42);
      expect(expr.dataType.kind).to.equal("number");
    });

    it("should create string constant with inferred type", () => {
      const expr = LogicalExprBuilder.constant("hello");
      expect(expr.kind).to.equal("Constant");
      expect(expr.value).to.equal("hello");
      expect(expr.dataType.kind).to.equal("string");
    });

    it("should create boolean constant with inferred type", () => {
      const expr = LogicalExprBuilder.constant(true);
      expect(expr.kind).to.equal("Constant");
      expect(expr.value).to.equal(true);
      expect(expr.dataType.kind).to.equal("boolean");
    });

    it("should accept explicit type override", () => {
      const expr = LogicalExprBuilder.constant(42, DataTypes.integer);
      expect(expr.dataType).to.deep.equal(DataTypes.integer);
    });
  });

  describe("arithmetic operations", () => {
    const left = LogicalExprBuilder.constant(10);
    const right = LogicalExprBuilder.constant(5);

    it("should create add expression", () => {
      const expr = LogicalExprBuilder.add(left, right);
      expect(expr.kind).to.equal("ScalarOp");
      expect(expr.op).to.equal("+");
    });

    it("should create sub expression", () => {
      const expr = LogicalExprBuilder.sub(left, right);
      expect(expr.op).to.equal("-");
    });

    it("should create mul expression", () => {
      const expr = LogicalExprBuilder.mul(left, right);
      expect(expr.op).to.equal("*");
    });

    it("should create div expression with decimal result type", () => {
      const expr = LogicalExprBuilder.div(left, right);
      expect(expr.op).to.equal("/");
      expect(expr.resultType).to.deep.equal(DataTypes.decimal);
    });
  });

  describe("comparison", () => {
    it("should create comparison with boolean result", () => {
      const left = LogicalExprBuilder.constant(10);
      const right = LogicalExprBuilder.constant(5);
      const expr = LogicalExprBuilder.comparison(left, ">", right);

      expect(expr.kind).to.equal("Comparison");
      expect(expr.op).to.equal(">");
      expect(expr.resultType.kind).to.equal("boolean");
    });
  });

  describe("logical operations", () => {
    const pred1: any = LogicalExprBuilder.comparison(
      LogicalExprBuilder.constant(1),
      ">",
      LogicalExprBuilder.constant(0)
    );
    const pred2: any = LogicalExprBuilder.comparison(
      LogicalExprBuilder.constant(2),
      "<",
      LogicalExprBuilder.constant(3)
    );

    it("should create AND expression", () => {
      const expr = LogicalExprBuilder.and(pred1, pred2);
      expect(expr.kind).to.equal("LogicalOp");
      expect(expr.op).to.equal("and");
      expect(expr.operands).to.have.lengthOf(2);
    });

    it("should create OR expression", () => {
      const expr = LogicalExprBuilder.or(pred1, pred2);
      expect(expr.op).to.equal("or");
    });

    it("should create NOT expression", () => {
      const expr = LogicalExprBuilder.not(pred1);
      expect(expr.op).to.equal("not");
      expect(expr.operands).to.have.lengthOf(1);
    });

    it("should throw for AND with less than 2 operands", () => {
      expect(() => LogicalExprBuilder.and(pred1)).to.throw(
        "AND requires at least 2 operands"
      );
    });

    it("should throw for OR with less than 2 operands", () => {
      expect(() => LogicalExprBuilder.or(pred1)).to.throw(
        "OR requires at least 2 operands"
      );
    });
  });
});

// ---------------------------------------------------------------------------
// SYNTAX TO LOGICAL TRANSFORMATION TESTS
// ---------------------------------------------------------------------------

describe("syntaxToLogical", () => {
  describe("Literal transformation", () => {
    it("should transform numeric literal to LogicalConstant", () => {
      const syntax: MetricExpr = Expr.lit(42);
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Constant");
      const constant = logical as LogicalConstant;
      expect(constant.value).to.equal(42);
      expect(constant.dataType.kind).to.equal("number");
    });

    it("should handle negative literals", () => {
      const syntax: MetricExpr = Expr.lit(-100);
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Constant");
      expect((logical as LogicalConstant).value).to.equal(-100);
    });

    it("should handle decimal literals", () => {
      const syntax: MetricExpr = Expr.lit(3.14);
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Constant");
      expect((logical as LogicalConstant).value).to.equal(3.14);
    });
  });

  describe("AttrRef transformation", () => {
    it("should transform attribute reference to LogicalAttributeRef", () => {
      const syntax: MetricExpr = Expr.attr("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("AttributeRef");
      const attrRef = logical as LogicalAttributeRef;
      expect(attrRef.attributeId).to.equal("salesAmount");
      expect(attrRef.logicalName).to.equal("salesAmount");
      expect(attrRef.physicalTable).to.equal("fact_sales");
      expect(attrRef.physicalColumn).to.equal("salesAmount");
      expect(attrRef.sourceKind).to.equal("fact");
    });

    it("should resolve dimension attribute", () => {
      const syntax: MetricExpr = Expr.attr("storeName");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("AttributeRef");
      const attrRef = logical as LogicalAttributeRef;
      expect(attrRef.physicalTable).to.equal("dim_store");
      expect(attrRef.physicalColumn).to.equal("name");
      expect(attrRef.sourceKind).to.equal("dimension");
    });

    it("should handle attribute with custom column name", () => {
      const syntax: MetricExpr = Expr.attr("storeId");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("AttributeRef");
      const attrRef = logical as LogicalAttributeRef;
      expect(attrRef.attributeId).to.equal("storeId");
      expect(attrRef.physicalColumn).to.equal("id");
    });

    it("should throw for unknown attribute", () => {
      const syntax: MetricExpr = Expr.attr("unknownAttr");

      expect(() => syntaxToLogical(syntax, model, "fact_sales")).to.throw(
        TransformationError,
        'Unknown attribute: "unknownAttr"'
      );
    });

    it("should handle count(*) special case", () => {
      const syntax: MetricExpr = Expr.attr("*");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("AttributeRef");
      const attrRef = logical as LogicalAttributeRef;
      expect(attrRef.attributeId).to.equal("*");
      expect(attrRef.physicalTable).to.equal("*");
    });
  });

  describe("MetricRef transformation", () => {
    it("should transform metric reference to LogicalMetricRef", () => {
      const syntax: MetricExpr = Expr.metric("totalSales");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("MetricRef");
      const metricRef = logical as LogicalMetricRef;
      expect(metricRef.metricName).to.equal("totalSales");
      expect(metricRef.baseFact).to.equal("fact_sales");
      expect(metricRef.resultType.kind).to.equal("number");
    });

    it("should throw for unknown metric", () => {
      const syntax: MetricExpr = Expr.metric("unknownMetric");

      expect(() => syntaxToLogical(syntax, model, "fact_sales")).to.throw(
        TransformationError,
        'Unknown metric: "unknownMetric"'
      );
    });
  });

  describe("BinaryOp transformation", () => {
    it("should transform addition", () => {
      const syntax: MetricExpr = Expr.add(Expr.lit(10), Expr.lit(5));
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("ScalarOp");
      const op = logical as LogicalScalarOp;
      expect(op.op).to.equal("+");
      expect((op.left as LogicalConstant).value).to.equal(10);
      expect((op.right as LogicalConstant).value).to.equal(5);
      expect(op.resultType.kind).to.equal("number");
    });

    it("should transform division with decimal result type", () => {
      const syntax: MetricExpr = Expr.div(Expr.lit(10), Expr.lit(3));
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("ScalarOp");
      const op = logical as LogicalScalarOp;
      expect(op.op).to.equal("/");
      expect(op.resultType).to.deep.equal(DataTypes.decimal);
    });

    it("should transform nested operations", () => {
      // (a + b) * c
      const syntax: MetricExpr = Expr.mul(
        Expr.add(Expr.lit(1), Expr.lit(2)),
        Expr.lit(3)
      );
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("ScalarOp");
      const mul = logical as LogicalScalarOp;
      expect(mul.op).to.equal("*");
      expect(mul.left.kind).to.equal("ScalarOp");
      expect((mul.left as LogicalScalarOp).op).to.equal("+");
    });
  });

  describe("Call (aggregate) transformation", () => {
    it("should transform sum() to LogicalAggregate", () => {
      const syntax: MetricExpr = Expr.sum("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Aggregate");
      const agg = logical as LogicalAggregate;
      expect(agg.op).to.equal("sum");
      expect(agg.distinct).to.be.false;
      expect(agg.input.kind).to.equal("AttributeRef");
    });

    it("should transform avg() to LogicalAggregate", () => {
      const syntax: MetricExpr = Expr.avg("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Aggregate");
      expect((logical as LogicalAggregate).op).to.equal("avg");
    });

    it("should transform count() to LogicalAggregate", () => {
      const syntax: MetricExpr = Expr.count("orderId");
      const logical = syntaxToLogical(syntax, model, "fact_orders");

      expect(logical.kind).to.equal("Aggregate");
      expect((logical as LogicalAggregate).op).to.equal("count");
    });

    it("should transform min() to LogicalAggregate", () => {
      const syntax: MetricExpr = Expr.min("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Aggregate");
      expect((logical as LogicalAggregate).op).to.equal("min");
    });

    it("should transform max() to LogicalAggregate", () => {
      const syntax: MetricExpr = Expr.max("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("Aggregate");
      expect((logical as LogicalAggregate).op).to.equal("max");
    });

    it("should throw for unknown function", () => {
      const syntax: MetricExpr = Expr.call("unknownFunc", Expr.lit(1));

      expect(() => syntaxToLogical(syntax, model, "fact_sales")).to.throw(
        TransformationError,
        'Unknown function: "unknownFunc"'
      );
    });
  });

  describe("Complex expression transformation", () => {
    it("should transform metric / metric expression", () => {
      // avg_ticket = totalSales / orderCount
      const syntax: MetricExpr = Expr.div(
        Expr.metric("totalSales"),
        Expr.metric("orderCount")
      );
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("ScalarOp");
      const op = logical as LogicalScalarOp;
      expect(op.op).to.equal("/");
      expect(op.left.kind).to.equal("MetricRef");
      expect(op.right.kind).to.equal("MetricRef");
    });

    it("should transform aggregate + literal expression", () => {
      // sum(salesAmount) + 100
      const syntax: MetricExpr = Expr.add(
        Expr.sum("salesAmount"),
        Expr.lit(100)
      );
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      expect(logical.kind).to.equal("ScalarOp");
      const op = logical as LogicalScalarOp;
      expect(op.left.kind).to.equal("Aggregate");
      expect(op.right.kind).to.equal("Constant");
    });
  });

  describe("Window node handling", () => {
    it("should throw in strict mode for Window nodes", () => {
      const syntax: MetricExpr = {
        kind: "Window",
        base: Expr.sum("salesAmount"),
        partitionBy: ["storeId"],
        orderBy: "month",
        frame: { kind: "rolling", count: 3 },
        aggregate: "sum",
      };

      expect(() =>
        syntaxToLogical(syntax, model, "fact_sales", { strictMode: true })
      ).to.throw(TransformationError, /Window expressions should be handled/);
    });

    it("should return placeholder in non-strict mode for Window nodes", () => {
      const syntax: MetricExpr = {
        kind: "Window",
        base: Expr.sum("salesAmount"),
        partitionBy: ["storeId"],
        orderBy: "month",
        frame: { kind: "rolling", count: 3 },
        aggregate: "sum",
      };

      const logical = syntaxToLogical(syntax, model, "fact_sales", {
        strictMode: false,
      });
      expect(logical.kind).to.equal("ScalarFunction");
    });
  });

  describe("Transform node handling", () => {
    it("should throw in strict mode for Transform nodes", () => {
      const syntax: MetricExpr = {
        kind: "Transform",
        transformId: "someTransform",
        transformKind: "table",
        base: Expr.sum("salesAmount"),
      };

      expect(() =>
        syntaxToLogical(syntax, model, "fact_sales", { strictMode: true })
      ).to.throw(TransformationError, /Transform expressions should be handled/);
    });
  });
});

// ---------------------------------------------------------------------------
// UTILITY FUNCTION TESTS
// ---------------------------------------------------------------------------

describe("Utility Functions", () => {
  describe("resolveAttribute", () => {
    it("should resolve fact attribute", () => {
      const resolved = resolveAttribute("salesAmount", model, "fact_sales");

      expect(resolved.attributeId).to.equal("salesAmount");
      expect(resolved.physicalTable).to.equal("fact_sales");
      expect(resolved.physicalColumn).to.equal("salesAmount");
      expect(resolved.sourceKind).to.equal("fact");
    });

    it("should resolve dimension attribute", () => {
      const resolved = resolveAttribute("storeName", model, "fact_sales");

      expect(resolved.physicalTable).to.equal("dim_store");
      expect(resolved.physicalColumn).to.equal("name");
      expect(resolved.sourceKind).to.equal("dimension");
    });
  });

  describe("collectAttributeRefs", () => {
    it("should collect all attribute references", () => {
      const expr: LogicalExpr = {
        kind: "ScalarOp",
        op: "+",
        left: {
          kind: "AttributeRef",
          attributeId: "a",
          logicalName: "a",
          physicalTable: "t1",
          physicalColumn: "a",
          dataType: DataTypes.number,
          sourceKind: "fact",
        },
        right: {
          kind: "AttributeRef",
          attributeId: "b",
          logicalName: "b",
          physicalTable: "t2",
          physicalColumn: "b",
          dataType: DataTypes.number,
          sourceKind: "dimension",
        },
        resultType: DataTypes.number,
      };

      const refs = collectAttributeRefs(expr);
      expect(refs).to.have.lengthOf(2);
      expect(refs.map((r) => r.attributeId)).to.include.members(["a", "b"]);
    });

    it("should collect attributes from nested aggregates", () => {
      const syntax = Expr.sum("salesAmount");
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      const refs = collectAttributeRefs(logical);
      expect(refs).to.have.lengthOf(1);
      expect(refs[0].attributeId).to.equal("salesAmount");
    });
  });

  describe("collectMetricRefs", () => {
    it("should collect all metric references", () => {
      const syntax = Expr.div(
        Expr.metric("totalSales"),
        Expr.metric("orderCount")
      );
      const logical = syntaxToLogical(syntax, model, "fact_sales");

      const refs = collectMetricRefs(logical);
      expect(refs).to.have.lengthOf(2);
      expect(refs.map((r) => r.metricName)).to.include.members([
        "totalSales",
        "orderCount",
      ]);
    });
  });
});
