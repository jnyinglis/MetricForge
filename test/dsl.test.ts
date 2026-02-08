import { expect } from "chai";
import {
  LogicalAttribute,
  SemanticModel,
  InMemoryDb,
  runSemanticQuery,
} from "../src/semanticEngine";
import {
  compileDslToModel,
  metricDecl,
  parseDslWithDiagnostics,
  parseMetricExprWithDiagnostics,
  parseAll,
  queryDecl,
} from "../src/dsl";
import { getDslCompletions } from "../src/dslLanguageService";

describe("DSL parser and compiler", () => {
  const attributes: Record<string, LogicalAttribute> = {
    tradyrwkcode: { table: "fact_sales" },
    sales_amount: { table: "fact_sales" },
  };

  it("parses metric declarations and expressions", () => {
    const metric = parseAll(
      metricDecl,
      "metric sum_sales on fact_sales = (sum(sales_amount) + 10) / 2"
    );

    expect(metric.name).to.equal("sum_sales");
    expect(metric.baseFact).to.equal("fact_sales");
    expect(metric.expr).to.deep.include({ kind: "BinaryOp", op: "/" });
  });

  it("parses queries with where and having", () => {
    const query = parseAll(
      queryDecl,
      `query weekly_sales {
        dimensions: tradyrwkcode
        metrics: sum_sales, sum_sales_last_year
        where: tradyrwkcode >= 202401
        having: sum_sales > 0
      }`
    );

    expect(query.name).to.equal("weekly_sales");
    expect(query.spec.dimensions).to.deep.equal(["tradyrwkcode"]);
    expect(query.spec.metrics).to.deep.equal(["sum_sales", "sum_sales_last_year"]);
    expect(query.spec.where).to.not.be.undefined;
    expect(query.spec.having).to.be.a("function");
  });

  it("compiles DSL to a model and executes queries", () => {
    const text = `
metric sum_sales on fact_sales = sum(sales_amount)
metric sum_sales_last_year on fact_sales = last_year(sum_sales, by tradyrwkcode)

query weekly_sales_vs_last_year {
  dimensions: tradyrwkcode
  metrics: sum_sales, sum_sales_last_year
  where: tradyrwkcode >= 202501
}
`;

    const db: InMemoryDb = {
      tables: {
        fact_sales: [
          { tradyrwkcode: 202401, sales_amount: 70 },
          { tradyrwkcode: 202402, sales_amount: 20 },
          { tradyrwkcode: 202501, sales_amount: 100 },
          { tradyrwkcode: 202502, sales_amount: 40 },
        ],
        tradyrwk_transform: [
          { tradyrwkcode: 202501, tradyrwkcode_lastyear: 202401 },
          { tradyrwkcode: 202502, tradyrwkcode_lastyear: 202402 },
        ],
      },
    };

    const baseModel: SemanticModel = {
      facts: { fact_sales: { table: "fact_sales" } },
      dimensions: {},
      attributes,
      joins: [],
      metrics: {},
      rowsetTransforms: {
        "last_year:tradyrwkcode": {
          table: "tradyrwk_transform",
          anchorAttr: "tradyrwkcode",
          fromColumn: "tradyrwkcode",
          toColumn: "tradyrwkcode_lastyear",
          factAttr: "tradyrwkcode",
        },
      },
    };

    const { model, queries } = compileDslToModel(text, baseModel);
    const rows = runSemanticQuery({ db, model }, queries.weekly_sales_vs_last_year);

    const wk1 = rows.find((r) => r.tradyrwkcode === 202501);
    const wk2 = rows.find((r) => r.tradyrwkcode === 202502);

    expect(wk1?.sum_sales).to.equal(100);
    expect(wk1?.sum_sales_last_year).to.equal(70);
    expect(wk2?.sum_sales).to.equal(40);
    expect(wk2?.sum_sales_last_year).to.equal(20);
  });

  it("returns diagnostics with line/column for invalid DSL", () => {
    const result = parseDslWithDiagnostics(`
metric total_sales on fact_sales = sum(sales_amount)
query bad_query {
  dimensions: tradyrwkcode
  metrics: total_sales
  where: tradyrwkcode >=
}
`);

    expect(result.errors).to.have.length.greaterThan(0);
    expect(result.errors[0].severity).to.equal("error");
    expect(result.errors[0].line).to.be.greaterThan(0);
    expect(result.errors[0].column).to.be.greaterThan(0);
  });

  it("returns diagnostics for invalid metric expressions", () => {
    const result = parseMetricExprWithDiagnostics("sum(sales_amount");
    expect(result.errors).to.have.length.greaterThan(0);
    expect(result.errors[0].severity).to.equal("error");
    expect(result.expr).to.not.equal(null);
  });

  it("provides DSL completions from core language service", () => {
    const completions = getDslCompletions({
      attributes: ["sales_amount"],
      metrics: ["total_sales"],
      facts: ["fact_sales"],
      dimensions: ["dim_week"],
    });

    expect(completions.some((item) => item.label === "metric" && item.kind === "keyword")).to.equal(true);
    expect(completions.some((item) => item.label === "sum" && item.kind === "function")).to.equal(true);
    expect(completions.some((item) => item.label === "sales_amount" && item.kind === "attribute")).to.equal(true);
    expect(completions.some((item) => item.label === "total_sales" && item.kind === "metric")).to.equal(true);
  });
});
