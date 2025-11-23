# Semantic Engine Playground User Manual

The Semantic Engine Playground is an interactive browser-based IDE for exploring and debugging the semantic metrics engine. This manual covers all features and workflows.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Interface Overview](#interface-overview)
3. [Working with Data](#working-with-data)
4. [Defining the Schema](#defining-the-schema)
5. [Creating Metrics](#creating-metrics)
6. [Writing Queries](#writing-queries)
7. [Viewing Results](#viewing-results)
8. [Import and Export](#import-and-export)
9. [DSL Reference](#dsl-reference)
10. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Accessing the Playground

The playground is a single-page application that runs entirely in your browser. No backend or installation is required.

- **Local Development**: Run `npm run dev` in the `playground/` directory
- **GitHub Pages**: Access via `https://<username>.github.io/MetricForge/`

### Quick Start with Sample Data

1. Open the playground
2. Click **"Load Sample Data"** on the welcome screen
3. The playground will populate with:
   - Three tables: `sales`, `products`, `regions`
   - A complete schema with facts, dimensions, attributes, and joins
   - Sample metrics: `total_revenue`, `total_quantity`, `avg_order_value`
   - A sample query: `revenue_by_region`
4. Click on a query in the sidebar and press **Run Query** to see results

---

## Interface Overview

The playground uses a three-pane IDE-style layout:

```
┌─────────────────────────────────────────────────────────────────┐
│                         HEADER                                   │
│  Semantic Engine Playground          [Import] [Export] [Reset]  │
├──────────────┬──────────────────────────────┬───────────────────┤
│   SIDEBAR    │         WORKSPACE            │   RIGHT PANEL     │
│              │                              │                   │
│ ▼ DATA (3)   │  ┌─────────────────────────┐ │ [Preview] [AST]   │
│   sales      │  │                         │ │ [Errors] [Results]│
│   products   │  │    Monaco Editor        │ │                   │
│   regions    │  │    or                   │ │  Context-aware    │
│              │  │    Form Editor          │ │  information      │
│ ▼ SCHEMA     │  │    or                   │ │  panel            │
│   Editor     │  │    Data Table           │ │                   │
│              │  │                         │ │                   │
│ ▼ METRICS    │  └─────────────────────────┘ │                   │
│   + Add      │                              │                   │
│              │                              │                   │
│ ▼ QUERIES    │                              │                   │
│   + Add      │                              │                   │
└──────────────┴──────────────────────────────┴───────────────────┘
```

### Header Bar

- **Import**: Load a previously exported workspace
- **Export**: Save the entire workspace (data, schema, metrics, queries) as JSON
- **Reset**: Clear all data and start fresh

### Left Sidebar

- **DATA**: Lists all loaded tables
- **SCHEMA**: Access the schema editor
- **METRICS**: Lists defined metrics (click + to add new)
- **QUERIES**: Lists defined queries (click + to add new)

### Center Workspace

The main editing area that displays:
- Data tables with column types
- Schema editor forms
- Monaco-based DSL editors for metrics and queries

### Right Panel

Context-sensitive panel with four tabs:
- **Preview**: Shows metadata about the selected item
- **AST**: Displays the parsed Abstract Syntax Tree (for metrics/queries)
- **Errors**: Lists all validation errors across the workspace
- **Results**: Shows query execution results

---

## Working with Data

### Uploading JSON Data

#### Method 1: File Upload
1. On the welcome screen, click the drop zone or drag a `.json` file
2. The file should contain either:
   - An array of objects: `[{"id": 1, "name": "..."}]`
   - An object with array properties: `{"table1": [...], "table2": [...]}`

#### Method 2: Paste JSON
1. Click **"Paste JSON Data"** on the welcome screen
2. Enter a table name
3. Paste your JSON array
4. Click **Add Table**

### Viewing Data

Click on a table name in the sidebar to view:
- Column names and inferred types
- First 100 rows (scrollable)
- Row count

### Column Types

The playground automatically infers column types:
- `string` - Text values
- `number` - Numeric values
- `boolean` - True/false values
- `date` - ISO date strings (YYYY-MM-DD)
- `unknown` - Mixed or null values

### Removing Tables

Click the **×** button next to a table name in the sidebar.

---

## Defining the Schema

The schema defines the semantic layer that maps physical tables to logical concepts.

### Opening the Schema Editor

Click **"Schema Editor"** under the SCHEMA section in the sidebar.

### Schema Components

#### Facts
Facts are your transaction or event tables (e.g., sales, orders, clicks).

1. Click the **Facts** tab
2. Click **+ Add**
3. Enter a logical name (e.g., `sales`)
4. Select the physical table
5. Click **Add**

#### Dimensions
Dimensions are lookup or reference tables (e.g., products, customers, dates).

1. Click the **Dimensions** tab
2. Click **+ Add**
3. Enter a logical name
4. Select the physical table
5. Click **Add**

#### Attributes
Attributes map logical names to physical columns. This abstraction layer allows you to:
- Use meaningful names in queries
- Change physical columns without breaking queries

1. Click the **Attributes** tab
2. Click **+ Add**
3. Enter a logical name (e.g., `product_name`)
4. Select the source table
5. Select the physical column
6. Click **Add**

#### Joins
Joins define relationships between facts and dimensions via foreign key relationships.

1. Click the **Joins** tab
2. Click **+ Add**
3. Select the fact table
4. Select the fact's foreign key column
5. Select the dimension table
6. Select the dimension's primary key column
7. Click **Add**

### Schema Preview

Switch to the **Preview** tab in the right panel to see the generated schema as JSON.

---

## Creating Metrics

Metrics define reusable calculations that can be computed at any grain.

### Adding a New Metric

1. Click **+** next to METRICS in the sidebar
2. Enter a metric name (e.g., `total_revenue`)
3. Press Enter or click **Add**

### Metric DSL Syntax

```
metric <name> on <fact> = <expression>
```

#### Examples

```
// Simple aggregate
metric total_revenue on sales = sum(amount)

// Count
metric order_count on sales = count(*)

// Average
metric avg_order_value on sales = avg(amount)

// Derived metric (uses other metrics)
metric revenue_per_order on sales = total_revenue / order_count

// Arithmetic expressions
metric profit_margin on sales = (sum(revenue) - sum(cost)) / sum(revenue)
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `sum(column)` | Sum of values |
| `avg(column)` | Average of values |
| `min(column)` | Minimum value |
| `max(column)` | Maximum value |
| `count(*)` | Count of rows |

### Editor Features

- **Syntax Highlighting**: Keywords, functions, and identifiers are color-coded
- **Autocomplete**: Press Ctrl+Space to see suggestions for:
  - Keywords: `metric`, `on`, `sum`, `avg`, etc.
  - Attributes from your schema
  - Other metrics
- **Real-time Validation**: Errors appear as red underlines
- **Status Badge**: Shows "Valid" (green) or "Invalid" (red)

### Viewing the Parsed AST

Select the metric and click the **AST** tab in the right panel to see how your metric was parsed.

---

## Writing Queries

Queries combine dimensions and metrics to retrieve aggregated results.

### Adding a New Query

1. Click **+** next to QUERIES in the sidebar
2. Enter a query name
3. Press Enter or click **Add**

### Query DSL Syntax

```
query <name> {
  dimensions: <attr1>, <attr2>, ...
  metrics: <metric1>, <metric2>, ...
  where: <filter_expression>
  having: <metric_filter>
}
```

### Examples

#### Basic Query
```
query revenue_by_region {
  dimensions: region_name
  metrics: total_revenue, total_quantity
}
```

#### With Filters
```
query large_orders {
  dimensions: product_name, region_name
  metrics: total_revenue
  where: amount > 1000
  having: total_revenue > 5000
}
```

#### Multiple Dimensions
```
query sales_analysis {
  dimensions: category, region_name, date
  metrics: total_revenue, order_count, avg_order_value
}
```

### Query Clauses

| Clause | Required | Description |
|--------|----------|-------------|
| `dimensions` | Yes | Attributes to group by (defines the grain) |
| `metrics` | Yes | Metrics to compute |
| `where` | No | Filter rows before aggregation |
| `having` | No | Filter groups after aggregation |

### Filter Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `amount > 100` |
| `>=` | Greater or equal | `quantity >= 5` |
| `<` | Less than | `price < 50` |
| `<=` | Less or equal | `discount <= 0.1` |
| `==` | Equals | `status == "active"` |
| `!=` | Not equals | `region != "unknown"` |
| `and` | Logical AND | `amount > 100 and quantity > 5` |
| `or` | Logical OR | `region == "North" or region == "South"` |

### Running Queries

1. Open a query in the editor
2. Click **Run Query** in the toolbar
3. View results in the **Results** tab of the right panel

---

## Viewing Results

### Results Panel

After running a query, the **Results** tab shows:
- Query name and execution time
- Row count
- Tabular data with sortable columns
- Null values displayed as "null" in gray

### Data Formats

- Numbers are formatted with thousands separators
- Null/undefined values are clearly marked
- Up to 100 rows displayed (for performance)

### Execution Errors

If a query fails, the error message is displayed in red with details about what went wrong.

---

## Import and Export

### Exporting a Workspace

1. Click **Export** in the header
2. A JSON file downloads containing:
   - All table data
   - Schema definitions
   - Metric DSL text
   - Query DSL text

### Export File Format

```json
{
  "version": "1.0",
  "data": {
    "tableName": [/* rows */]
  },
  "schema": {
    "facts": [],
    "dimensions": [],
    "attributes": [],
    "joins": []
  },
  "metrics": {
    "metricName": "metric DSL text"
  },
  "queries": {
    "queryName": "query DSL text"
  }
}
```

### Importing a Workspace

1. Click **Import** in the header
2. Select a previously exported `.json` file
3. The workspace is fully restored

### Auto-Save

The playground automatically saves to browser LocalStorage. Your work persists across browser sessions.

---

## DSL Reference

### Keywords

| Keyword | Context | Description |
|---------|---------|-------------|
| `metric` | Metric definition | Declares a new metric |
| `on` | Metric definition | Specifies the base fact |
| `query` | Query definition | Declares a new query |
| `dimensions` | Query body | Lists grouping attributes |
| `metrics` | Query body | Lists metrics to compute |
| `where` | Query body | Row-level filter |
| `having` | Query body | Aggregate-level filter |
| `and` | Filters | Logical conjunction |
| `or` | Filters | Logical disjunction |
| `by` | Time intelligence | Anchor attribute for transforms |

### Grammar Summary

```
// Metric Declaration
metric_decl := "metric" IDENT "on" IDENT "=" expr

// Query Declaration
query_decl := "query" IDENT "{" query_line* "}"
query_line := dimensions_line | metrics_line | where_line | having_line

// Expression
expr := term (("+" | "-") term)*
term := factor (("*" | "/") factor)*
factor := NUMBER | IDENT | function_call | "(" expr ")"
function_call := IDENT "(" arg_list ")"
```

---

## Troubleshooting

### Common Errors

#### "Unknown attribute: xyz"
- Ensure the attribute is defined in the schema
- Check spelling and case sensitivity

#### "Unknown metric: abc"
- Ensure the metric is defined before it's referenced
- Check for circular dependencies

#### "Parse error"
- Check DSL syntax
- Ensure brackets and parentheses are balanced
- Verify keyword spelling

#### "No tables available"
- Load data before running queries
- Check that the base fact table exists

### Performance Tips

- Keep datasets under 10MB for best performance
- Limit dimensions to avoid explosion of group combinations
- Use `where` filters to reduce data before aggregation

### Resetting the Workspace

If you encounter persistent issues:
1. Click **Reset** in the header
2. Confirm the reset
3. Reload sample data or import your workspace

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl + Space | Trigger autocomplete |
| Ctrl + Z | Undo |
| Ctrl + Shift + Z | Redo |
| Ctrl + / | Toggle comment |
| Ctrl + F | Find |
| Ctrl + H | Find and replace |

---

## Getting Help

- Check the **Errors** panel for validation issues
- Review the **AST** panel to understand how your code was parsed
- Refer to this manual for DSL syntax
- For bugs or feature requests, visit the GitHub repository
