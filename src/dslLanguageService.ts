export type DslCompletionKind =
  | "keyword"
  | "function"
  | "attribute"
  | "metric"
  | "fact"
  | "dimension";

export interface DslCompletionItem {
  label: string;
  kind: DslCompletionKind;
  detail?: string;
}

export interface DslCompletionContext {
  attributes?: string[];
  metrics?: string[];
  facts?: string[];
  dimensions?: string[];
  text?: string;
  line?: number;
  column?: number;
}

export const DSL_KEYWORDS = [
  "metric",
  "on",
  "query",
  "dimensions",
  "metrics",
  "where",
  "having",
  "and",
  "or",
  "by",
] as const;

export const DSL_FUNCTIONS = ["sum", "avg", "min", "max", "count", "last_year"] as const;

function pushUnique(
  target: DslCompletionItem[],
  seen: Set<string>,
  item: DslCompletionItem
): void {
  const key = `${item.kind}:${item.label}`;
  if (seen.has(key)) return;
  seen.add(key);
  target.push(item);
}

export function getDslCompletions(
  context: DslCompletionContext = {}
): DslCompletionItem[] {
  const completions: DslCompletionItem[] = [];
  const seen = new Set<string>();

  DSL_KEYWORDS.forEach((keyword) =>
    pushUnique(completions, seen, { label: keyword, kind: "keyword" })
  );

  DSL_FUNCTIONS.forEach((fn) =>
    pushUnique(completions, seen, { label: fn, kind: "function", detail: `${fn}()` })
  );

  (context.attributes ?? []).forEach((attr) =>
    pushUnique(completions, seen, { label: attr, kind: "attribute", detail: "Attribute" })
  );

  (context.metrics ?? []).forEach((metric) =>
    pushUnique(completions, seen, { label: metric, kind: "metric", detail: "Metric" })
  );

  (context.facts ?? []).forEach((fact) =>
    pushUnique(completions, seen, { label: fact, kind: "fact", detail: "Fact" })
  );

  (context.dimensions ?? []).forEach((dimension) =>
    pushUnique(completions, seen, { label: dimension, kind: "dimension", detail: "Dimension" })
  );

  return completions;
}
