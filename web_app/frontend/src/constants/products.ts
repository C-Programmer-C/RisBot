export const REPORT_PRODUCT_NAMES = [
  "Мука рисовая В.С.",
  "Мука рисовая 1 С.",
  "Мука рисовая 2 С.",
  "Дробь",
  "Рис",
  "Крупа",
  "Мука",
  "Кормовые",
] as const;

export type ReportProductName = (typeof REPORT_PRODUCT_NAMES)[number];

export const DEFAULT_PRODUCT_ITEM_IDS: Record<ReportProductName, number[]> = {
  "Мука рисовая В.С.": [165022095],
  "Мука рисовая 1 С.": [165022096],
  "Мука рисовая 2 С.": [165022097],
  Дробь: [165022089, 165022091, 165022092],
  Рис: [176538234, 165022085, 167224299, 175075715, 165022086],
  Крупа: [
    165022099, 170169477, 165022100, 170169479, 165022101, 170169480, 165022102,
    170169481, 165022103, 170169483,
  ],
  Мука: [165022104, 170827124, 170827127, 170827123, 165022105, 170827131],
  Кормовые: [165022107, 165022108, 165022109, 165022110, 165022111, 165022112, 165022113],
};

export function productIdsParam(
  productName: string,
  mappings: Record<string, number[]>,
): string {
  return (mappings[productName] ?? []).join(",");
}

export function normalizeProductItemIds(
  value: unknown,
): Record<ReportProductName, number[]> {
  const result: Record<string, number[]> = { ...DEFAULT_PRODUCT_ITEM_IDS };

  if (!value || typeof value !== "object") {
    return result as Record<ReportProductName, number[]>;
  }

  for (const name of REPORT_PRODUCT_NAMES) {
    const ids = (value as Record<string, unknown>)[name];
    if (Array.isArray(ids)) {
      result[name] = ids.filter((id): id is number => typeof id === "number");
    }
  }

  return result as Record<ReportProductName, number[]>;
}
