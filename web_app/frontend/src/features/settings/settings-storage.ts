import { DEFAULT_VISIBLE_COLUMNS, DETAIL_COLUMNS } from "@/constants/detail-columns";
import { DEFAULT_PRODUCT_ITEM_IDS, normalizeProductItemIds } from "@/constants/products";
import { DEFAULT_MAX_COMPARISON_SELECTIONS } from "@/features/sales-report/selection-slots";
import type { AppSettings } from "@/types/settings";

const STORAGE_KEY = "sales-report-settings";

export const DEFAULT_SETTINGS: AppSettings = {
  columnOrder: [...DETAIL_COLUMNS],
  visibleColumns: [...DEFAULT_VISIBLE_COLUMNS],
  refreshIntervalSec: 60,
  paymentFilter: "all",
  productItemIds: { ...DEFAULT_PRODUCT_ITEM_IDS },
  defaultYear: 2026,
  compactTable: false,
  highlightUnpaidRows: true,
  showRowNumbers: false,
  maxComparisonSelections: DEFAULT_MAX_COMPARISON_SELECTIONS,
};

function isDetailColumn(value: unknown): value is AppSettings["columnOrder"][number] {
  return typeof value === "string" && DETAIL_COLUMNS.includes(value as AppSettings["columnOrder"][number]);
}

function normalizeColumnList(value: unknown, fallback: AppSettings["columnOrder"]): AppSettings["columnOrder"] {
  if (!Array.isArray(value)) {
    return fallback;
  }

  const normalized = value.filter(isDetailColumn);
  const missing = fallback.filter((column) => !normalized.includes(column));
  return [...normalized, ...missing];
}

function normalizePaymentFilter(
  parsed: Partial<AppSettings> & { defaultUnpaidOnly?: boolean },
): AppSettings["paymentFilter"] {
  if (parsed.paymentFilter === "all" || parsed.paymentFilter === "unpaid" || parsed.paymentFilter === "paid") {
    return parsed.paymentFilter;
  }
  if (parsed.defaultUnpaidOnly === true) {
    return "unpaid";
  }
  return DEFAULT_SETTINGS.paymentFilter;
}

export function loadSettings(): AppSettings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return DEFAULT_SETTINGS;
    }

    const parsed = JSON.parse(raw) as Partial<AppSettings> & { defaultUnpaidOnly?: boolean };
    const columnOrder = normalizeColumnList(parsed.columnOrder, DEFAULT_SETTINGS.columnOrder);
    const visibleColumns = normalizeColumnList(parsed.visibleColumns, DEFAULT_SETTINGS.visibleColumns).filter(
      (column) => columnOrder.includes(column),
    );

    return {
      columnOrder,
      visibleColumns: visibleColumns.length > 0 ? visibleColumns : [...columnOrder],
      refreshIntervalSec:
        typeof parsed.refreshIntervalSec === "number" ? parsed.refreshIntervalSec : DEFAULT_SETTINGS.refreshIntervalSec,
      paymentFilter: normalizePaymentFilter(parsed),
      productItemIds: normalizeProductItemIds(parsed.productItemIds),
      defaultYear:
        typeof parsed.defaultYear === "number" ? parsed.defaultYear : DEFAULT_SETTINGS.defaultYear,
      compactTable:
        typeof parsed.compactTable === "boolean" ? parsed.compactTable : DEFAULT_SETTINGS.compactTable,
      highlightUnpaidRows:
        typeof parsed.highlightUnpaidRows === "boolean"
          ? parsed.highlightUnpaidRows
          : DEFAULT_SETTINGS.highlightUnpaidRows,
      showRowNumbers:
        typeof parsed.showRowNumbers === "boolean"
          ? parsed.showRowNumbers
          : DEFAULT_SETTINGS.showRowNumbers,
      maxComparisonSelections:
        typeof parsed.maxComparisonSelections === "number"
          ? Math.min(10, Math.max(1, parsed.maxComparisonSelections))
          : DEFAULT_SETTINGS.maxComparisonSelections,
    };
  } catch {
    return DEFAULT_SETTINGS;
  }
}

export function saveSettings(settings: AppSettings): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
}
