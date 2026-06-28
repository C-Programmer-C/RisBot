import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { fetchCatalogItems } from "@/api/sales-report";
import {
  DEFAULT_PRODUCT_ITEM_IDS,
  REPORT_PRODUCT_NAMES,
  type ReportProductName,
} from "@/constants/products";
import type { AppSettings } from "@/types/settings";

interface ProductMappingSettingsProps {
  value: Record<ReportProductName, number[]>;
  onChange: (next: Record<ReportProductName, number[]>) => void;
  enabled: boolean;
}

function toggleItemIds(current: number[], itemId: number): number[] {
  if (current.includes(itemId)) {
    return current.filter((id) => id !== itemId);
  }
  return [...current, itemId].sort((a, b) => a - b);
}

export function ProductMappingSettings({ value, onChange, enabled }: ProductMappingSettingsProps) {
  const [expandedProduct, setExpandedProduct] = useState<ReportProductName | null>(
    REPORT_PRODUCT_NAMES[0],
  );
  const [search, setSearch] = useState("");

  const catalogQuery = useQuery({
    queryKey: ["pyrus-catalog-items"],
    queryFn: fetchCatalogItems,
    enabled,
    staleTime: 10 * 60_000,
  });

  const filteredItems = useMemo(() => {
    const query = search.trim().toLowerCase();
    const items = catalogQuery.data?.items ?? [];
    if (!query) return items;
    return items.filter(
      (item) =>
        item.name.toLowerCase().includes(query) || String(item.item_id).includes(query),
    );
  }, [catalogQuery.data?.items, search]);

  const updateProduct = (productName: ReportProductName, itemIds: number[]) => {
    onChange({
      ...value,
      [productName]: itemIds,
    });
  };

  if (catalogQuery.isLoading) {
    return <p className="text-sm text-slate-600">Загрузка прайса из Pyrus...</p>;
  }

  if (catalogQuery.isError) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
        {(catalogQuery.error as Error).message}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs text-slate-500">
          Каталог «{catalogQuery.data?.name}» — {catalogQuery.data?.items.length} позиций
        </p>
        <button
          type="button"
          onClick={() => onChange({ ...DEFAULT_PRODUCT_ITEM_IDS })}
          className="rounded-full border border-slate-300 px-3 py-1 text-xs text-slate-700 hover:bg-slate-50"
        >
          Сбросить прайс
        </button>
      </div>

      <input
        type="search"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Поиск по названию или id..."
        className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
      />

      <div className="space-y-2">
        {REPORT_PRODUCT_NAMES.map((productName) => {
          const selectedIds = value[productName] ?? [];
          const isExpanded = expandedProduct === productName;

          return (
            <div key={productName} className="rounded-xl border border-slate-200">
              <button
                type="button"
                onClick={() => setExpandedProduct(isExpanded ? null : productName)}
                className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left hover:bg-slate-50"
              >
                <span className="text-sm font-medium text-slate-800">{productName}</span>
                <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600">
                  {selectedIds.length} выбрано
                </span>
              </button>

              {isExpanded ? (
                <div className="border-t border-slate-200 p-3">
                  <div className="mb-2 flex flex-wrap gap-2">
                    <button
                      type="button"
                      onClick={() =>
                        updateProduct(
                          productName,
                          filteredItems.map((item) => item.item_id),
                        )
                      }
                      className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600"
                    >
                      Все из списка
                    </button>
                    <button
                      type="button"
                      onClick={() => updateProduct(productName, [])}
                      className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600"
                    >
                      Очистить
                    </button>
                  </div>
                  <ul className="max-h-56 space-y-1 overflow-y-auto">
                    {filteredItems.map((item) => {
                      const checked = selectedIds.includes(item.item_id);
                      return (
                        <li key={item.item_id}>
                          <label className="flex items-start gap-2 rounded px-1 py-1 hover:bg-slate-50">
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() =>
                                updateProduct(
                                  productName,
                                  toggleItemIds(selectedIds, item.item_id),
                                )
                              }
                              className="mt-0.5 h-4 w-4 rounded border-slate-300"
                            />
                            <span className="text-xs text-slate-700">
                              <span className="font-mono text-slate-500">{item.item_id}</span>
                              {" — "}
                              {item.name}
                            </span>
                          </label>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export type ProductMappingDraft = AppSettings["productItemIds"];
