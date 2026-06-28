import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { fetchDetailTable } from "@/api/sales-report";
import { ReportTableView } from "@/components/tables/ReportTableView";
import { useSettings } from "@/features/settings/SettingsContext";
import type { DetailSelection, ReportTable } from "@/types/sales-report";
import { filterTableColumns } from "@/utils/filter-table-columns";
import { getSlotStyles, slotLabel } from "./selection-slots";
import { selectionLabel } from "./selection-utils";

interface DetailStats {
  rowCount: number;
  volumeKg: number;
  totalPrice: number;
  unpaidCount: number;
}

interface DetailPanelProps {
  selection: DetailSelection;
  slotIndex: number;
  monthNames: string[];
  onRemove: () => void;
}

function computeStats(table: ReportTable): DetailStats {
  const volumeIdx = table.columns.indexOf("объем кг");
  const priceIdx = table.columns.indexOf("Цена");
  const paidIdx = table.columns.indexOf("Оплачено");

  let volumeKg = 0;
  let totalPrice = 0;
  let unpaidCount = 0;

  for (const row of table.rows) {
    if (volumeIdx >= 0) {
      const value = row[volumeIdx];
      if (typeof value === "number") volumeKg += value;
    }
    if (priceIdx >= 0) {
      const value = row[priceIdx];
      if (typeof value === "number") totalPrice += value;
    }
    if (paidIdx >= 0 && row[paidIdx] === "Нет") {
      unpaidCount += 1;
    }
  }

  return {
    rowCount: table.rows.length,
    volumeKg,
    totalPrice,
    unpaidCount,
  };
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(value);
}

export function DetailPanel({ selection, slotIndex, monthNames, onRemove }: DetailPanelProps) {
  const { settings } = useSettings();
  const styles = getSlotStyles(slotIndex);
  const label = slotLabel(slotIndex);
  const paymentFilter = settings.paymentFilter;
  const productItemIds = settings.productItemIds;

  const query = useQuery({
    queryKey: ["sales-detail", selection, paymentFilter, productItemIds],
    queryFn: () => fetchDetailTable(selection, productItemIds, paymentFilter),
    staleTime: settings.refreshIntervalSec * 1000 || 60_000,
    placeholderData: keepPreviousData,
  });

  const displayTable = useMemo(
    () =>
      query.data
        ? filterTableColumns(query.data, settings.columnOrder, settings.visibleColumns)
        : null,
    [query.data, settings.columnOrder, settings.visibleColumns],
  );

  const stats = query.data ? computeStats(query.data) : null;

  return (
    <div className={`rounded-xl border-2 ${styles.border} bg-white shadow-sm`}>
      <div className={`flex flex-wrap items-start justify-between gap-3 border-b ${styles.border} ${styles.header} px-4 py-3`}>
        <div className="flex items-start gap-3">
          <span
            className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs font-bold text-white ${styles.badge}`}
          >
            {label}
          </span>
          <div>
            <p className="text-sm font-semibold text-slate-800">
              {selectionLabel(selection, monthNames)}
            </p>
            {stats ? (
              <p className="mt-1 text-xs text-slate-600">
                {stats.rowCount} поз. · {formatNumber(stats.volumeKg)} кг ·{" "}
                {formatNumber(stats.totalPrice)} ₽
                {stats.unpaidCount > 0 ? ` · неопл.: ${stats.unpaidCount}` : ""}
              </p>
            ) : null}
          </div>
        </div>
        <button
          type="button"
          onClick={onRemove}
          className="rounded-lg border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-white"
        >
          Убрать
        </button>
      </div>

      {query.isLoading && !query.data ? (
        <p className="px-4 py-6 text-sm text-slate-600">Загрузка позиций...</p>
      ) : null}

      {query.isError ? (
        <div className="m-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {(query.error as Error).message}
        </div>
      ) : null}

      {displayTable ? (
        <div className="[&>section]:rounded-none [&>section]:border-0 [&>section]:shadow-none">
          <ReportTableView
            table={displayTable}
            compactTable={settings.compactTable}
            highlightUnpaidRows={settings.highlightUnpaidRows}
            showRowNumbers={settings.showRowNumbers}
            embedded
          />
        </div>
      ) : null}
    </div>
  );
}

export function computeStatsForTable(table: ReportTable): DetailStats {
  return computeStats(table);
}
