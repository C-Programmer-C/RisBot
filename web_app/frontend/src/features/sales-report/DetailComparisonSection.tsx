import { keepPreviousData, useQueries } from "@tanstack/react-query";
import { fetchDetailTable } from "@/api/sales-report";
import { useSettings } from "@/features/settings/SettingsContext";
import { ComparisonSummary } from "./ComparisonSummary";
import { DetailPanel } from "./DetailPanel";
import { comparisonGridClass } from "./selection-slots";
import type { DetailSelection } from "@/types/sales-report";

interface DetailComparisonSectionProps {
  selections: DetailSelection[];
  monthNames: string[];
  onRemove: (selection: DetailSelection) => void;
  onClear: () => void;
}

export function DetailComparisonSection({
  selections,
  monthNames,
  onRemove,
  onClear,
}: DetailComparisonSectionProps) {
  const { settings } = useSettings();
  const staleTime = settings.refreshIntervalSec * 1000 || 60_000;
  const maxSelections = settings.maxComparisonSelections;
  const paymentFilter = settings.paymentFilter;
  const productItemIds = settings.productItemIds;

  const comparisonQueries = useQueries({
    queries: selections.map((selection) => ({
      queryKey: ["sales-detail", selection, paymentFilter, productItemIds],
      queryFn: () => fetchDetailTable(selection, productItemIds, paymentFilter),
      staleTime,
      placeholderData: keepPreviousData,
    })),
  });

  const allLoaded =
    selections.length > 0 &&
    comparisonQueries.length === selections.length &&
    comparisonQueries.every((query) => query.data);

  const statusText =
    selections.length >= maxSelections
      ? `Выбрано максимум (${selections.length}/${maxSelections}) — новый клик заменит последнюю`
      : selections.length === 0
        ? "Выбери ячейки в сводке для сравнения"
        : `Выбрано ${selections.length}/${maxSelections} — кликни ещё для сравнения`;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-slate-700">{statusText}</p>
        <button
          type="button"
          onClick={onClear}
          className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-50"
        >
          Сбросить выбор
        </button>
      </div>

      {allLoaded ? (
        <ComparisonSummary
          selections={selections}
          tables={comparisonQueries.map((query) => query.data!)}
          monthNames={monthNames}
        />
      ) : null}

      <div className={`grid gap-4 ${comparisonGridClass(selections.length)}`}>
        {selections.map((selection, index) => (
          <DetailPanel
            key={`${selection.product}-${selection.year}-${selection.month}`}
            selection={selection}
            slotIndex={index}
            monthNames={monthNames}
            onRemove={() => onRemove(selection)}
          />
        ))}
      </div>
    </div>
  );
}
