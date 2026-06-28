import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { fetchSummaryMatrix } from "@/api/sales-report";
import { REFRESH_INTERVAL_OPTIONS } from "@/constants/detail-columns";
import { useSettings } from "@/features/settings/SettingsContext";
import { SummaryMatrix } from "@/components/tables/SummaryMatrix";
import type { DetailSelection } from "@/types/sales-report";
import { DetailComparisonSection } from "./DetailComparisonSection";
import { selectionKey, toggleSelection, trimSelections } from "./selection-utils";

const MONTH_NAMES = [
  "Январь",
  "Февраль",
  "Март",
  "Апрель",
  "Май",
  "Июнь",
  "Июль",
  "Август",
  "Сентябрь",
  "Октябрь",
  "Ноябрь",
  "Декабрь",
];

export function SalesReportPage() {
  const queryClient = useQueryClient();
  const { settings } = useSettings();
  const [year, setYear] = useState(settings.defaultYear);
  const [selections, setSelections] = useState<DetailSelection[]>([]);

  const maxSelections = settings.maxComparisonSelections;
  const refreshMs = settings.refreshIntervalSec * 1000;
  const productItemIds = settings.productItemIds;

  useEffect(() => {
    setSelections((prev) => trimSelections(prev, maxSelections));
  }, [maxSelections]);

  const summaryQuery = useQuery({
    queryKey: ["sales-summary", year, productItemIds],
    queryFn: () => fetchSummaryMatrix(year, productItemIds),
    staleTime: refreshMs || Number.POSITIVE_INFINITY,
    refetchInterval: refreshMs || false,
    refetchIntervalInBackground: true,
    placeholderData: keepPreviousData,
  });

  const refreshLabel =
    REFRESH_INTERVAL_OPTIONS.find((option) => option.value === settings.refreshIntervalSec)?.label ??
    `${settings.refreshIntervalSec} сек`;

  const handleManualRefresh = () => {
    void queryClient.fetchQuery({
      queryKey: ["sales-summary", year, productItemIds],
      queryFn: () => fetchSummaryMatrix(year, productItemIds, true),
    });
  };

  const handleToggleSelect = (selection: DetailSelection) => {
    setSelections((prev) => toggleSelection(prev, selection, maxSelections));
  };

  const handleRemoveSelection = (selection: DetailSelection) => {
    setSelections((prev) => prev.filter((item) => selectionKey(item) !== selectionKey(selection)));
  };

  return (
    <div className="space-y-6">
      <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-end gap-4">
          <label className="flex flex-col gap-1 text-sm">
            <span className="font-medium text-slate-700">Год</span>
            <input
              type="number"
              min={2000}
              max={2100}
              value={year}
              onChange={(e) => {
                setYear(Number(e.target.value));
                setSelections([]);
              }}
              className="w-28 rounded-lg border border-slate-300 px-3 py-2"
            />
          </label>

          <button
            type="button"
            onClick={handleManualRefresh}
            disabled={summaryQuery.isFetching && !summaryQuery.data}
            className="rounded-lg bg-report-header px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-60"
          >
            {summaryQuery.isFetching ? "Обновление..." : "Обновить сводку"}
          </button>
        </div>
        <p className="mt-3 text-xs text-slate-500">
          Клик по ячейкам — сравнение до {maxSelections} поз. Автообновление:{" "}
          {refreshLabel.toLowerCase()}.
        </p>
      </section>

      {summaryQuery.isLoading && !summaryQuery.data ? (
        <p className="text-sm text-slate-600">Загрузка сводки из Pyrus...</p>
      ) : null}

      {summaryQuery.isError ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {(summaryQuery.error as Error).message}
        </div>
      ) : null}

      {summaryQuery.data ? (
        <>
          <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            <span>
              Сводка: {new Date(summaryQuery.data.generated_at).toLocaleString("ru-RU")}
            </span>
            {summaryQuery.isFetching ? (
              <span className="rounded-full bg-blue-100 px-2 py-0.5 text-blue-700">
                обновление...
              </span>
            ) : settings.refreshIntervalSec > 0 ? (
              <span className="rounded-full bg-green-100 px-2 py-0.5 text-green-700">
                авто {refreshLabel.toLowerCase()}
              </span>
            ) : (
              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-slate-600">
                авто выкл
              </span>
            )}
            {selections.length > 0 ? (
              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-slate-700">
                выбрано: {selections.length}/{maxSelections}
              </span>
            ) : null}
          </div>
          <SummaryMatrix
            data={summaryQuery.data}
            selections={selections}
            onToggleSelect={handleToggleSelect}
          />
        </>
      ) : null}

      {selections.length > 0 ? (
        <DetailComparisonSection
          selections={selections}
          monthNames={MONTH_NAMES}
          onRemove={handleRemoveSelection}
          onClear={() => setSelections([])}
        />
      ) : null}
    </div>
  );
}
