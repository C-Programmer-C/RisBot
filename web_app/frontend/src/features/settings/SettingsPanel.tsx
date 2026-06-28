import { useEffect, useState } from "react";
import {
  DETAIL_COLUMNS,
  REFRESH_INTERVAL_OPTIONS,
  type DetailColumnId,
} from "@/constants/detail-columns";
import { PAYMENT_FILTER_OPTIONS } from "@/constants/payment-filter";
import type { AppSettings } from "@/types/settings";
import { ProductMappingSettings } from "./ProductMappingSettings";
import { useSettings } from "./SettingsContext";
import { DEFAULT_SETTINGS } from "./settings-storage";

function toggleColumn(visibleColumns: DetailColumnId[], column: DetailColumnId): DetailColumnId[] {
  if (visibleColumns.includes(column)) {
    const next = visibleColumns.filter((item) => item !== column);
    return next.length > 0 ? next : visibleColumns;
  }
  return [...visibleColumns, column];
}

function reorderColumns(
  list: DetailColumnId[],
  from: DetailColumnId,
  to: DetailColumnId,
): DetailColumnId[] {
  const fromIndex = list.indexOf(from);
  const toIndex = list.indexOf(to);
  if (fromIndex < 0 || toIndex < 0 || fromIndex === toIndex) {
    return list;
  }

  const next = [...list];
  const [item] = next.splice(fromIndex, 1);
  next.splice(toIndex, 0, item);
  return next;
}

export function SettingsPanel() {
  const { settings, updateSettings, isPanelOpen, closePanel } = useSettings();
  const [draft, setDraft] = useState<AppSettings>(settings);
  const [draggingColumn, setDraggingColumn] = useState<DetailColumnId | null>(null);
  const [dropTarget, setDropTarget] = useState<DetailColumnId | null>(null);

  useEffect(() => {
    if (isPanelOpen) {
      setDraft(settings);
      setDraggingColumn(null);
      setDropTarget(null);
    }
  }, [isPanelOpen, settings]);

  if (!isPanelOpen) {
    return null;
  }

  const showAllColumns = () => {
    setDraft((prev) => ({
      ...prev,
      columnOrder: [...DETAIL_COLUMNS],
      visibleColumns: [...DETAIL_COLUMNS],
    }));
  };

  const handleSave = () => {
    updateSettings(draft);
    closePanel();
  };

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <button
        type="button"
        aria-label="Закрыть настройки"
        className="absolute inset-0 bg-slate-900/40"
        onClick={closePanel}
      />

      <aside className="relative flex h-full w-full max-w-lg flex-col bg-white shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Настройки</h2>
            <p className="text-xs text-slate-500">Сохраняются в браузере</p>
          </div>
          <button
            type="button"
            onClick={closePanel}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50"
          >
            ✕
          </button>
        </div>

        <div className="flex-1 space-y-6 overflow-y-auto px-5 py-5">
          <section className="space-y-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <p className="text-xs text-slate-500">
                Перетащи столбцы для порядка. Минимум один видимый.
              </p>
              <button
                type="button"
                onClick={showAllColumns}
                className="rounded-full border border-slate-300 px-3 py-1 text-xs text-slate-700 hover:bg-slate-50"
              >
                Все
              </button>
            </div>

            <ul className="space-y-2 rounded-xl border border-slate-200 p-3">
              {draft.columnOrder.map((column) => {
                const isVisible = draft.visibleColumns.includes(column);
                const isDragging = draggingColumn === column;
                const isDropTarget = dropTarget === column && draggingColumn !== column;

                return (
                  <li
                    key={column}
                    onDragOver={(event) => {
                      event.preventDefault();
                      setDropTarget(column);
                    }}
                    onDrop={(event) => {
                      event.preventDefault();
                      if (draggingColumn) {
                        setDraft((prev) => ({
                          ...prev,
                          columnOrder: reorderColumns(prev.columnOrder, draggingColumn, column),
                        }));
                      }
                      setDraggingColumn(null);
                      setDropTarget(null);
                    }}
                    onDragLeave={() => {
                      if (dropTarget === column) {
                        setDropTarget(null);
                      }
                    }}
                    className={`flex items-center gap-2 rounded-lg border px-3 py-2 transition-colors ${
                      isDragging
                        ? "border-blue-300 bg-blue-50 opacity-60"
                        : isDropTarget
                          ? "border-blue-400 bg-blue-50 ring-2 ring-blue-200"
                          : isVisible
                            ? "border-slate-200 bg-white"
                            : "border-slate-100 bg-slate-50"
                    }`}
                  >
                    <span
                      draggable
                      onDragStart={(event) => {
                        setDraggingColumn(column);
                        event.dataTransfer.effectAllowed = "move";
                      }}
                      onDragEnd={() => {
                        setDraggingColumn(null);
                        setDropTarget(null);
                      }}
                      className="cursor-grab select-none text-slate-400 active:cursor-grabbing"
                      title="Перетащить"
                      aria-hidden
                    >
                      ⠿
                    </span>
                    <input
                      type="checkbox"
                      checked={isVisible}
                      onChange={() =>
                        setDraft((prev) => ({
                          ...prev,
                          visibleColumns: toggleColumn(prev.visibleColumns, column),
                        }))
                      }
                      className="h-4 w-4 rounded border-slate-300"
                    />
                    <span className={`flex-1 text-sm ${isVisible ? "text-slate-800" : "text-slate-400"}`}>
                      {column}
                    </span>
                  </li>
                );
              })}
            </ul>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">Привязка к прайсу Pyrus</h3>
            <ProductMappingSettings
              value={draft.productItemIds}
              onChange={(productItemIds) => setDraft((prev) => ({ ...prev, productItemIds }))}
              enabled={isPanelOpen}
            />
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">Сравнение позиций</h3>
            <label className="flex flex-col gap-1 text-sm">
              <span className="font-medium text-slate-700">Максимум выбранных ячеек</span>
              <select
                value={draft.maxComparisonSelections}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    maxComparisonSelections: Number(e.target.value),
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2"
              >
                {[1, 2, 3, 4, 5, 6, 8, 10].map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <p className="text-xs text-slate-500">
              Сколько позиций можно выбрать в сводке для одновременного сравнения.
            </p>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">Обновление сводки</h3>
            <label className="flex flex-col gap-1 text-sm">
              <span className="font-medium text-slate-700">Интервал автообновления</span>
              <select
                value={draft.refreshIntervalSec}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    refreshIntervalSec: Number(e.target.value),
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2"
              >
                {REFRESH_INTERVAL_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">Фильтр оплаты</h3>
            <div className="space-y-2">
              {PAYMENT_FILTER_OPTIONS.map((option) => (
                <label key={option.value} className="flex items-center gap-2 text-sm">
                  <input
                    type="radio"
                    name="payment-filter"
                    checked={draft.paymentFilter === option.value}
                    onChange={() =>
                      setDraft((prev) => ({
                        ...prev,
                        paymentFilter: option.value,
                      }))
                    }
                    className="h-4 w-4 border-slate-300"
                  />
                  <span className="text-slate-700">{option.label}</span>
                </label>
              ))}
            </div>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">По умолчанию</h3>
            <label className="flex flex-col gap-1 text-sm">
              <span className="font-medium text-slate-700">Год при открытии</span>
              <input
                type="number"
                min={2000}
                max={2100}
                value={draft.defaultYear}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    defaultYear: Number(e.target.value),
                  }))
                }
                className="w-32 rounded-lg border border-slate-300 px-3 py-2"
              />
            </label>
          </section>

          <section className="space-y-3">
            <h3 className="text-sm font-semibold text-slate-800">Отображение</h3>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={draft.compactTable}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    compactTable: e.target.checked,
                  }))
                }
                className="h-4 w-4 rounded border-slate-300"
              />
              <span className="text-slate-700">Компактная таблица</span>
            </label>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={draft.highlightUnpaidRows}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    highlightUnpaidRows: e.target.checked,
                  }))
                }
                className="h-4 w-4 rounded border-slate-300"
              />
              <span className="text-slate-700">Подсвечивать неоплаченные строки</span>
            </label>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={draft.showRowNumbers}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    showRowNumbers: e.target.checked,
                  }))
                }
                className="h-4 w-4 rounded border-slate-300"
              />
              <span className="text-slate-700">Нумерация строк</span>
            </label>
          </section>
        </div>

        <div className="flex items-center justify-between gap-3 border-t border-slate-200 px-5 py-4">
          <button
            type="button"
            onClick={() => setDraft(DEFAULT_SETTINGS)}
            className="rounded-lg border border-slate-300 px-4 py-2 text-sm text-slate-600 hover:bg-slate-50"
          >
            Сбросить
          </button>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={closePanel}
              className="rounded-lg border border-slate-300 px-4 py-2 text-sm text-slate-600 hover:bg-slate-50"
            >
              Отмена
            </button>
            <button
              type="button"
              onClick={handleSave}
              className="rounded-lg bg-report-header px-4 py-2 text-sm font-medium text-white hover:bg-slate-800"
            >
              Сохранить
            </button>
          </div>
        </div>
      </aside>
    </div>
  );
}
