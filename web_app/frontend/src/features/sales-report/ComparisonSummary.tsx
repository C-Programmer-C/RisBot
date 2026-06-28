import type { DetailSelection, ReportTable } from "@/types/sales-report";
import { slotLabel } from "./selection-slots";
import { computeStatsForTable } from "./DetailPanel";
import { selectionLabel } from "./selection-utils";

interface ComparisonSummaryProps {
  selections: DetailSelection[];
  tables: ReportTable[];
  monthNames: string[];
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(value);
}

function formatDelta(a: number, b: number): string {
  const diff = b - a;
  const prefix = diff > 0 ? "+" : "";
  return `${prefix}${formatNumber(diff)}`;
}

function deltaClass(a: number, b: number, higherIsBetter = true): string {
  if (a === b) return "text-slate-500";
  const bBetter = higherIsBetter ? b > a : b < a;
  return bBetter ? "text-emerald-700" : "text-red-700";
}

const METRICS = [
  { key: "rowCount" as const, label: "Позиций", format: (v: number) => String(v), higherIsBetter: true },
  { key: "volumeKg" as const, label: "Объём, кг", format: formatNumber, higherIsBetter: true },
  { key: "totalPrice" as const, label: "Сумма, ₽", format: formatNumber, higherIsBetter: true },
  { key: "unpaidCount" as const, label: "Неоплаченных", format: (v: number) => String(v), higherIsBetter: false },
];

export function ComparisonSummary({ selections, tables, monthNames }: ComparisonSummaryProps) {
  const stats = tables.map((table) => computeStatsForTable(table));
  const showDelta = selections.length === 2;

  return (
    <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 bg-slate-800 px-4 py-3">
        <h2 className="text-sm font-semibold text-white">
          {selections.length === 1
            ? "Итого по позиции"
            : `Сравнение ${selections.length} позиций`}
        </h2>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse text-sm">
          <thead>
            <tr className="bg-slate-100">
              <th className="border border-report-border px-3 py-2 text-left font-semibold">
                Показатель
              </th>
              {selections.map((selection, index) => (
                  <th
                    key={selectionKey(selection, index)}
                    className="border border-report-border px-3 py-2 text-left font-semibold text-slate-800"
                  >
                    {slotLabel(index)} — {selectionLabel(selection, monthNames)}
                  </th>
                ))}
              {showDelta ? (
                <th className="border border-report-border px-3 py-2 text-left font-semibold">
                  Δ (B − A)
                </th>
              ) : null}
            </tr>
          </thead>
          <tbody>
            {METRICS.map((metric) => (
              <tr key={metric.key} className="hover:bg-slate-50">
                <td className="border border-report-border px-3 py-2 font-medium text-slate-700">
                  {metric.label}
                </td>
                {stats.map((item, index) => (
                  <td key={`${metric.key}-${index}`} className="border border-report-border px-3 py-2 text-slate-800">
                    {metric.format(item[metric.key])}
                  </td>
                ))}
                {showDelta ? (
                  <td
                    className={`border border-report-border px-3 py-2 font-medium ${deltaClass(
                      stats[0][metric.key],
                      stats[1][metric.key],
                      metric.higherIsBetter,
                    )}`}
                  >
                    {formatDelta(stats[0][metric.key], stats[1][metric.key])}
                  </td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function selectionKey(selection: DetailSelection, index: number): string {
  return `${selection.product}-${selection.year}-${selection.month}-${index}`;
}
