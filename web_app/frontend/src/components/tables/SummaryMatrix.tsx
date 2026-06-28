import { Fragment } from "react";
import { getSlotStyles, slotLabel } from "@/features/sales-report/selection-slots";
import type { DetailSelection, SummaryMatrixResponse } from "@/types/sales-report";

interface SummaryMatrixProps {
  data: SummaryMatrixResponse;
  selections: DetailSelection[];
  onToggleSelect: (selection: DetailSelection) => void;
}

function formatKg(value: number): string {
  if (value === 0) return "0";
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(value);
}

function rowYearTotal(months: number[]): number {
  return months.reduce((sum, value) => sum + value, 0);
}

export function SummaryMatrix({ data, selections, onToggleSelect }: SummaryMatrixProps) {
  return (
    <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 bg-report-header px-4 py-3">
        <h2 className="text-sm font-semibold text-white">
          Сводка по продажам — {data.year}
        </h2>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse text-xs sm:text-sm">
          <thead>
            <tr className="bg-slate-100">
              <th className="sticky left-0 z-10 border border-report-border bg-slate-100 px-3 py-2 text-left font-semibold">
                Продукт
              </th>
              {data.months.map((month) => (
                <th
                  key={month}
                  className="whitespace-nowrap border border-report-border px-2 py-2 text-center font-semibold"
                >
                  {month}
                </th>
              ))}
              <th className="whitespace-nowrap border border-report-border bg-slate-200 px-2 py-2 text-center font-semibold">
                Итого
              </th>
            </tr>
          </thead>
          <tbody>
            {data.sections.map((section, sectionIndex) => (
              <Fragment key={`section-block-${sectionIndex}`}>
                {section.title ? (
                  <tr className="bg-slate-200">
                    <td
                      colSpan={data.months.length + 2}
                      className="border border-report-border px-3 py-2 font-semibold text-slate-800"
                    >
                      {section.title}
                    </td>
                  </tr>
                ) : null}
                {section.rows.map((row) => {
                  const yearTotal = rowYearTotal(row.months);
                  const isTotalRow = row.type === "total";

                  if (isTotalRow) {
                    return (
                      <tr key={`total-${row.name}`} className="bg-amber-50 font-semibold">
                        <td className="sticky left-0 border border-report-border bg-amber-50 px-3 py-2">
                          {row.name}
                        </td>
                        {row.months.map((value, index) => (
                          <td
                            key={`${row.name}-${index}`}
                            className="border border-report-border px-2 py-2 text-center"
                          >
                            {formatKg(value)}
                          </td>
                        ))}
                        <td className="border border-report-border bg-amber-100 px-2 py-2 text-center">
                          {formatKg(yearTotal)}
                        </td>
                      </tr>
                    );
                  }

                  return (
                    <tr key={row.name} className="hover:bg-slate-50">
                      <td className="sticky left-0 border border-report-border bg-white px-3 py-2 font-medium">
                        {row.name}
                      </td>
                      {row.months.map((value, monthIndex) => {
                        const month = monthIndex + 1;
                        const slotIndex = selections.findIndex(
                          (item) =>
                            item.product === row.name &&
                            item.year === data.year &&
                            item.month === month,
                        );
                        const isSelected = slotIndex >= 0;
                        const slotStyles = isSelected ? getSlotStyles(slotIndex) : null;

                        return (
                          <td
                            key={`${row.name}-${month}`}
                            className="border border-report-border p-0 text-center"
                          >
                            <button
                              type="button"
                              onClick={() =>
                                onToggleSelect({
                                  product: row.name,
                                  year: data.year,
                                  month,
                                })
                              }
                              className={`relative block w-full px-2 py-2 font-semibold transition-colors hover:bg-blue-50 ${
                                isSelected
                                  ? `${slotStyles!.cell} ring-2 ring-inset ${slotStyles!.ring}`
                                  : ""
                              }`}
                            >
                              {isSelected ? (
                                <span
                                  className={`absolute right-1 top-1 flex h-4 w-4 items-center justify-center rounded-full text-[10px] font-bold text-white ${slotStyles!.badge}`}
                                >
                                  {slotLabel(slotIndex)}
                                </span>
                              ) : null}
                              {formatKg(value)}
                            </button>
                          </td>
                        );
                      })}
                      <td className="border border-report-border bg-slate-50 px-2 py-2 text-center font-medium">
                        {formatKg(yearTotal)}
                      </td>
                    </tr>
                  );
                })}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
