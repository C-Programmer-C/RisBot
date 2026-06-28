import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type ColumnFiltersState,
  type SortingState,
} from "@tanstack/react-table";
import { useEffect, useMemo, useState } from "react";
import type { CellValue, ReportTable } from "@/types/sales-report";

interface ReportTableViewProps {
  table: ReportTable;
  compactTable?: boolean;
  highlightUnpaidRows?: boolean;
  showRowNumbers?: boolean;
  embedded?: boolean;
}

function formatCell(value: CellValue): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") {
    return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(value);
  }
  return String(value);
}

function renderCell(columnName: string, value: CellValue) {
  if (columnName === "Pyrus") {
    const url = String(value ?? "").trim();
    if (!url.startsWith("http")) return "—";

    const match = url.match(/#id(\d+)/i);
    const label = match ? `#${match[1]}` : "Открыть";

    return (
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className="font-medium text-blue-600 hover:text-blue-800 hover:underline"
      >
        {label}
      </a>
    );
  }

  return formatCell(value);
}

function cellSortValue(value: CellValue): string | number {
  if (typeof value === "number") return value;
  if (value === null || value === undefined) return "";
  return String(value).toLowerCase();
}

function compareCells(a: CellValue, b: CellValue): number {
  const left = cellSortValue(a);
  const right = cellSortValue(b);

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  return String(left).localeCompare(String(right), "ru", { numeric: true, sensitivity: "base" });
}

function columnIncludesFilter(rowValue: CellValue, filterValue: string, columnName?: string): boolean {
  const query = filterValue.trim().toLowerCase();
  if (!query) return true;

  if (columnName === "Pyrus") {
    const url = String(rowValue ?? "").toLowerCase();
    return url.includes(query) || url.replace("https://pyrus.com/t#id", "").includes(query);
  }

  const formatted = formatCell(rowValue).toLowerCase();
  if (formatted.includes(query)) return true;

  if (typeof rowValue === "number") {
    return String(rowValue).includes(query);
  }

  return false;
}

function SortIndicator({ direction }: { direction: false | "asc" | "desc" }) {
  if (!direction) {
    return <span className="ml-1 text-slate-400">↕</span>;
  }
  return <span className="ml-1 text-blue-600">{direction === "asc" ? "↑" : "↓"}</span>;
}

export function ReportTableView({
  table,
  compactTable = false,
  highlightUnpaidRows = true,
  showRowNumbers = false,
  embedded = false,
}: ReportTableViewProps) {
  const paidColumnIndex = table.columns.indexOf("Оплачено");
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [globalFilter, setGlobalFilter] = useState("");

  useEffect(() => {
    setSorting([]);
    setColumnFilters([]);
    setGlobalFilter("");
  }, [table.id]);

  const cellClass = compactTable
    ? "whitespace-nowrap border border-report-border px-2 py-1 text-xs text-slate-800"
    : "whitespace-nowrap border border-report-border px-3 py-2 text-slate-800";
  const headClass = compactTable
    ? "whitespace-nowrap border border-report-border px-2 py-1 text-left text-xs font-semibold text-slate-700"
    : "whitespace-nowrap border border-report-border px-3 py-2 text-left font-semibold text-slate-700";
  const filterInputClass = compactTable
    ? "w-full min-w-[72px] rounded border border-slate-300 px-1.5 py-1 text-[11px]"
    : "w-full min-w-[88px] rounded border border-slate-300 px-2 py-1 text-xs";

  const columns = useMemo<ColumnDef<Record<string, CellValue>>[]>(
    () =>
      table.columns.map((col, index) => ({
        accessorKey: `col_${index}`,
        header: col,
        cell: ({ getValue }) => renderCell(col, getValue() as CellValue),
        enableSorting: true,
        enableColumnFilter: true,
        sortingFn: (rowA, rowB, columnId) =>
          compareCells(rowA.getValue(columnId) as CellValue, rowB.getValue(columnId) as CellValue),
        filterFn: (row, columnId, filterValue) => {
          const index = Number(columnId.replace("col_", ""));
          const columnName = table.columns[index];
          return columnIncludesFilter(
            row.getValue(columnId) as CellValue,
            String(filterValue ?? ""),
            columnName,
          );
        },
      })),
    [table.columns],
  );

  const data = useMemo(
    () =>
      table.rows.map((row) =>
        Object.fromEntries(row.map((cell, index) => [`col_${index}`, cell])),
      ),
    [table.rows],
  );

  const reactTable = useReactTable({
    data,
    columns,
    state: { sorting, columnFilters, globalFilter },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: (row, _columnId, filterValue) => {
      const query = String(filterValue ?? "")
        .trim()
        .toLowerCase();
      if (!query) return true;

      return table.columns.some((column, index) =>
        columnIncludesFilter(row.getValue(`col_${index}`) as CellValue, query, column),
      );
    },
  });

  const filteredRows = reactTable.getRowModel().rows;
  const totalRows = table.rows.length;
  const hasActiveFilters =
    globalFilter.trim().length > 0 ||
    columnFilters.some((filter) => String(filter.value ?? "").trim().length > 0) ||
    sorting.length > 0;

  const resetTableState = () => {
    setSorting([]);
    setColumnFilters([]);
    setGlobalFilter("");
  };

  const colSpan = table.columns.length + (showRowNumbers ? 1 : 0);

  return (
    <section
      className={
        embedded
          ? "overflow-hidden bg-white"
          : "overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm"
      }
    >
      {!embedded ? (
        <div className="border-b border-slate-200 bg-report-header px-4 py-3">
          <h2 className="text-sm font-semibold text-white">{table.title}</h2>
          {typeof table.meta?.row_count === "number" ? (
            <p className="mt-1 text-xs text-blue-100">Строк: {table.meta.row_count}</p>
          ) : null}
        </div>
      ) : null}

      <div className="flex flex-wrap items-center gap-2 border-b border-slate-200 bg-slate-50 px-3 py-2">
        <input
          type="search"
          value={globalFilter}
          onChange={(e) => setGlobalFilter(e.target.value)}
          placeholder="Поиск по всей таблице..."
          className={`min-w-[180px] flex-1 rounded-lg border border-slate-300 bg-white ${
            compactTable ? "px-2 py-1 text-xs" : "px-3 py-1.5 text-sm"
          }`}
        />
        {hasActiveFilters ? (
          <button
            type="button"
            onClick={resetTableState}
            className={`rounded-lg border border-slate-300 bg-white text-slate-600 hover:bg-slate-100 ${
              compactTable ? "px-2 py-1 text-xs" : "px-3 py-1.5 text-sm"
            }`}
          >
            Сбросить
          </button>
        ) : null}
        <span className={`text-slate-500 ${compactTable ? "text-[11px]" : "text-xs"}`}>
          Показано {filteredRows.length} из {totalRows}
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className={`min-w-full border-collapse ${compactTable ? "text-xs" : "text-sm"}`}>
          <thead>
            {reactTable.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="bg-slate-100">
                {showRowNumbers ? <th className={headClass}>#</th> : null}
                {headerGroup.headers.map((header) => (
                  <th key={header.id} className={headClass}>
                    {header.column.getCanSort() ? (
                      <button
                        type="button"
                        onClick={header.column.getToggleSortingHandler()}
                        className="flex w-full items-center text-left hover:text-blue-700"
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        <SortIndicator direction={header.column.getIsSorted()} />
                      </button>
                    ) : (
                      flexRender(header.column.columnDef.header, header.getContext())
                    )}
                  </th>
                ))}
              </tr>
            ))}
            <tr className="bg-slate-50">
              {showRowNumbers ? (
                <th className="border border-report-border px-2 py-1" />
              ) : null}
              {reactTable.getHeaderGroups()[0]?.headers.map((header) => (
                <th key={`filter-${header.id}`} className="border border-report-border p-1">
                  <input
                    type="search"
                    value={(header.column.getFilterValue() as string) ?? ""}
                    onChange={(e) => header.column.setFilterValue(e.target.value)}
                    placeholder="Фильтр"
                    className={filterInputClass}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredRows.length === 0 ? (
              <tr>
                <td colSpan={colSpan} className={`${cellClass} text-center text-slate-500`}>
                  {totalRows === 0 ? "Нет данных за выбранный период" : "Ничего не найдено по фильтрам"}
                </td>
              </tr>
            ) : (
              filteredRows.map((row, rowIndex) => {
                const paidValue =
                  paidColumnIndex >= 0
                    ? (row.getValue(`col_${paidColumnIndex}`) as CellValue)
                    : null;
                const isUnpaid = highlightUnpaidRows && paidValue === "Нет";

                return (
                  <tr
                    key={row.id}
                    className={
                      isUnpaid
                        ? "bg-red-50"
                        : rowIndex % 2 === 0
                          ? "bg-white"
                          : "bg-report-stripe"
                    }
                  >
                    {showRowNumbers ? (
                      <td className={`${cellClass} text-slate-500`}>{rowIndex + 1}</td>
                    ) : null}
                    {row.getVisibleCells().map((cell) => (
                      <td key={cell.id} className={cellClass}>
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
