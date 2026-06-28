import type { DetailColumnId } from "@/constants/detail-columns";
import type { ReportTable } from "@/types/sales-report";

export function filterTableColumns(
  table: ReportTable,
  columnOrder: DetailColumnId[],
  visibleColumns: DetailColumnId[],
): ReportTable {
  const visibleSet = new Set(visibleColumns);
  const orderedVisible = columnOrder.filter(
    (column) => visibleSet.has(column) && table.columns.includes(column),
  );

  if (
    orderedVisible.length === table.columns.length &&
    orderedVisible.every((column, index) => column === table.columns[index])
  ) {
    return table;
  }

  const indices = orderedVisible.map((column) => table.columns.indexOf(column));

  return {
    ...table,
    columns: orderedVisible,
    rows: table.rows.map((row) => indices.map((index) => row[index])),
  };
}
