export type CellValue = string | number | boolean | null;

export interface ReportTable {
  id: string;
  title: string;
  columns: string[];
  rows: CellValue[][];
  meta?: Record<string, unknown>;
}

export interface SummaryRow {
  type: "product" | "total";
  name: string;
  months: number[];
}

export interface SummarySection {
  title: string | null;
  rows: SummaryRow[];
}

export interface SummaryMatrixResponse {
  year: number;
  months: string[];
  sections: SummarySection[];
  generated_at: string;
}

export interface DetailSelection {
  product: string;
  year: number;
  month: number;
}

import type { PaymentFilter } from "@/constants/payment-filter";

export interface SalesReportFilters {
  year: number;
  paymentFilter: PaymentFilter;
}
