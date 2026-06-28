import type { DetailColumnId } from "@/constants/detail-columns";
import type { PaymentFilter } from "@/constants/payment-filter";
import type { ReportProductName } from "@/constants/products";

export interface AppSettings {
  columnOrder: DetailColumnId[];
  visibleColumns: DetailColumnId[];
  refreshIntervalSec: number;
  paymentFilter: PaymentFilter;
  productItemIds: Record<ReportProductName, number[]>;
  defaultYear: number;  compactTable: boolean;
  highlightUnpaidRows: boolean;
  showRowNumbers: boolean;
  maxComparisonSelections: number;
}
