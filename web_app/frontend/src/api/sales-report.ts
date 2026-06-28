import type { PaymentFilter } from "@/constants/payment-filter";
import type { ReportProductName } from "@/constants/products";
import type {
  DetailSelection,
  ReportTable,
  SummaryMatrixResponse,
} from "@/types/sales-report";
import { getApiUrl } from "./config";

export interface CatalogItem {
  item_id: number;
  name: string;
}

export interface CatalogResponse {
  catalog_id: number;
  name: string;
  items: CatalogItem[];
}

export async function fetchCatalogItems(): Promise<CatalogResponse> {
  const apiUrl = await getApiUrl();
  const response = await fetch(`${apiUrl}/api/sales-report/catalog-items`);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail ?? `Ошибка загрузки прайса: ${response.status}`);
  }
  return response.json();
}

export async function fetchSummaryMatrix(
  year: number,
  productItemIds: Record<ReportProductName, number[]>,
  force = false,
): Promise<SummaryMatrixResponse> {
  const apiUrl = await getApiUrl();
  const response = await fetch(`${apiUrl}/api/sales-report/summary`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      year,
      product_mappings: productItemIds,
      force,
    }),
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail ?? `Ошибка загрузки сводки: ${response.status}`);
  }
  return response.json();
}

export async function fetchDetailTable(
  selection: DetailSelection,
  productItemIds: Record<ReportProductName, number[]>,
  paymentFilter: PaymentFilter,
  force = false,
): Promise<ReportTable> {
  const productIds = productItemIds[selection.product as ReportProductName] ?? [];
  const params = new URLSearchParams({
    product: selection.product,
    year: String(selection.year),
    month: String(selection.month),
    payment_filter: paymentFilter,
    product_ids: productIds.join(","),
  });
  if (force) {
    params.set("force", "true");
  }
  const apiUrl = await getApiUrl();
  const response = await fetch(`${apiUrl}/api/sales-report/details?${params}`);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail ?? `Ошибка загрузки деталей: ${response.status}`);
  }
  return response.json();
}
