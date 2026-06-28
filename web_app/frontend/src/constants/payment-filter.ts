export type PaymentFilter = "all" | "unpaid" | "paid";

export const PAYMENT_FILTER_OPTIONS: Array<{ value: PaymentFilter; label: string }> = [
  { value: "all", label: "Все позиции" },
  { value: "unpaid", label: "Только неоплаченные" },
  { value: "paid", label: "Только оплаченные" },
];
