export const DETAIL_COLUMNS = [
  "Дата отгрузки",
  "Прайс",
  "объем кг",
  "Цена за кг",
  "Цена за кг (Дост)",
  "Организация",
  "Цена",
  "Оплачено",
  "Поставщик",
  "Адрес отгрузки",
  "Новый лид",
  "ID",
  "Pyrus",
] as const;

export type DetailColumnId = (typeof DETAIL_COLUMNS)[number];

export const HIDDEN_BY_DEFAULT_COLUMNS: DetailColumnId[] = ["ID", "Pyrus"];

export const DEFAULT_VISIBLE_COLUMNS = DETAIL_COLUMNS.filter(
  (column) => !HIDDEN_BY_DEFAULT_COLUMNS.includes(column),
);

export const REFRESH_INTERVAL_OPTIONS = [
  { value: 0, label: "Выключено" },
  { value: 15, label: "15 сек" },
  { value: 30, label: "30 сек" },
  { value: 60, label: "1 мин" },
  { value: 120, label: "2 мин" },
  { value: 300, label: "5 мин" },
] as const;

export const PYRUS_LINK_COLUMN = "Pyrus" as const;
