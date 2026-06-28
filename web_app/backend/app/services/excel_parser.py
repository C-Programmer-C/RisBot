from pathlib import Path
from typing import Any

import pandas as pd


def read_excel_sheets(path: Path) -> dict[str, pd.DataFrame]:
    """Читает все листы Excel-файла. Используется при миграции логики."""
    return pd.read_excel(path, sheet_name=None, engine="openpyxl")


def dataframe_to_table_rows(df: pd.DataFrame) -> tuple[list[str], list[list[Any]]]:
    """Конвертирует DataFrame в columns + rows для API."""
    columns = [str(col) for col in df.columns.tolist()]
    rows: list[list[Any]] = []
    for _, row in df.iterrows():
        rows.append([_serialize_cell(value) for value in row.tolist()])
    return columns, rows


def _serialize_cell(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value
