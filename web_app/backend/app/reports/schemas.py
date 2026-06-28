from typing import Any

from pydantic import BaseModel, Field


class ReportTable(BaseModel):
    id: str
    title: str
    columns: list[str]
    rows: list[list[Any]]
    meta: dict[str, Any] = Field(default_factory=dict)


class SummaryMatrixResponse(BaseModel):
    year: int
    months: list[str]
    sections: list[dict[str, Any]]
    generated_at: str


class SalesReportResponse(BaseModel):
    period_from: str | None = None
    period_to: str | None = None
    generated_at: str
    tables: list[ReportTable]
    summary: dict[str, Any] = Field(default_factory=dict)


class CatalogItem(BaseModel):
    item_id: int
    name: str


class CatalogResponse(BaseModel):
    catalog_id: int
    name: str
    items: list[CatalogItem]


class SummaryRequest(BaseModel):
    year: int = Field(default=2026, ge=2000, le=2100)
    product_mappings: dict[str, list[int]] | None = None
    force: bool = False
