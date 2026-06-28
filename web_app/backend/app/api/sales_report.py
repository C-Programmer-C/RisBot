from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.reports.products import MONTH_NAMES, default_product_mappings, get_product_ids
from app.reports.schemas import (
    CatalogItem,
    CatalogResponse,
    ReportTable,
    SummaryMatrixResponse,
    SummaryRequest,
)
from app.reports.sales_report import build_detail_table, build_summary_matrix
from app.services.pyrus_client import PyrusClient

router = APIRouter()


def _ensure_pyrus() -> None:
    if not settings.pyrus_configured:
        raise HTTPException(
            status_code=503,
            detail="Pyrus не настроен. Заполни PYRUS_LOGIN и PYRUS_SECURITY_KEY в backend/.env",
        )


@router.get("/catalog-items", response_model=CatalogResponse)
def get_catalog_items() -> CatalogResponse:
    _ensure_pyrus()
    try:
        client = PyrusClient.shared()
        payload = client.get_catalog()
        raw_items = payload.get("items") or []
        items: list[CatalogItem] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("item_id")
            values = item.get("values") or []
            if item_id is None:
                continue
            name = str(values[0]).strip() if values else str(item_id)
            items.append(CatalogItem(item_id=int(item_id), name=name))
        items.sort(key=lambda row: row.name.lower())
        return CatalogResponse(
            catalog_id=int(payload.get("catalog_id") or settings.pyrus_catalog_id),
            name=str(payload.get("name") or "Прайс"),
            items=items,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/product-mappings/default")
def get_default_product_mappings() -> dict[str, list[int]]:
    return default_product_mappings()


@router.post("/summary", response_model=SummaryMatrixResponse)
def post_summary(body: SummaryRequest) -> SummaryMatrixResponse:
    _ensure_pyrus()
    try:
        return build_summary_matrix(
            body.year,
            product_mappings=body.product_mappings,
            force=body.force,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/summary", response_model=SummaryMatrixResponse)
def get_summary(
    year: int = Query(default=2026, ge=2000, le=2100),
    force: bool = Query(False, description="Сбросить кэш и загрузить заново"),
) -> SummaryMatrixResponse:
    _ensure_pyrus()
    try:
        return build_summary_matrix(year, force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/details", response_model=ReportTable)
def get_details(
    product: str = Query(..., description="Название продукта"),
    year: int = Query(..., ge=2000, le=2100),
    month: int = Query(..., ge=1, le=12),
    payment_filter: str = Query(
        "all",
        description="Фильтр оплаты: all, unpaid, paid",
        pattern="^(all|unpaid|paid)$",
    ),
    product_ids: str | None = Query(None, description="CSV item_id из прайса"),
    force: bool = Query(False, description="Сбросить кэш и загрузить заново"),
) -> ReportTable:
    _ensure_pyrus()

    mappings: dict[str, list[int]] | None = None
    if product_ids:
        parsed_ids = [int(item_id.strip()) for item_id in product_ids.split(",") if item_id.strip()]
        if not parsed_ids:
            raise HTTPException(status_code=400, detail="product_ids пустой")
        mappings = {product: parsed_ids}
    elif not get_product_ids(product):
        raise HTTPException(status_code=400, detail=f"Неизвестный продукт: {product}")

    try:
        return build_detail_table(
            product,
            year,
            month,
            payment_filter,
            product_mappings=mappings,
            force=force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/products")
def list_products() -> dict[str, list[str]]:
    return {"months": MONTH_NAMES}
