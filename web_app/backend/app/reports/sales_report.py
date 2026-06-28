from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone

from app.config import settings
from app.reports.products import (
    MONTH_NAMES,
    REPORT_SECTIONS,
    get_product_ids,
    month_number_from_name,
)
from app.reports.schemas import ReportTable, SalesReportResponse, SummaryMatrixResponse
from app.services.pyrus_client import PyrusClient, filter_and_sort_tasks, sum_kg_from_tasks
from app.services.task_cache import task_cache

DETAIL_COLUMNS = [
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
]


@dataclass(frozen=True)
class _MonthJob:
    product_name: str
    month: int
    product_ids: str


def _collect_month_jobs(
    product_mappings: dict[str, list[int]] | None = None,
) -> list[_MonthJob]:
    jobs: list[_MonthJob] = []
    seen_products: set[str] = set()

    for section in REPORT_SECTIONS:
        for row in section["rows"]:
            if row["type"] != "product":
                continue
            product_name = str(row["name"])
            if product_name in seen_products:
                continue
            seen_products.add(product_name)

            product_ids = get_product_ids(product_name, product_mappings)
            if not product_ids:
                continue

            for month in range(1, 13):
                jobs.append(_MonthJob(product_name, month, product_ids))

    return jobs


def _fetch_job(client: PyrusClient, year: int, job: _MonthJob) -> tuple[str, int, float]:
    tasks = task_cache.get_or_fetch(client, year, job.month, job.product_ids)
    total = sum_kg_from_tasks(tasks)
    return job.product_name, job.month, total if total else 0.0


def _fetch_all_product_totals(
    client: PyrusClient,
    year: int,
    product_mappings: dict[str, list[int]] | None = None,
) -> dict[str, list[float]]:
    jobs = _collect_month_jobs(product_mappings)
    if not jobs:
        return {}

    totals: dict[str, list[float]] = {}
    workers = max(1, settings.pyrus_fetch_workers)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(lambda job: _fetch_job(client, year, job), jobs)

    for product_name, month, total in results:
        if product_name not in totals:
            totals[product_name] = [0.0] * 12
        totals[product_name][month - 1] = total

    return totals


def _sum_months(rows: list[list[float]]) -> list[float]:
    if not rows:
        return [0.0] * 12
    totals = [0.0] * 12
    for row in rows:
        for index, value in enumerate(row):
            totals[index] += value
    return totals


def build_summary_matrix(
    year: int,
    *,
    product_mappings: dict[str, list[int]] | None = None,
    force: bool = False,
) -> SummaryMatrixResponse:
    if force:
        task_cache.clear_year(year)

    client = PyrusClient.shared()
    product_totals = _fetch_all_product_totals(client, year, product_mappings)
    sections: list[dict[str, object]] = []

    for section in REPORT_SECTIONS:
        section_rows: list[dict[str, object]] = []
        product_months: list[list[float]] = []

        for row in section["rows"]:
            row_type = str(row["type"])

            if row_type == "total":
                months = _sum_months(product_months)
                section_rows.append(
                    {
                        "type": "total",
                        "name": row["name"],
                        "months": months,
                    }
                )
                continue

            product_name = str(row["name"])
            product_ids = get_product_ids(product_name, product_mappings)
            if not product_ids:
                months = [0.0] * 12
            else:
                months = product_totals.get(product_name, [0.0] * 12)

            product_months.append(months)
            section_rows.append(
                {
                    "type": "product",
                    "name": product_name,
                    "months": months,
                }
            )

        sections.append(
            {
                "title": section.get("title"),
                "rows": section_rows,
            }
        )

    return SummaryMatrixResponse(
        year=year,
        months=MONTH_NAMES,
        sections=sections,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def build_detail_table(
    product_name: str,
    year: int,
    month: int | str,
    payment_filter: str = "all",
    *,
    product_mappings: dict[str, list[int]] | None = None,
    force: bool = False,
) -> ReportTable:
    month_num = month if isinstance(month, int) else month_number_from_name(str(month))
    if month_num < 1 or month_num > 12:
        raise ValueError("Invalid month")

    product_ids = get_product_ids(product_name, product_mappings)
    if not product_ids:
        raise ValueError(f"Unknown product or no catalog items selected: {product_name}")

    if force:
        task_cache.clear_year(year)

    client = PyrusClient.shared()
    tasks = task_cache.get_or_fetch(client, year, month_num, product_ids)
    rows_data = filter_and_sort_tasks(tasks, payment_filter=payment_filter)

    month_name = MONTH_NAMES[month_num - 1]
    title = f"{product_name} за {month_name} {year}"
    if payment_filter == "unpaid":
        title += " (неоплаченные)"
    elif payment_filter == "paid":
        title += " (оплаченные)"

    rows = [
        [
            item["date_ship"],
            item["price_name"],
            item["volume_kg"],
            item["price_per_kg"],
            item["price_per_kg_delivery"],
            item["organization"],
            item["total_price"],
            item["paid_label"],
            item["supplier"],
            item["ship_address"],
            item["new_lead"],
            item["task_id"],
            item["pyrus_url"],
        ]
        for item in rows_data
    ]

    return ReportTable(
        id=f"detail-{product_name}-{year}-{month_num}",
        title=title,
        columns=DETAIL_COLUMNS,
        rows=rows,
        meta={
            "product": product_name,
            "year": year,
            "month": month_num,
            "month_name": month_name,
            "payment_filter": payment_filter,
            "row_count": len(rows),
        },
    )


def build_sales_report(
    period_from: str | None = None,
    period_to: str | None = None,
) -> SalesReportResponse:
    year = datetime.now().year
    if period_from and len(period_from) >= 4:
        try:
            year = int(period_from[:4])
        except ValueError:
            pass

    summary = build_summary_matrix(year)
    return SalesReportResponse(
        period_from=period_from,
        period_to=period_to,
        generated_at=summary.generated_at,
        tables=[],
        summary={"year": year, "legacy": True},
    )
