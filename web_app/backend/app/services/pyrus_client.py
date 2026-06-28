from __future__ import annotations

import calendar
from datetime import date
from typing import Any

import httpx

from app.config import settings


class PyrusClient:
    AUTH_URL = "https://accounts.pyrus.com/api/v4/auth"
    _shared: PyrusClient | None = None

    def __init__(self) -> None:
        self._token: str | None = None
        self._http = httpx.Client(timeout=120.0)

    @classmethod
    def shared(cls) -> PyrusClient:
        if cls._shared is None:
            cls._shared = cls()
        return cls._shared

    @property
    def register_url(self) -> str:
        return f"https://api.pyrus.com/v4/forms/{settings.pyrus_form_id}/register"

    def get_access_token(self) -> str:
        if self._token:
            return self._token

        response = self._http.post(
            self.AUTH_URL,
            json={
                "login": settings.pyrus_login,
                "security_key": settings.pyrus_security_key,
            },
            timeout=60.0,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Pyrus auth: access_token not found")
        self._token = token
        return token

    def register_tasks(
        self,
        year: int,
        month_num: int,
        product_ids: str,
    ) -> list[dict[str, Any]]:
        first_day = date(year, month_num, 1)
        last_day = date(year, month_num, calendar.monthrange(year, month_num)[1])
        date_condition = f"gt{first_day.isoformat()},lt{last_day.isoformat()}"

        response = self._http.post(
            self.register_url,
            headers={"Authorization": f"Bearer {self.get_access_token()}"},
            json={
                "fld1": date_condition,
                "include_archived": "y",
                "fld6": product_ids,
            },
        )
        response.raise_for_status()
        payload = response.json()
        tasks = payload.get("tasks") or []
        return tasks if isinstance(tasks, list) else []

    def get_catalog(self, catalog_id: int | None = None) -> dict[str, Any]:
        catalog = catalog_id if catalog_id is not None else settings.pyrus_catalog_id
        response = self._http.get(
            f"https://api.pyrus.com/v4/catalogs/{catalog}",
            headers={"Authorization": f"Bearer {self.get_access_token()}"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def get_total_kg_for_month(
        self,
        year: int,
        month_num: int,
        product_ids: str,
    ) -> float:
        tasks = self.register_tasks(year, month_num, product_ids)
        return sum_kg_from_tasks(tasks)


def sum_kg_from_tasks(tasks: list[dict[str, Any]]) -> float:
    total = 0.0
    for task in tasks:
        value = extract_field_value(task, 4)
        if isinstance(value, (int, float)):
            total += float(value)
        elif isinstance(value, str):
            cleaned = value.replace(",", ".")
            try:
                total += float(cleaned)
            except ValueError:
                continue
    return total


def extract_field_value(task: dict[str, Any], field_id: int) -> Any:
    for field in task.get("fields") or []:
        if field.get("id") != field_id:
            continue
        return field.get("value")
    return None


def extract_field_text(task: dict[str, Any], field_id: int) -> str:
    value = extract_field_value(task, field_id)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "checked" if value else ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("choice_names", "subject", "text"):
            nested = value.get(key)
            if isinstance(nested, str):
                return nested
            if isinstance(nested, list) and nested:
                first = nested[0]
                return str(first) if first is not None else ""
        rows = value.get("rows")
        if isinstance(rows, list) and rows and isinstance(rows[0], list) and rows[0]:
            return str(rows[0][0])
    if isinstance(value, list) and value:
        return str(value[0])
    return ""


def parse_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return 0.0
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


def is_paid(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.lower() == "checked"
    if isinstance(value, dict):
        if value.get("checked"):
            return True
        if value.get("value") in (True, "checked"):
            return True
    return False


def task_to_row(task: dict[str, Any]) -> dict[str, Any]:
    paid = is_paid(extract_field_value(task, 18))

    price_name = extract_field_text(task, 39)
    if not price_name:
        price_name = extract_field_text(task, 6)

    new_lead = extract_field_text(task, 30).replace("\\", "").strip()
    task_id = task.get("id")

    return {
        "date_ship": extract_field_text(task, 1),
        "price_name": price_name,
        "volume_kg": parse_number(extract_field_value(task, 35)),
        "price_per_kg": parse_number(extract_field_value(task, 12)),
        "price_per_kg_delivery": parse_number(extract_field_value(task, 14)),
        "organization": extract_field_text(task, 5),
        "total_price": parse_number(extract_field_value(task, 7)),
        "paid": paid,
        "paid_label": "Да" if paid else "Нет",
        "supplier": extract_field_text(task, 28),
        "ship_address": extract_field_text(task, 27),
        "new_lead": new_lead,
        "task_id": task_id if task_id is not None else "",
        "pyrus_url": f"https://pyrus.com/t#id{task_id}" if task_id is not None else "",
    }


def filter_and_sort_tasks(
    tasks: list[dict[str, Any]],
    payment_filter: str = "all",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for task in tasks:
        row = task_to_row(task)
        if row["volume_kg"] <= 0 or row["price_per_kg"] <= 0:
            continue
        if payment_filter == "unpaid" and row["paid"]:
            continue
        if payment_filter == "paid" and not row["paid"]:
            continue
        rows.append(row)

    rows.sort(key=lambda item: item["volume_kg"], reverse=True)
    return rows
