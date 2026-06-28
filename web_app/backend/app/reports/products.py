PRODUCT_IDS: dict[str, str] = {
    "Мука рисовая В.С.": "165022095",
    "Мука рисовая 1 С.": "165022096",
    "Мука рисовая 2 С.": "165022097",
    "Дробь": "165022089,165022091,165022092",
    "Рис": "176538234,165022085,167224299,175075715,165022086",
    "Крупа": (
        "165022099,170169477,165022100,170169479,165022101,"
        "170169480,165022102,170169481,165022103,170169483"
    ),
    "Мука": "165022104,170827124,170827127,170827123,165022105,170827131",
    "Кормовые": (
        "165022107,165022108,165022109,165022110,"
        "165022111,165022112,165022113"
    ),
}

MONTH_NAMES = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]

REPORT_SECTIONS: list[dict[str, object]] = [
    {
        "title": "Джерелиевка",
        "rows": [
            {"type": "product", "name": "Мука рисовая В.С."},
            {"type": "product", "name": "Мука рисовая 1 С."},
            {"type": "product", "name": "Мука рисовая 2 С."},
            {"type": "total", "name": "Итого мука"},
        ],
    },
    {
        "title": "Дробь",
        "rows": [{"type": "product", "name": "Дробь"}],
    },
    {
        "title": "Ленинский",
        "rows": [{"type": "product", "name": "Рис"}],
    },
    {
        "title": "Северская",
        "rows": [
            {"type": "product", "name": "Крупа"},
            {"type": "product", "name": "Мука"},
            {"type": "product", "name": "Кормовые"},
            {"type": "total", "name": "Итого"},
        ],
    },
]


def get_product_ids(
    product_name: str,
    product_mappings: dict[str, list[int]] | None = None,
) -> str:
    name = product_name.strip()
    if product_mappings is not None and name in product_mappings:
        ids = product_mappings[name]
        return ",".join(str(item_id) for item_id in ids)

    return PRODUCT_IDS.get(name, "")


def default_product_mappings() -> dict[str, list[int]]:
    mappings: dict[str, list[int]] = {}
    for name, ids in PRODUCT_IDS.items():
        mappings[name] = [int(item_id) for item_id in ids.split(",") if item_id.strip()]
    return mappings


def month_number_from_name(month_name: str) -> int:
    normalized = month_name.strip().lower()
    for index, name in enumerate(MONTH_NAMES, start=1):
        if name.lower() == normalized:
            return index
    return 0
