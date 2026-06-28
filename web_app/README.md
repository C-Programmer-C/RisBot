# Отчёт по позициям с неоплаченными

Веб-версия Excel-отчёта с интеграцией Pyrus API (форма 1562280).

## Логика (из VBA)

- **Сводка** — продукты × месяцы, объёмы кг из Pyrus (поле id=4)
- **Детализация** — клик по ячейке месяца: 11 колонок как в Excel
- **Фильтр** — объём > 0 и цена за кг > 0; опционально «только неоплаченные»

## Настройка Pyrus

Скопируй `backend/.env.example` → `backend/.env` и укажи:

```
PYRUS_LOGIN=...
PYRUS_SECURITY_KEY=...
PYRUS_FORM_ID=1562280
```

## API

| Endpoint | Описание |
|----------|----------|
| `GET /api/sales-report/summary?year=2026` | Матрица сводки |
| `GET /api/sales-report/details?product=...&year=...&month=...&unpaid_only=false` | Детальная таблица |

## Быстрый старт

```
web_app/
├── frontend/          # React + Vite + TypeScript
│   └── src/
│       ├── features/sales-report/   # страницы и логика отчёта
│       ├── components/              # UI-компоненты (таблицы и т.д.)
│       ├── api/                     # запросы к backend
│       └── types/                   # типы данных отчёта
├── backend/           # FastAPI
│   └── app/
│       ├── api/                     # REST-эндпоинты
│       ├── reports/                 # бизнес-логика отчёта (из Excel)
│       └── services/                # парсинг Excel, расчёты
└── scripts/           # утилиты (dev-запуск и т.п.)
```

## Требования

- Node.js 20+
- Python 3.11+
- npm или pnpm

## Быстрый старт

### 1. Backend

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8000
```

API: http://localhost:8000  
Документация: http://localhost:8000/docs

### 2. Frontend

```powershell
cd frontend
npm install
copy .env.example .env
npm run dev
```

Приложение: http://localhost:5173

### 3. Оба сервиса одной командой (из корня)

```powershell
.\scripts\dev.ps1
```

## Что добавить позже

1. Скинуть Excel-файл отчёта и/или VBA/формулы — перенесём логику в `backend/app/reports/`
2. Описать структуру листов и таблиц — заполним типы в `frontend/src/types/sales-report.ts`
3. Подключим реальные данные к таблицам в `frontend/src/features/sales-report/`

## Переменные окружения

| Файл | Переменная | Описание |
|------|-----------|----------|
| `frontend/.env` | `VITE_API_URL` | URL backend (по умолчанию `http://localhost:8000`) |
| `backend/.env` | `CORS_ORIGINS` | Разрешённые origin для CORS |
| `backend/.env` | `DATA_DIR` | Папка для загрузок и экспорта |
