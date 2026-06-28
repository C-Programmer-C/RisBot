from fastapi import APIRouter

from app.api import sales_report

router = APIRouter()
router.include_router(sales_report.router, prefix="/sales-report", tags=["sales-report"])
