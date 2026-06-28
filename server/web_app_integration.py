from __future__ import annotations

import sys
from pathlib import Path

WEB_APP_BACKEND = Path(__file__).resolve().parent.parent / "web_app" / "backend"
WEB_APP_FRONTEND_DIST = Path(__file__).resolve().parent.parent / "web_app" / "frontend" / "dist"

if str(WEB_APP_BACKEND) not in sys.path:
    sys.path.insert(0, str(WEB_APP_BACKEND))

from app.api.routes import router as web_api_router  # noqa: E402

__all__ = ["web_api_router", "WEB_APP_FRONTEND_DIST"]
