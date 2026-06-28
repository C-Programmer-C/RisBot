from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_BACKEND_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_BACKEND_ROOT / ".env"),
        env_file_encoding="utf-8",
    )

    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    data_dir: Path = Path("./data")

    pyrus_login: str = ""
    pyrus_security_key: str = ""
    pyrus_form_id: int = 1562280
    pyrus_catalog_id: int = 252029
    pyrus_cache_ttl: int = 60
    pyrus_fetch_workers: int = 16

    @model_validator(mode="after")
    def _fill_pyrus_from_bot_env(self) -> "Settings":
        import os

        if not self.pyrus_login:
            self.pyrus_login = os.getenv("LOGIN", "")
        if not self.pyrus_security_key:
            self.pyrus_security_key = os.getenv("SECURITY_KEY", "")
        return self

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def exports_dir(self) -> Path:
        return self.data_dir / "exports"

    @property
    def pyrus_configured(self) -> bool:
        return bool(self.pyrus_login and self.pyrus_security_key)


settings = Settings()
