from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    """Runtime configuration loaded from environment or .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg2://lattice:lattice@localhost:5432/police_lattice"
    app_env: str = "development"
    log_level: str = "INFO"

    # Tempe
    tempe_calls_for_service_url: str | None = None
    tempe_general_offense_url: str | None = None
    tempe_arrests_url: str | None = None
    tempe_hate_crime_url: str | None = None
    tempe_sentiment_survey_url: str | None = None

    # Phoenix
    phx_crime_grid_url: str | None = None
    phx_station_url: str | None = None
    phx_beats_url: str | None = None
    phx_open_data_portal_domain: str | None = None
    phx_uof_dataset_id: str | None = None
    phx_ois_dataset_id: str | None = None
    phx_pgp_dataset_id: str | None = None
    phx_sof_dataset_id: str | None = None

    # External APIs
    courtlistener_token: str | None = None
    muckrock_token: str | None = None
    muckrock_username: str | None = None

    # News RSS
    abc15_rss_url: str | None = None
    azcentral_rss_url: str | None = None
    fox10_rss_url: str | None = None
    ktar_rss_url: str | None = None
    tempe_news_rss_url: str | None = None
    propublica_rss_url: str | None = None
    cronkite_rss_url: str | None = None
    phoenix_new_times_rss_url: str | None = None
    az_free_news_rss_url: str | None = None

    # Manual / OCR
    manual_drop_dir: str = "./data/manual_drops"
    pdf_ocr_output_dir: str = "./data/ocr_output"
    tesseract_cmd: str = "tesseract"


@lru_cache
def get_settings() -> Settings:
    return Settings()
