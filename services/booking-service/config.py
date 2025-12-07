# services/booking-service/app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/booking_db"
    
    # Booking constraints
    MAX_BOOKING_HOURS: int = 6

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
