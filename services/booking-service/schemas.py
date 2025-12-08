from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field


# ------------------------------------------------------------
# Базовый класс для всех выходных схем (включает orm_mode)
# ------------------------------------------------------------
class ORMBase(BaseModel):
    model_config = {"from_attributes": True}  # FastAPI + SQLAlchemy 2.0-friendly


# ============================================================
#                          ZONES
# ============================================================

class ZoneBase(BaseModel):
    name: str = Field(..., json_schema_extra={"example": "Главный коворкинг"})
    address: Optional[str] = Field(None, json_schema_extra={"example": "пр. Гагарина 15"})
    is_active: bool = True


class ZoneCreate(ZoneBase):
    places_count: int = Field(..., ge=1, json_schema_extra={"example": 10})


class ZoneUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    is_active: Optional[bool] = None


class ZoneOut(ORMBase):
    id: int
    name: str
    address: Optional[str]
    is_active: bool
    closure_reason: Optional[str]
    closed_until: Optional[datetime]
    created_at: datetime
    updated_at: datetime


# ============================================================
#                          PLACES
# ============================================================

class PlaceOut(ORMBase):
    id: int
    zone_id: int
    name: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


# ============================================================
#                          SLOTS
# ============================================================

class SlotOut(ORMBase):
    id: int
    place_id: int
    start_time: datetime
    end_time: datetime
    is_available: bool


# ============================================================
#                        BOOKINGS
# ============================================================

# ---- CREATE / CANCEL / EXTEND ----

class BookingCreate(BaseModel):
    """
    Пользователь создаёт бронь через slot_id.
    user_id берётся из контекста авторизации:
      - сейчас: читаем из заголовка X-User-Id (временное решение)
      - позже: будет браться из JWT от Auth-сервиса
    """
    slot_id: int


class BookingCreateTimeRange(BaseModel):
    """
    Создание брони по времени для зоны (новый способ)
    """
    zone_id: int
    date: str = Field(..., json_schema_extra={"example": "2025-12-15"})
    start_hour: int = Field(..., ge=0, le=23)
    start_minute: int = Field(..., ge=0, le=55)
    end_hour: int = Field(..., ge=0, le=23)
    end_minute: int = Field(..., ge=0, le=55)
    
    @classmethod
    def model_validate(cls, value):
        # Валидация, что минуты кратны 5
        if isinstance(value, dict):
            if value.get('start_minute') is not None and value['start_minute'] % 5 != 0:
                raise ValueError('start_minute must be a multiple of 5')
            if value.get('end_minute') is not None and value['end_minute'] % 5 != 0:
                raise ValueError('end_minute must be a multiple of 5')
        return super().model_validate(value)

class BookingCancelRequest(BaseModel):
    booking_id: int


class BookingExtendRequest(BaseModel):
    """
    По факту тело не обязательно, но оставим
    на будущее (например, опция "продлить на 2 часа")
    """
    pass


class BookingHistoryFilters(BaseModel):
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    zone_id: Optional[int] = None
    status: Optional[str] = None


# ---- OUT ----

class BookingOut(ORMBase):
    id: int
    user_id: int
    slot_id: int
    zone_name: Optional[str]
    zone_address: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    status: str
    cancellation_reason: Optional[str]
    created_at: datetime
    updated_at: datetime


# ============================================================
#                      ADMIN ACTIONS
# ============================================================

class ZoneCloseRequest(BaseModel):
    reason: str = Field(..., json_schema_extra={"example": "Плановая уборка"})
    from_time: datetime = Field(..., json_schema_extra={"example": "2025-02-01T10:00:00"})
    to_time: datetime = Field(..., json_schema_extra={"example": "2025-02-01T18:00:00"})


class BookingExtendTimeRequest(BaseModel):
    """
    Запрос на продление брони с указанием времени продления.
    """
    extend_hours: int = Field(..., ge=1, le=6, json_schema_extra={"example": 2})
    extend_minutes: int = Field(default=0, ge=0, le=55, json_schema_extra={"example": 0})


class ZoneStatistics(BaseModel):
    """Статистика по зоне"""
    zone_id: int
    zone_name: str
    is_active: bool
    closure_reason: Optional[str]
    closed_until: Optional[datetime]
    active_bookings: int
    cancelled_bookings: int
    current_occupancy: int  # Сколько человек сейчас в зоне


class GlobalStatistics(BaseModel):
    """Общая статистика по всем зонам"""
    total_active_bookings: int
    total_cancelled_bookings: int
    users_in_coworking_now: int
