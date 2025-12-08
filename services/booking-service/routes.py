from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from security import get_current_user_id

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Query,
    Path,
)
from sqlalchemy.ext.asyncio import AsyncSession

import crud
import schemas
from crud import BookingExtensionError
from db import get_session

router = APIRouter(tags=["booking"])


@router.get(
    "/zones",
    response_model=List[schemas.ZoneOut],
    summary="Список зон",
)
async def list_zones(
    include_inactive: bool = Query(False, description="Включить неактивные зоны"),
    session: AsyncSession = Depends(get_session),
):
    return await crud.get_zones(session, include_inactive=include_inactive)


@router.get(
    "/zones/{zone_id}/places",
    response_model=List[schemas.PlaceOut],
    summary="Список мест в зоне",
)
async def list_places_in_zone(
    zone_id: int,
    session: AsyncSession = Depends(get_session),
):
    return await crud.get_places_by_zone(session, zone_id)


@router.get(
    "/places/{place_id}/slots",
    response_model=List[schemas.SlotOut],
    summary="Доступные слоты по месту и дате",
)
async def list_slots(
    place_id: int,
    date_: date = Query(..., alias="date"),
    session: AsyncSession = Depends(get_session),
):
    return await crud.get_slots_by_place_and_date(session, place_id, date_)


@router.post(
    "/bookings",
    response_model=schemas.BookingOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать бронь",
)
async def create_booking(
    booking_in: schemas.BookingCreate,
    session: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_current_user_id),
):
    booking = await crud.create_booking(session, user_id, booking_in)
    if booking is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT, 
            "Невозможно создать бронь: слот недоступен или зона переполнена"
        )
    return booking


@router.post(
    "/bookings/by-time",
    response_model=schemas.BookingOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать бронь по времени",
)
async def create_booking_by_time(
    booking_in: schemas.BookingCreateTimeRange,
    session: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_current_user_id),
):
    booking = await crud.create_booking_by_time_range(session, user_id, booking_in)
    if booking is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Невозможно создать бронь: нет свободных мест, некорректный интервал, превышен лимит в 6 часов или зона переполнена"
        )
    return booking


@router.post(
    "/bookings/cancel",
    response_model=schemas.BookingOut,
    summary="Отмена брони",
)
async def cancel_booking(
    data: schemas.BookingCancelRequest,
    session: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_current_user_id),
):
    booking = await crud.cancel_booking(
        session, user_id, data.booking_id, is_admin=False
    )
    if booking is None:
        raise HTTPException(404, "Бронь не найдена или нет прав")
    return booking


@router.get(
    "/bookings/history",
    response_model=List[schemas.BookingOut],
    summary="История броней",
)
async def booking_history(
    status_: Optional[str] = Query(None, alias="status"),
    zone_id: Optional[int] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    session: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_current_user_id),
):
    filters = schemas.BookingHistoryFilters(
        status=status_,
        zone_id=zone_id,
        date_from=(
            None if date_from is None else datetime.combine(date_from, datetime.min.time())
        ),
        date_to=(
            None if date_to is None else datetime.combine(date_to, datetime.max.time())
        ),
    )

    return await crud.get_booking_history(session, user_id, filters)


@router.post(
    "/bookings/{booking_id}/extend",
    response_model=schemas.BookingOut,
    summary="Продлить бронь",
)
async def extend_booking(
    booking_id: int,
    extend_data: schemas.BookingExtendTimeRequest,
    session: AsyncSession = Depends(get_session),
    user_id: int = Depends(get_current_user_id),
):
    try:
        booking = await crud.extend_booking(
            session, 
            user_id, 
            booking_id,
            extend_hours=extend_data.extend_hours,
            extend_minutes=extend_data.extend_minutes,
        )
        return booking
    except BookingExtensionError as e:
        # Возвращаем детальное описание ошибки пользователю
        raise HTTPException(400, str(e))
