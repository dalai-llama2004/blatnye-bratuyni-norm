from __future__ import annotations

from datetime import datetime, date
from typing import List, Optional

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

import models
import schemas
from config import settings


# ============================================================
#                       READ-ONLY ЧАСТЬ
# ============================================================


async def get_zones(session: AsyncSession) -> List[models.Zone]:
    """Вернуть все активные зоны (по умолчанию)."""
    stmt = (
        select(models.Zone)
        .where(models.Zone.is_active.is_(True))
        .order_by(models.Zone.name)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_places_by_zone(
    session: AsyncSession,
    zone_id: int,
) -> List[models.Place]:
    """Вернуть все активные места в зоне."""
    stmt = (
        select(models.Place)
        .where(
            and_(
                models.Place.zone_id == zone_id,
                models.Place.is_active.is_(True),
            )
        )
        .order_by(models.Place.name)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_slots_by_place_and_date(
    session: AsyncSession,
    place_id: int,
    target_date: date,
) -> List[models.Slot]:
    """
    Вернуть слоты для места на конкретную дату.

    Фильтруем по дате начала слота (start_time.date() == target_date).
    """
    date_start = datetime.combine(target_date, datetime.min.time())
    date_end = datetime.combine(target_date, datetime.max.time())

    stmt = (
        select(models.Slot)
        .where(
            and_(
                models.Slot.place_id == place_id,
                models.Slot.start_time >= date_start,
                models.Slot.start_time <= date_end,
            )
        )
        .order_by(models.Slot.start_time)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


# ============================================================
#                      BOOKING ОПЕРАЦИИ
# ============================================================


async def create_booking(
    session: AsyncSession,
    user_id: int,
    booking_in: schemas.BookingCreate,
) -> Optional[models.Booking]:
    """
    Создание брони для указанного слота.

    На этом этапе:
    - проверяем, что слот существует
    - проверяем is_available
    - грубо проверяем, что у пользователя нет активной брони на этот же слот
    (более сложные проверки конкуренции можно вынести в отдельную ветку).
    """
    # 1. Найти слот с загруженными связями для получения zone info
    stmt = (
        select(models.Slot)
        .options(joinedload(models.Slot.place).joinedload(models.Place.zone))
        .where(models.Slot.id == booking_in.slot_id)
    )
    result = await session.execute(stmt)
    slot = result.scalar_one_or_none()
    
    if slot is None:
        return None  # роутер может превратить это в 404

    if not slot.is_available:
        return None  # роутер может вернуть 400 / 409

    # 2. Проверить, нет ли уже активной брони этого слота у пользователя
    stmt = select(models.Booking).where(
        and_(
            models.Booking.user_id == user_id,
            models.Booking.slot_id == slot.id,
            models.Booking.status == "active",
        )
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
        return None

    # 3. Создать бронь с денормализованными данными
    zone = slot.place.zone if slot.place else None
    booking = models.Booking(
        user_id=user_id,
        slot_id=slot.id,
        status="active",
        zone_name=zone.name if zone else None,
        zone_address=zone.address if zone else None,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    session.add(booking)

    # (опционально можно сразу пометить слот недоступным)
    slot.is_available = False

    await session.commit()
    await session.refresh(booking)
    return booking


async def create_booking_by_time_range(
    session: AsyncSession,
    user_id: int,
    booking_in: schemas.BookingCreateTimeRange,
) -> Optional[models.Booking]:
    """
    Создание брони по диапазону времени для зоны.
    
    Алгоритм:
    1. Проверить, что интервал не больше 6 часов
    2. Найти зону и получить её название и адрес
    3. Найти места в зоне
    4. Для каждого места проверить, свободно ли оно в заданном диапазоне
    5. Если есть свободное место, создать слот и бронь
    """
    from datetime import datetime, timedelta, date as date_type
    
    # Парсим дату и создаем datetime объекты
    try:
        target_date = date_type.fromisoformat(booking_in.date)
    except ValueError:
        return None
    
    start_time = datetime.combine(
        target_date,
        datetime.min.time().replace(
            hour=booking_in.start_hour,
            minute=booking_in.start_minute
        )
    )
    end_time = datetime.combine(
        target_date,
        datetime.min.time().replace(
            hour=booking_in.end_hour,
            minute=booking_in.end_minute
        )
    )
    
    # Проверка: не больше MAX_BOOKING_HOURS
    duration = end_time - start_time
    if duration.total_seconds() <= 0:
        return None  # Некорректный интервал
    if duration.total_seconds() > settings.MAX_BOOKING_HOURS * 3600:
        return None  # Больше лимита
    
    # Получить зону
    zone = await session.get(models.Zone, booking_in.zone_id)
    if zone is None or not zone.is_active:
        return None
    
    # Получить активные места в зоне
    stmt = (
        select(models.Place)
        .where(
            and_(
                models.Place.zone_id == zone.id,
                models.Place.is_active.is_(True),
            )
        )
    )
    result = await session.execute(stmt)
    places = list(result.scalars().all())
    
    if not places:
        return None
    
    # Ищем свободное место
    for place in places:
        # Проверяем, нет ли пересечений с существующими слотами
        stmt = (
            select(models.Slot)
            .where(
                and_(
                    models.Slot.place_id == place.id,
                    # Проверка пересечения интервалов
                    models.Slot.start_time < end_time,
                    models.Slot.end_time > start_time,
                )
            )
        )
        result = await session.execute(stmt)
        overlapping_slots = list(result.scalars().all())
        
        # Если есть пересекающиеся слоты, проверяем их доступность
        has_conflict = False
        for slot in overlapping_slots:
            # Если слот недоступен, значит есть конфликт
            if not slot.is_available:
                has_conflict = True
                break
        
        # Если нет конфликтов, это место свободно
        if not has_conflict:
            # Создаем слот
            slot = models.Slot(
                place_id=place.id,
                start_time=start_time,
                end_time=end_time,
                is_available=False,
            )
            session.add(slot)
            await session.flush()  # Получить slot.id
            
            # Создаем бронь
            booking = models.Booking(
                user_id=user_id,
                slot_id=slot.id,
                status="active",
                zone_name=zone.name,
                zone_address=zone.address,
                start_time=start_time,
                end_time=end_time,
            )
            session.add(booking)
            
            await session.commit()
            await session.refresh(booking)
            return booking
    
    # Нет свободных мест
    return None


async def get_booking_by_id(
    session: AsyncSession,
    booking_id: int,
) -> Optional[models.Booking]:
    """Получить бронь по id (с подгруженным слотом)."""
    stmt = (
        select(models.Booking)
        .options(joinedload(models.Booking.slot))
        .where(models.Booking.id == booking_id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def cancel_booking(
    session: AsyncSession,
    user_id: int,
    booking_id: int,
    *,
    is_admin: bool = False,
) -> Optional[models.Booking]:
    """
    Отмена брони пользователем (или админом).

    - пользователь может отменить только свою бронь;
    - админ может отменить любую.
    """
    booking = await get_booking_by_id(session, booking_id)
    if booking is None:
        return None

    if not is_admin and booking.user_id != user_id:
        return None

    if booking.status != "active":
        # уже отменена / завершена
        return booking

    booking.status = "cancelled"

    # слот можно снова пометить доступным (упрощённо)
    if booking.slot:
        booking.slot.is_available = True

    await session.commit()
    await session.refresh(booking)
    return booking


async def get_booking_history(
    session: AsyncSession,
    user_id: int,
    filters: Optional[schemas.BookingHistoryFilters] = None,
) -> List[models.Booking]:
    """
    История бронирований пользователя с фильтрами:
    - дата (по времени слота)
    - зона
    - статус
    """
    filters = filters or schemas.BookingHistoryFilters()

    # Джойнимся к Slot и Place/Zone, чтобы фильтровать по зоне и датам
    stmt = (
        select(models.Booking)
        .join(models.Slot, models.Slot.id == models.Booking.slot_id)
        .join(models.Place, models.Place.id == models.Slot.place_id)
        .join(models.Zone, models.Zone.id == models.Place.zone_id)
        .where(models.Booking.user_id == user_id)
        .options(joinedload(models.Booking.slot))
        .order_by(models.Booking.created_at.desc())
    )

    conds = []

    if filters.status:
        conds.append(models.Booking.status == filters.status)

    if filters.zone_id:
        conds.append(models.Zone.id == filters.zone_id)

    if filters.date_from:
        conds.append(models.Slot.start_time >= filters.date_from)

    if filters.date_to:
        conds.append(models.Slot.start_time <= filters.date_to)

    if conds:
        stmt = stmt.where(and_(*conds))

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def extend_booking(
    session: AsyncSession,
    user_id: int,
    booking_id: int,
) -> Optional[models.Booking]:
    """
    Продление брони на следующий слот.

    Логика (упрощённо):
    - находим бронь;
    - проверяем, что она активна и принадлежит пользователю;
    - находим следующий слот для того же места (start_time = end_time текущего слота);
    - если он свободен, создаём НОВУЮ бронь на следующий слот.
    """
    booking = await get_booking_by_id(session, booking_id)
    if booking is None:
        return None

    if booking.user_id != user_id:
        return None

    if booking.status != "active":
        return None

    slot = booking.slot
    if slot is None:
        return None

    # Ищем следующий слот
    stmt = select(models.Slot).where(
        and_(
            models.Slot.place_id == slot.place_id,
            models.Slot.start_time == slot.end_time,
            models.Slot.is_available.is_(True),
        )
    )
    result = await session.execute(stmt)
    next_slot = result.scalar_one_or_none()
    if next_slot is None:
        return None

    # Проверяем, нет ли уже активной брони этого пользователя на этот слот
    stmt = select(models.Booking).where(
        and_(
            models.Booking.user_id == user_id,
            models.Booking.slot_id == next_slot.id,
            models.Booking.status == "active",
        )
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
        return None

    # Создаём новую бронь на следующий слот
    new_booking = models.Booking(
        user_id=user_id,
        slot_id=next_slot.id,
        status="active",
    )
    session.add(new_booking)
    next_slot.is_available = False

    await session.commit()
    await session.refresh(new_booking)
    return new_booking


# ============================================================
#                      АДМИНСКИЕ ОПЕРАЦИИ
# ============================================================


async def create_zone(
    session: AsyncSession,
    data: schemas.ZoneCreate,
) -> models.Zone:
    zone = models.Zone(
        name=data.name,
        address=data.address,
        is_active=data.is_active,
    )
    session.add(zone)
    await session.flush()  # Получить zone.id для создания мест
    
    # Автоматически создаём places_count мест
    for i in range(1, data.places_count + 1):
        place = models.Place(
            zone_id=zone.id,
            name=f"Место {i}",
            is_active=True,
        )
        session.add(place)
    
    await session.commit()
    await session.refresh(zone)
    return zone


async def update_zone(
    session: AsyncSession,
    zone_id: int,
    data: schemas.ZoneUpdate,
) -> Optional[models.Zone]:
    zone = await session.get(models.Zone, zone_id)
    if zone is None:
        return None

    # Обновляем только те поля, которые переданы
    update_data = data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(zone, field, value)

    await session.commit()
    await session.refresh(zone)
    return zone


async def delete_zone(
    session: AsyncSession,
    zone_id: int,
) -> bool:
    zone = await session.get(models.Zone, zone_id)
    if zone is None:
        return False

    await session.delete(zone)
    await session.commit()
    return True


async def close_zone(
    session: AsyncSession,
    zone_id: int,
    data: schemas.ZoneCloseRequest,
) -> List[models.Booking]:
    """
    Закрыть зону на обслуживание:
    - найти все будущие активные брони в этой зоне за заданный интервал;
    - пометить их как cancelled;
    - вернуть список затронутых броней (для уведомлений).
    """
    # Находим все брони через join: Booking -> Slot -> Place -> Zone
    stmt = (
        select(models.Booking)
        .join(models.Slot, models.Slot.id == models.Booking.slot_id)
        .join(models.Place, models.Place.id == models.Slot.place_id)
        .join(models.Zone, models.Zone.id == models.Place.zone_id)
        .where(
            and_(
                models.Zone.id == zone_id,
                models.Booking.status == "active",
                models.Slot.start_time >= data.from_time,
                models.Slot.start_time <= data.to_time,
            )
        )
        .options(joinedload(models.Booking.slot))
    )

    result = await session.execute(stmt)
    affected_bookings: List[models.Booking] = list(result.scalars().all())

    if not affected_bookings:
        return []

    # Отменяем все эти брони
    for booking in affected_bookings:
        booking.status = "cancelled"
        if booking.slot:
            booking.slot.is_available = True

    await session.commit()
    # Можно не делать refresh для всех, но на всякий случай:
    for booking in affected_bookings:
        await session.refresh(booking)

    return affected_bookings
