from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import List, Optional

from sqlalchemy import select, and_, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

import models
import schemas
from config import settings


# ============================================================
#                       READ-ONLY ЧАСТЬ
# ============================================================


async def get_zones(session: AsyncSession, include_inactive: bool = False) -> List[models.Zone]:
    """
    Вернуть все зоны.
    
    Параметры:
    - include_inactive: если True, вернуть все зоны (включая неактивные)
    """
    # Автоматически активируем зоны, у которых истекло время закрытия
    now = datetime.utcnow()
    stmt_reactivate = (
        select(models.Zone)
        .where(
            and_(
                models.Zone.is_active.is_(False),
                models.Zone.closed_until.isnot(None),
                models.Zone.closed_until <= now,
            )
        )
    )
    result = await session.execute(stmt_reactivate)
    zones_to_reactivate = list(result.scalars().all())
    
    for zone in zones_to_reactivate:
        zone.is_active = True
        zone.closure_reason = None
        zone.closed_until = None
    
    if zones_to_reactivate:
        await session.commit()
    
    # Вернуть зоны согласно фильтру
    if include_inactive:
        stmt = select(models.Zone).order_by(models.Zone.name)
    else:
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


async def check_user_booking_conflicts(
    session: AsyncSession,
    user_id: int,
    start_time: datetime,
    end_time: datetime,
    exclude_booking_id: Optional[int] = None,
) -> bool:
    """
    Проверить, нет ли у пользователя пересекающихся активных броней.
    
    Возвращает True, если есть конфликт (пересечение).
    Возвращает False, если конфликта нет (можно бронировать).
    """
    # Найти все активные брони пользователя, которые пересекаются с заданным интервалом
    stmt = select(models.Booking).where(
        and_(
            models.Booking.user_id == user_id,
            models.Booking.status == "active",
            models.Booking.start_time < end_time,
            models.Booking.end_time > start_time,
        )
    )
    
    # Исключить определённую бронь, если указано (для операции extend)
    if exclude_booking_id is not None:
        stmt = stmt.where(models.Booking.id != exclude_booking_id)
    
    result = await session.execute(stmt)
    conflicting_bookings = result.scalars().all()
    
    return len(conflicting_bookings) > 0


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
    
    # 2.0. Проверить, нет ли у пользователя пересекающихся активных броней (в любой зоне)
    has_conflict = await check_user_booking_conflicts(
        session=session,
        user_id=user_id,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    if has_conflict:
        return None  # У пользователя уже есть бронь на это время
    
    # 2.1. Проверить, не будет ли переполнения зоны
    zone = slot.place.zone if slot.place else None
    if zone:
        can_book = await check_zone_capacity(
            session=session,
            zone_id=zone.id,
            start_time=slot.start_time,
            end_time=slot.end_time,
        )
        if not can_book:
            return None  # Зона будет переполнена

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
    
    # Проверить, нет ли у пользователя пересекающихся активных броней (в любой зоне)
    has_conflict = await check_user_booking_conflicts(
        session=session,
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
    )
    if has_conflict:
        return None  # У пользователя уже есть бронь на это время
    
    # Проверить, не будет ли переполнения зоны
    can_book = await check_zone_capacity(
        session=session,
        zone_id=zone.id,
        start_time=start_time,
        end_time=end_time,
    )
    if not can_book:
        return None  # Зона будет переполнена
    
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
        # Сначала проверяем, есть ли уже слот с точно таким же временем
        stmt_exact = (
            select(models.Slot)
            .where(
                and_(
                    models.Slot.place_id == place.id,
                    models.Slot.start_time == start_time,
                    models.Slot.end_time == end_time,
                )
            )
        )
        result_exact = await session.execute(stmt_exact)
        exact_slot = result_exact.scalar_one_or_none()
        
        # Если есть точный слот и он доступен, используем его
        if exact_slot and exact_slot.is_available:
            exact_slot.is_available = False
            
            # Создаем бронь
            booking = models.Booking(
                user_id=user_id,
                slot_id=exact_slot.id,
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
        
        # Если точный слот уже занят, пропускаем это место
        if exact_slot and not exact_slot.is_available:
            continue
        
        # Проверяем, нет ли пересечений с существующими слотами (если точного слота нет)
        if not exact_slot:
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
            
            # Если нет конфликтов, это место свободно - создаем новый слот
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
    extend_hours: int = 1,
    extend_minutes: int = 0,
) -> Optional[models.Booking]:
    """
    Продление брони на заданное время.

    Логика:
    - находим бронь;
    - проверяем, что она активна и принадлежит пользователю;
    - вычисляем новое время окончания (end_time + extend_hours + extend_minutes);
    - проверяем, что продление не превышает MAX_BOOKING_HOURS от начала брони;
    - проверяем отсутствие конфликтов с другими бронями пользователя;
    - проверяем, что зона не будет переполнена;
    - создаём или находим подходящий слот для продлённого времени;
    - создаём НОВУЮ бронь на продлённый период.
    """
    booking = await get_booking_by_id(session, booking_id)
    if booking is None:
        return None

    if booking.user_id != user_id:
        return None

    if booking.status != "active":
        return None

    if booking.start_time is None or booking.end_time is None:
        return None

    slot = booking.slot
    if slot is None:
        return None

    # Вычисляем новое время окончания
    new_end_time = booking.end_time + timedelta(hours=extend_hours, minutes=extend_minutes)
    
    # Проверяем, что общая продолжительность брони не превышает MAX_BOOKING_HOURS
    total_duration = new_end_time - booking.start_time
    if total_duration.total_seconds() > settings.MAX_BOOKING_HOURS * 3600:
        return None  # Превышен лимит
    
    # Проверяем, нет ли у пользователя пересекающихся активных броней (кроме текущей)
    has_conflict = await check_user_booking_conflicts(
        session=session,
        user_id=user_id,
        start_time=booking.end_time,
        end_time=new_end_time,
        exclude_booking_id=booking_id,
    )
    if has_conflict:
        return None  # У пользователя уже есть другая бронь на это время
    
    # Получаем zone для проверки вместимости и денормализации
    zone = None
    if slot.place:
        # Загружаем zone для place
        stmt = (
            select(models.Place)
            .options(joinedload(models.Place.zone))
            .where(models.Place.id == slot.place_id)
        )
        result = await session.execute(stmt)
        place = result.scalar_one_or_none()
        if place and place.zone:
            zone = place.zone
    
    if zone is None:
        return None
    
    # Проверяем, не будет ли переполнения зоны в новом интервале
    can_book = await check_zone_capacity(
        session=session,
        zone_id=zone.id,
        start_time=booking.end_time,
        end_time=new_end_time,
    )
    if not can_book:
        return None  # Зона будет переполнена

    # Ищем существующий слот для продлённого времени или создаём новый
    stmt_exact = (
        select(models.Slot)
        .where(
            and_(
                models.Slot.place_id == slot.place_id,
                models.Slot.start_time == booking.end_time,
                models.Slot.end_time == new_end_time,
            )
        )
    )
    result_exact = await session.execute(stmt_exact)
    extended_slot = result_exact.scalar_one_or_none()
    
    # Если слот существует и доступен, используем его
    if extended_slot and extended_slot.is_available:
        extended_slot.is_available = False
    # Если слот существует, но занят, не можем продлить
    elif extended_slot and not extended_slot.is_available:
        return None
    # Если слота нет, проверяем конфликты и создаём новый
    else:
        # Проверяем, нет ли пересекающихся слотов для этого места
        stmt_overlap = (
            select(models.Slot)
            .where(
                and_(
                    models.Slot.place_id == slot.place_id,
                    models.Slot.start_time < new_end_time,
                    models.Slot.end_time > booking.end_time,
                )
            )
        )
        result_overlap = await session.execute(stmt_overlap)
        overlapping_slots = list(result_overlap.scalars().all())
        
        # Если есть занятые пересекающиеся слоты, не можем продлить
        for overlap_slot in overlapping_slots:
            if not overlap_slot.is_available:
                return None
        
        # Создаём новый слот
        extended_slot = models.Slot(
            place_id=slot.place_id,
            start_time=booking.end_time,
            end_time=new_end_time,
            is_available=False,
        )
        session.add(extended_slot)
        await session.flush()  # Получить extended_slot.id
    
    # Создаём новую бронь на продлённый период с денормализованными данными
    new_booking = models.Booking(
        user_id=user_id,
        slot_id=extended_slot.id,
        status="active",
        zone_name=zone.name if zone else None,
        zone_address=zone.address if zone else None,
        start_time=booking.end_time,
        end_time=new_end_time,
    )
    session.add(new_booking)

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
    - установить is_active=False и сохранить причину закрытия и время до которого закрыта;
    - найти все будущие активные брони в этой зоне за заданный интервал;
    - пометить их как cancelled с указанием причины;
    - вернуть список затронутых броней (для уведомлений).
    """
    # Получить зону и установить is_active=False, сохранить причину и время
    zone = await session.get(models.Zone, zone_id)
    if zone is None:
        return []
    
    zone.is_active = False
    zone.closure_reason = data.reason
    zone.closed_until = data.to_time
    
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

    # Отменяем все эти брони с указанием причины
    for booking in affected_bookings:
        booking.status = "cancelled"
        booking.cancellation_reason = f"Зона закрыта: {data.reason}"
        if booking.slot:
            booking.slot.is_available = True

    await session.commit()
    # Можно не делать refresh для всех, но на всякий случай:
    await session.refresh(zone)
    for booking in affected_bookings:
        await session.refresh(booking)

    return affected_bookings


async def get_zones_statistics(
    session: AsyncSession,
) -> List[schemas.ZoneStatistics]:
    """
    Получить статистику по всем зонам:
    - количество активных бронирований (status=active)
    - количество отмененных бронирований (status=cancelled)
    - текущая загрузка зоны (сколько человек сейчас в зоне)
    
    Использует единый запрос с условной агрегацией для избежания N+1 проблемы.
    """
    now = datetime.utcnow()
    
    # Единый запрос с условной агрегацией для подсчета активных и отмененных броней
    stmt = (
        select(
            models.Zone.id,
            models.Zone.name,
            models.Zone.is_active,
            models.Zone.closure_reason,
            models.Zone.closed_until,
            func.count(
                case((models.Booking.status == "active", 1))
            ).label("active_bookings"),
            func.count(
                case((models.Booking.status == "cancelled", 1))
            ).label("cancelled_bookings"),
            func.count(
                case(
                    (
                        and_(
                            models.Booking.status == "active",
                            models.Booking.start_time <= now,
                            models.Booking.end_time > now,
                        ),
                        1
                    )
                )
            ).label("current_occupancy"),
        )
        .outerjoin(models.Place, models.Place.zone_id == models.Zone.id)
        .outerjoin(models.Slot, models.Slot.place_id == models.Place.id)
        .outerjoin(models.Booking, models.Booking.slot_id == models.Slot.id)
        .group_by(
            models.Zone.id,
            models.Zone.name,
            models.Zone.is_active,
            models.Zone.closure_reason,
            models.Zone.closed_until,
        )
        .order_by(models.Zone.name)
    )
    
    result = await session.execute(stmt)
    rows = result.all()
    
    statistics = []
    for row in rows:
        statistics.append(
            schemas.ZoneStatistics(
                zone_id=row.id,
                zone_name=row.name,
                is_active=row.is_active,
                closure_reason=row.closure_reason,
                closed_until=row.closed_until,
                active_bookings=row.active_bookings,
                cancelled_bookings=row.cancelled_bookings,
                current_occupancy=row.current_occupancy,
            )
        )
    
    return statistics


async def get_global_statistics(
    session: AsyncSession,
) -> schemas.GlobalStatistics:
    """
    Получить общую статистику:
    - общее число активных бронирований (status=active)
    - общее число отмененных бронирований (status=cancelled)
    - число пользователей "прямо сейчас" в коворкинге
    """
    # Подсчет активных и отмененных бронирований
    stmt = select(
        func.count(models.Booking.id).filter(models.Booking.status == "active").label("active_count"),
        func.count(models.Booking.id).filter(models.Booking.status == "cancelled").label("cancelled_count"),
    )
    result = await session.execute(stmt)
    row = result.one()
    
    total_active = row.active_count or 0
    total_cancelled = row.cancelled_count or 0
    
    # Подсчет пользователей прямо сейчас в коворкинге
    # Берем все активные брони, у которых start_time <= now < end_time
    now = datetime.utcnow()
    stmt = select(func.count(func.distinct(models.Booking.user_id))).where(
        and_(
            models.Booking.status == "active",
            models.Booking.start_time <= now,
            models.Booking.end_time > now,
        )
    )
    result = await session.execute(stmt)
    users_now = result.scalar() or 0
    
    return schemas.GlobalStatistics(
        total_active_bookings=total_active,
        total_cancelled_bookings=total_cancelled,
        users_in_coworking_now=users_now,
    )


async def check_zone_capacity(
    session: AsyncSession,
    zone_id: int,
    start_time: datetime,
    end_time: datetime,
) -> bool:
    """
    Проверить, не будет ли переполнения зоны в заданном временном интервале.
    
    Алгоритм:
    1. Получить количество мест в зоне (это максимальная вместимость)
    2. Найти все активные брони в зоне, пересекающиеся с заданным интервалом
    3. Для каждой точки времени в интервале проверить, что число активных броней не превышает количество мест
    
    Возвращает True, если зона НЕ переполнена (бронь можно создать)
    Возвращает False, если зона будет переполнена
    """
    # Получить количество активных мест в зоне
    stmt = select(func.count(models.Place.id)).where(
        and_(
            models.Place.zone_id == zone_id,
            models.Place.is_active.is_(True),
        )
    )
    result = await session.execute(stmt)
    max_capacity = result.scalar() or 0
    
    if max_capacity == 0:
        return False  # Нет мест в зоне
    
    # Найти все активные брони в зоне, пересекающиеся с заданным интервалом
    # Используем денормализованные поля start_time и end_time в Booking
    stmt = (
        select(models.Booking)
        .join(models.Slot, models.Slot.id == models.Booking.slot_id)
        .join(models.Place, models.Place.id == models.Slot.place_id)
        .where(
            and_(
                models.Place.zone_id == zone_id,
                models.Booking.status == "active",
                models.Booking.start_time < end_time,
                models.Booking.end_time > start_time,
            )
        )
    )
    result = await session.execute(stmt)
    overlapping_bookings = list(result.scalars().all())
    
    # Создаем список критических точек времени (начала и концы броней)
    # для проверки максимальной загрузки в каждой точке
    time_points = []
    
    # Добавляем начало и конец нашей новой брони
    time_points.append(start_time)
    time_points.append(end_time)
    
    # Добавляем начала и концы всех пересекающихся броней
    for booking in overlapping_bookings:
        if booking.start_time and booking.end_time:
            time_points.append(booking.start_time)
            time_points.append(booking.end_time)
    
    # Сортируем точки времени
    time_points = sorted(set(time_points))
    
    # Проверяем загрузку в каждый момент времени
    # Проверяем все точки, включая start_time и end_time
    for check_time in time_points:
        # Проверяем только точки внутри нашего интервала [start_time, end_time)
        if check_time < start_time or check_time >= end_time:
            continue
        
        # Считаем количество активных броней в этот момент времени
        # Используем полуоткрытый интервал [start, end)
        active_count = 0
        for booking in overlapping_bookings:
            if (booking.start_time and booking.end_time and
                booking.start_time <= check_time < booking.end_time):
                active_count += 1
        
        # Добавляем нашу новую бронь (она активна в интервале [start_time, end_time))
        if start_time <= check_time < end_time:
            active_count += 1
        
        # Проверяем переполнение
        if active_count > max_capacity:
            return False  # Переполнение обнаружено
    
    return True  # Зона не переполнена
