from __future__ import annotations

from datetime import datetime, date
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


async def get_zones(session: AsyncSession) -> List[models.Zone]:
    """Вернуть все активные зоны (по умолчанию)."""
    stmt = (
        select(models.Zone)
        .where(models.Zone.is_active.is_(True))
        .order_by(models.Zone.name)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_all_zones(session: AsyncSession) -> List[models.Zone]:
    """Вернуть все зоны (включая закрытые)."""
    stmt = (
        select(models.Zone)
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
    extension_hours: int = 1,
) -> Optional[models.Booking]:
    """
    Продление брони на указанное количество часов.
    
    Логика:
    - находим бронь;
    - проверяем, что она активна и принадлежит пользователю;
    - вычисляем новое end_time = current_end_time + extension_hours;
    - проверяем, что не превышен лимит MAX_BOOKING_HOURS для общей длительности;
    - проверяем, что нет конфликтов с другими бронями;
    - проверяем, что не будет переполнения зоны;
    - создаём или находим подходящий слот и создаём новую бронь.
    """
    from datetime import timedelta
    
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
    
    # Загружаем place и zone для slot
    stmt = (
        select(models.Slot)
        .options(joinedload(models.Slot.place).joinedload(models.Place.zone))
        .where(models.Slot.id == slot.id)
    )
    result = await session.execute(stmt)
    slot = result.scalar_one_or_none()
    if slot is None or slot.place is None:
        return None
    
    # Используем денормализованные данные или берем из слота
    current_start_time = booking.start_time if booking.start_time else slot.start_time
    current_end_time = booking.end_time if booking.end_time else slot.end_time
    
    # Вычисляем новое время окончания
    new_end_time = current_end_time + timedelta(hours=extension_hours)
    
    # Проверяем, что общая длительность не превышает лимит
    total_duration = new_end_time - current_start_time
    if total_duration.total_seconds() > settings.MAX_BOOKING_HOURS * 3600:
        return None  # Превышен лимит
    
    # Проверяем, нет ли у пользователя других пересекающихся броней (кроме текущей)
    has_conflict = await check_user_booking_conflicts(
        session=session,
        user_id=user_id,
        start_time=current_end_time,
        end_time=new_end_time,
        exclude_booking_id=booking_id,
    )
    if has_conflict:
        return None  # У пользователя уже есть другая бронь на это время
    
    # Получаем zone для проверки переполнения
    zone = slot.place.zone if slot.place else None
    if zone is None or not zone.is_active:
        return None  # Зона неактивна
    
    # Проверяем, не будет ли переполнения зоны
    can_book = await check_zone_capacity(
        session=session,
        zone_id=zone.id,
        start_time=current_end_time,
        end_time=new_end_time,
    )
    if not can_book:
        return None  # Зона будет переполнена
    
    # Ищем существующий слот с точно таким временем или создаём новый
    stmt = (
        select(models.Slot)
        .options(joinedload(models.Slot.place).joinedload(models.Place.zone))
        .where(
            and_(
                models.Slot.place_id == slot.place_id,
                models.Slot.start_time == current_end_time,
                models.Slot.end_time == new_end_time,
            )
        )
    )
    result = await session.execute(stmt)
    extension_slot = result.scalar_one_or_none()
    
    if extension_slot is None:
        # Проверяем, нет ли пересекающихся слотов на этом месте
        stmt = (
            select(models.Slot)
            .where(
                and_(
                    models.Slot.place_id == slot.place_id,
                    models.Slot.start_time < new_end_time,
                    models.Slot.end_time > current_end_time,
                    models.Slot.is_available.is_(False),
                )
            )
        )
        result = await session.execute(stmt)
        conflicting_slots = list(result.scalars().all())
        
        if conflicting_slots:
            return None  # Есть конфликтующие недоступные слоты
        
        # Создаём новый слот
        extension_slot = models.Slot(
            place_id=slot.place_id,
            start_time=current_end_time,
            end_time=new_end_time,
            is_available=False,
        )
        session.add(extension_slot)
        await session.flush()
    else:
        # Если слот существует, проверяем его доступность
        if not extension_slot.is_available:
            return None  # Слот уже занят
        extension_slot.is_available = False
    
    # Создаём новую бронь на продление с денормализованными данными
    new_booking = models.Booking(
        user_id=user_id,
        slot_id=extension_slot.id,
        status="active",
        zone_name=zone.name if zone else None,
        zone_address=zone.address if zone else None,
        start_time=extension_slot.start_time,
        end_time=extension_slot.end_time,
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
    - установить is_active=False и сохранить причину закрытия и время окончания закрытия;
    - найти все активные брони в этой зоне в заданном интервале;
    - пометить их как cancelled;
    - вернуть список затронутых броней (для уведомлений).
    
    Зона автоматически откроется после окончания времени закрытия (to_time).
    """
    # Получить зону и установить is_active=False, сохранить причину и время окончания
    zone = await session.get(models.Zone, zone_id)
    if zone is None:
        return []
    
    zone.is_active = False
    zone.closure_reason = data.reason
    zone.closure_end_time = data.to_time
    
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

    # Отменяем все эти брони
    for booking in affected_bookings:
        booking.status = "cancelled"
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
    
    Использует единый запрос с условной агрегацией для избежания N+1 проблемы.
    """
    # Единый запрос с условной агрегацией для подсчета активных и отмененных броней
    stmt = (
        select(
            models.Zone.id,
            models.Zone.name,
            func.count(
                case((models.Booking.status == "active", 1))
            ).label("active_bookings"),
            func.count(
                case((models.Booking.status == "cancelled", 1))
            ).label("cancelled_bookings"),
        )
        .outerjoin(models.Place, models.Place.zone_id == models.Zone.id)
        .outerjoin(models.Slot, models.Slot.place_id == models.Place.id)
        .outerjoin(models.Booking, models.Booking.slot_id == models.Slot.id)
        .group_by(models.Zone.id, models.Zone.name)
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
                active_bookings=row.active_bookings,
                cancelled_bookings=row.cancelled_bookings,
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


async def reopen_closed_zones(
    session: AsyncSession,
) -> List[models.Zone]:
    """
    Проверить и переоткрыть зоны, у которых истекло время закрытия.
    
    Возвращает список переоткрытых зон.
    """
    now = datetime.utcnow()
    
    # Найти все закрытые зоны, у которых истекло время закрытия
    stmt = (
        select(models.Zone)
        .where(
            and_(
                models.Zone.is_active.is_(False),
                models.Zone.closure_end_time.isnot(None),
                models.Zone.closure_end_time <= now,
            )
        )
    )
    
    result = await session.execute(stmt)
    zones_to_reopen = list(result.scalars().all())
    
    # Переоткрываем зоны
    for zone in zones_to_reopen:
        zone.is_active = True
        zone.closure_reason = None
        zone.closure_end_time = None
    
    if zones_to_reopen:
        await session.commit()
        for zone in zones_to_reopen:
            await session.refresh(zone)
    
    return zones_to_reopen
