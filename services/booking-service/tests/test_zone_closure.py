"""
Тесты для проверки функциональности закрытия зон и автоматического открытия.
"""
import pytest
from datetime import datetime, timedelta

import models
import crud


@pytest.mark.asyncio
async def test_close_zone_adds_cancellation_reason(test_session):
    """
    Тест проверяет, что при закрытии зоны в брони добавляется причина отмены.
    """
    # Создаём зону, место и слот
    zone = models.Zone(name="Тестовая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    future_time = datetime.utcnow() + timedelta(days=1)
    slot = models.Slot(
        place_id=place.id,
        start_time=future_time,
        end_time=future_time + timedelta(hours=2),
        is_available=False
    )
    test_session.add(slot)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Закрываем зону
    from schemas import ZoneCloseRequest
    close_request = ZoneCloseRequest(
        reason="Ремонт",
        from_time=future_time - timedelta(hours=1),
        to_time=future_time + timedelta(hours=3),
    )
    
    affected_bookings = await crud.close_zone(test_session, zone.id, close_request)
    
    # Проверяем, что бронь отменена с причиной
    assert len(affected_bookings) == 1
    assert affected_bookings[0].status == "cancelled"
    assert affected_bookings[0].cancellation_reason is not None
    assert "Зона закрыта: Ремонт" in affected_bookings[0].cancellation_reason
    
    # Проверяем, что зона закрыта и сохранено время закрытия
    await test_session.refresh(zone)
    assert zone.is_active is False
    assert zone.closure_reason == "Ремонт"
    assert zone.closed_until == close_request.to_time


@pytest.mark.asyncio
async def test_zone_auto_reactivation(test_session):
    """
    Тест проверяет автоматическое открытие зоны после окончания времени закрытия.
    """
    # Создаём закрытую зону с истекшим временем закрытия
    past_time = datetime.utcnow() - timedelta(hours=1)
    zone = models.Zone(
        name="Закрытая зона",
        address="Адрес",
        is_active=False,
        closure_reason="Уборка",
        closed_until=past_time,
    )
    test_session.add(zone)
    await test_session.commit()
    
    # Получаем список зон (должно вызвать автоматическое открытие)
    zones = await crud.get_zones(test_session, include_inactive=True)
    
    # Проверяем, что зона снова активна
    await test_session.refresh(zone)
    assert zone.is_active is True
    assert zone.closure_reason is None
    assert zone.closed_until is None


@pytest.mark.asyncio
async def test_zone_not_reactivated_if_still_closed(test_session):
    """
    Тест проверяет, что зона не открывается, если время закрытия ещё не истекло.
    """
    # Создаём закрытую зону с будущим временем закрытия
    future_time = datetime.utcnow() + timedelta(hours=2)
    zone = models.Zone(
        name="Закрытая зона",
        address="Адрес",
        is_active=False,
        closure_reason="Уборка",
        closed_until=future_time,
    )
    test_session.add(zone)
    await test_session.commit()
    
    # Получаем список зон
    zones = await crud.get_zones(test_session, include_inactive=True)
    
    # Проверяем, что зона всё ещё закрыта
    await test_session.refresh(zone)
    assert zone.is_active is False
    assert zone.closure_reason == "Уборка"
    assert zone.closed_until == future_time


@pytest.mark.asyncio
async def test_get_zones_include_inactive(test_session):
    """
    Тест проверяет получение всех зон, включая закрытые.
    """
    # Создаём активную и неактивную зоны
    active_zone = models.Zone(name="Активная", address="Адрес1", is_active=True)
    inactive_zone = models.Zone(
        name="Неактивная",
        address="Адрес2",
        is_active=False,
        closure_reason="Ремонт",
        closed_until=datetime.utcnow() + timedelta(days=1),
    )
    test_session.add_all([active_zone, inactive_zone])
    await test_session.commit()
    
    # Получаем только активные зоны
    active_zones = await crud.get_zones(test_session, include_inactive=False)
    assert len(active_zones) == 1
    assert active_zones[0].name == "Активная"
    
    # Получаем все зоны
    all_zones = await crud.get_zones(test_session, include_inactive=True)
    assert len(all_zones) == 2
    zone_names = {z.name for z in all_zones}
    assert "Активная" in zone_names
    assert "Неактивная" in zone_names


@pytest.mark.asyncio
async def test_extend_booking_with_custom_time(test_session):
    """
    Тест проверяет продление брони на указанное время (часы и минуты).
    """
    # Создаём зону, место и слот
    zone = models.Zone(name="Тестовая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот на завтра
    base_time = datetime.utcnow() + timedelta(days=1)
    slot = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=1),
        is_available=False
    )
    test_session.add(slot)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Продлеваем бронь на 2 часа 30 минут
    extended_booking = await crud.extend_booking(
        test_session,
        user_id=1,
        booking_id=booking.id,
        extend_hours=2,
        extend_minutes=30,
    )
    
    # Проверяем, что создана новая бронь
    assert extended_booking is not None
    assert extended_booking.id != booking.id
    assert extended_booking.user_id == 1
    assert extended_booking.status == "active"
    
    # Проверяем время новой брони
    assert extended_booking.start_time == booking.end_time
    expected_end_time = booking.end_time + timedelta(hours=2, minutes=30)
    assert extended_booking.end_time == expected_end_time
    
    # Проверяем денормализованные данные
    assert extended_booking.zone_name == zone.name
    assert extended_booking.zone_address == zone.address


@pytest.mark.asyncio
async def test_extend_booking_respects_max_hours(test_session):
    """
    Тест проверяет, что продление не позволяет превысить максимальное время брони.
    """
    from config import settings
    
    # Создаём зону, место и слот
    zone = models.Zone(name="Тестовая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот на максимально допустимое время минус 1 час
    base_time = datetime.utcnow() + timedelta(days=1)
    slot = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=settings.MAX_BOOKING_HOURS - 1),
        is_available=False
    )
    test_session.add(slot)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Пытаемся продлить бронь на 2 часа (должно превысить лимит)
    with pytest.raises(crud.BookingExtensionError) as exc_info:
        await crud.extend_booking(
            test_session,
            user_id=1,
            booking_id=booking.id,
            extend_hours=2,
            extend_minutes=0,
        )
    
    # Проверяем, что получили правильную ошибку
    assert "максимальный лимит" in str(exc_info.value).lower()
