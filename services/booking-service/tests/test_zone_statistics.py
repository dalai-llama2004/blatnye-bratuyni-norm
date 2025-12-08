"""
Тесты для проверки статистики зон с текущей загрузкой.
"""
import pytest
from datetime import datetime, timedelta

import models
import crud


@pytest.mark.asyncio
async def test_zone_statistics_current_occupancy(test_session):
    """
    Тест проверяет, что статистика зоны показывает текущую загрузку.
    """
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    now = datetime.utcnow()
    
    # Создаём 3 слота и брони:
    # 1. Прошлая бронь (завершена)
    past_slot = models.Slot(
        place_id=place.id,
        start_time=now - timedelta(hours=3),
        end_time=now - timedelta(hours=2),
        is_available=False
    )
    test_session.add(past_slot)
    await test_session.flush()
    
    past_booking = models.Booking(
        user_id=1,
        slot_id=past_slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=past_slot.start_time,
        end_time=past_slot.end_time,
    )
    test_session.add(past_booking)
    
    # 2. Текущая активная бронь (прямо сейчас)
    current_slot = models.Slot(
        place_id=place.id,
        start_time=now - timedelta(minutes=30),
        end_time=now + timedelta(minutes=30),
        is_available=False
    )
    test_session.add(current_slot)
    await test_session.flush()
    
    current_booking = models.Booking(
        user_id=2,
        slot_id=current_slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=current_slot.start_time,
        end_time=current_slot.end_time,
    )
    test_session.add(current_booking)
    
    # 3. Будущая бронь
    future_slot = models.Slot(
        place_id=place.id,
        start_time=now + timedelta(hours=2),
        end_time=now + timedelta(hours=3),
        is_available=False
    )
    test_session.add(future_slot)
    await test_session.flush()
    
    future_booking = models.Booking(
        user_id=3,
        slot_id=future_slot.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=future_slot.start_time,
        end_time=future_slot.end_time,
    )
    test_session.add(future_booking)
    await test_session.commit()
    
    # Получаем статистику
    statistics = await crud.get_zones_statistics(test_session)
    
    # Проверяем статистику
    assert len(statistics) == 1
    zone_stat = statistics[0]
    
    assert zone_stat.zone_id == zone.id
    assert zone_stat.zone_name == zone.name
    assert zone_stat.is_active is True
    assert zone_stat.active_bookings == 3
    assert zone_stat.cancelled_bookings == 0
    # Текущая загрузка должна быть 1 (только current_booking)
    assert zone_stat.current_occupancy == 1


@pytest.mark.asyncio
async def test_zone_statistics_with_closed_zone(test_session):
    """
    Тест проверяет, что статистика включает закрытые зоны с информацией о закрытии.
    """
    # Создаём активную и закрытую зоны
    active_zone = models.Zone(name="Активная зона", address="Адрес1", is_active=True)
    test_session.add(active_zone)
    
    future_time = datetime.utcnow() + timedelta(days=1)
    closed_zone = models.Zone(
        name="Закрытая зона",
        address="Адрес2",
        is_active=False,
        closure_reason="Ремонт",
        closed_until=future_time,
    )
    test_session.add(closed_zone)
    await test_session.commit()
    
    # Получаем статистику
    statistics = await crud.get_zones_statistics(test_session)
    
    # Проверяем, что обе зоны присутствуют
    assert len(statistics) == 2
    
    # Находим закрытую зону в статистике
    closed_stat = next(s for s in statistics if s.zone_name == "Закрытая зона")
    
    assert closed_stat.is_active is False
    assert closed_stat.closure_reason == "Ремонт"
    assert closed_stat.closed_until == future_time
    assert closed_stat.active_bookings == 0
    assert closed_stat.cancelled_bookings == 0
    assert closed_stat.current_occupancy == 0


@pytest.mark.asyncio
async def test_zone_statistics_multiple_users_now(test_session):
    """
    Тест проверяет подсчёт нескольких пользователей в зоне прямо сейчас.
    """
    # Создаём зону с несколькими местами
    zone = models.Zone(name="Большая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    place2 = models.Place(zone_id=zone.id, name="Место 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.flush()
    
    now = datetime.utcnow()
    
    # Создаём два текущих активных слота на разных местах
    slot1 = models.Slot(
        place_id=place1.id,
        start_time=now - timedelta(minutes=15),
        end_time=now + timedelta(minutes=45),
        is_available=False
    )
    slot2 = models.Slot(
        place_id=place2.id,
        start_time=now - timedelta(minutes=10),
        end_time=now + timedelta(minutes=50),
        is_available=False
    )
    test_session.add_all([slot1, slot2])
    await test_session.flush()
    
    # Создаём брони для двух разных пользователей
    booking1 = models.Booking(
        user_id=1,
        slot_id=slot1.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot1.start_time,
        end_time=slot1.end_time,
    )
    booking2 = models.Booking(
        user_id=2,
        slot_id=slot2.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot2.start_time,
        end_time=slot2.end_time,
    )
    test_session.add_all([booking1, booking2])
    await test_session.commit()
    
    # Получаем статистику
    statistics = await crud.get_zones_statistics(test_session)
    
    # Проверяем, что текущая загрузка = 2
    assert len(statistics) == 1
    zone_stat = statistics[0]
    assert zone_stat.current_occupancy == 2
    assert zone_stat.active_bookings == 2


@pytest.mark.asyncio
async def test_zone_statistics_excludes_cancelled(test_session):
    """
    Тест проверяет, что отменённые брони не учитываются в текущей загрузке.
    """
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    now = datetime.utcnow()
    
    # Создаём текущий слот
    slot = models.Slot(
        place_id=place.id,
        start_time=now - timedelta(minutes=30),
        end_time=now + timedelta(minutes=30),
        is_available=True  # Доступен, так как бронь отменена
    )
    test_session.add(slot)
    await test_session.flush()
    
    # Создаём отменённую бронь
    cancelled_booking = models.Booking(
        user_id=1,
        slot_id=slot.id,
        status="cancelled",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot.start_time,
        end_time=slot.end_time,
    )
    test_session.add(cancelled_booking)
    await test_session.commit()
    
    # Получаем статистику
    statistics = await crud.get_zones_statistics(test_session)
    
    # Проверяем, что текущая загрузка = 0
    assert len(statistics) == 1
    zone_stat = statistics[0]
    assert zone_stat.current_occupancy == 0
    assert zone_stat.active_bookings == 0
    assert zone_stat.cancelled_bookings == 1
