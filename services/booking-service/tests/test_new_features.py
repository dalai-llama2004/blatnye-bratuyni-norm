"""
Тесты для новых функций:
1. Продление брони с выбором времени
2. Автоматическое переоткрытие зон после истечения времени закрытия
"""
import pytest
from datetime import datetime, timedelta

import crud
import models
import schemas


@pytest.mark.asyncio
async def test_extend_booking_with_custom_hours(test_session):
    """Тест продления брони на произвольное количество часов"""
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём первый слот
    base_time = datetime.now() + timedelta(days=1)
    slot1 = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=2),
        is_available=False
    )
    test_session.add(slot1)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot1.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot1.start_time,
        end_time=slot1.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Продлеваем на 2 часа
    extended_booking = await crud.extend_booking(
        test_session, user_id=1, booking_id=booking.id, extension_hours=2
    )
    
    assert extended_booking is not None
    assert extended_booking.start_time == base_time + timedelta(hours=2)
    assert extended_booking.end_time == base_time + timedelta(hours=4)
    assert extended_booking.status == "active"


@pytest.mark.asyncio
async def test_extend_booking_exceeds_max_hours(test_session):
    """Тест что продление не может превысить MAX_BOOKING_HOURS"""
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот на 5 часов (близко к лимиту 6 часов)
    base_time = datetime.now() + timedelta(days=1)
    slot1 = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=5),
        is_available=False
    )
    test_session.add(slot1)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot1.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot1.start_time,
        end_time=slot1.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Пытаемся продлить на 2 часа (будет 7 часов, превышает лимит)
    extended_booking = await crud.extend_booking(
        test_session, user_id=1, booking_id=booking.id, extension_hours=2
    )
    
    # Должно вернуть None, так как превышен лимит
    assert extended_booking is None


@pytest.mark.asyncio
async def test_extend_booking_endpoint_with_custom_hours(test_client, test_session):
    """Тест эндпоинта продления брони с произвольным временем"""
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот
    base_time = datetime.now() + timedelta(days=1)
    slot1 = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=1),
        is_available=False
    )
    test_session.add(slot1)
    await test_session.flush()
    
    # Создаём бронь
    booking = models.Booking(
        user_id=1,
        slot_id=slot1.id,
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=slot1.start_time,
        end_time=slot1.end_time,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Продлеваем на 3 часа через API
    response = await test_client.post(
        f"/bookings/{booking.id}/extend",
        json={"extension_hours": 3},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "active"
    # Проверяем, что время правильное
    start = datetime.fromisoformat(data['start_time'].replace('Z', '+00:00'))
    end = datetime.fromisoformat(data['end_time'].replace('Z', '+00:00'))
    duration = (end - start).total_seconds() / 3600
    assert duration == 3.0


@pytest.mark.asyncio
async def test_close_zone_saves_closure_end_time(test_session):
    """Тест что при закрытии зоны сохраняется время окончания закрытия"""
    # Создаём зону
    zone = models.Zone(name="Тестовая зона", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.commit()
    
    # Закрываем зону
    now = datetime.utcnow()
    close_data = schemas.ZoneCloseRequest(
        reason="Плановая уборка",
        from_time=now,
        to_time=now + timedelta(hours=2)
    )
    
    await crud.close_zone(test_session, zone.id, close_data)
    
    # Проверяем, что зона закрыта и сохранено время окончания
    await test_session.refresh(zone)
    assert zone.is_active is False
    assert zone.closure_reason == "Плановая уборка"
    assert zone.closure_end_time == now + timedelta(hours=2)


@pytest.mark.asyncio
async def test_reopen_closed_zones(test_session):
    """Тест автоматического переоткрытия зон после истечения времени закрытия"""
    # Создаём две зоны
    now = datetime.utcnow()
    
    # Зона 1: закрыта, время истекло (должна переоткрыться)
    zone1 = models.Zone(
        name="Зона 1",
        address="Адрес 1",
        is_active=False,
        closure_reason="Уборка завершена",
        closure_end_time=now - timedelta(hours=1)  # Час назад
    )
    test_session.add(zone1)
    
    # Зона 2: закрыта, время не истекло (должна остаться закрытой)
    zone2 = models.Zone(
        name="Зона 2",
        address="Адрес 2",
        is_active=False,
        closure_reason="Ремонт",
        closure_end_time=now + timedelta(hours=1)  # Через час
    )
    test_session.add(zone2)
    
    # Зона 3: активна (не должна измениться)
    zone3 = models.Zone(
        name="Зона 3",
        address="Адрес 3",
        is_active=True
    )
    test_session.add(zone3)
    
    await test_session.commit()
    
    # Вызываем функцию переоткрытия
    reopened = await crud.reopen_closed_zones(test_session)
    
    # Проверяем результаты
    assert len(reopened) == 1
    assert reopened[0].id == zone1.id
    
    # Проверяем состояния зон
    await test_session.refresh(zone1)
    await test_session.refresh(zone2)
    await test_session.refresh(zone3)
    
    assert zone1.is_active is True
    assert zone1.closure_reason is None
    assert zone1.closure_end_time is None
    
    assert zone2.is_active is False  # Время не истекло
    assert zone2.closure_reason == "Ремонт"
    
    assert zone3.is_active is True  # Не изменилась


@pytest.mark.asyncio
async def test_get_all_zones_includes_closed(test_session):
    """Тест что get_all_zones возвращает все зоны, включая закрытые"""
    # Создаём активную зону
    zone1 = models.Zone(name="Активная зона", address="Адрес 1", is_active=True)
    test_session.add(zone1)
    
    # Создаём закрытую зону
    zone2 = models.Zone(
        name="Закрытая зона",
        address="Адрес 2",
        is_active=False,
        closure_reason="Ремонт"
    )
    test_session.add(zone2)
    
    await test_session.commit()
    
    # Получаем все зоны
    all_zones = await crud.get_all_zones(test_session)
    
    assert len(all_zones) == 2
    
    # Получаем только активные зоны
    active_zones = await crud.get_zones(test_session)
    
    assert len(active_zones) == 1
    assert active_zones[0].id == zone1.id


@pytest.mark.asyncio
async def test_admin_get_all_zones_endpoint(test_client, test_session):
    """Тест эндпоинта получения всех зон для админа"""
    # Создаём активную и закрытую зону
    zone1 = models.Zone(name="Активная зона", address="Адрес 1", is_active=True)
    zone2 = models.Zone(
        name="Закрытая зона",
        address="Адрес 2",
        is_active=False,
        closure_reason="Ремонт"
    )
    test_session.add_all([zone1, zone2])
    await test_session.commit()
    
    # Запрашиваем все зоны как админ
    response = await test_client.get(
        "/admin/zones/all",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    
    # Проверяем, что закрытая зона присутствует
    closed_zone = [z for z in data if z["name"] == "Закрытая зона"][0]
    assert closed_zone["is_active"] is False
    assert closed_zone["closure_reason"] == "Ремонт"


@pytest.mark.asyncio
async def test_admin_reopen_expired_zones_endpoint(test_client, test_session):
    """Тест эндпоинта переоткрытия зон с истекшим временем закрытия"""
    now = datetime.utcnow()
    
    # Создаём закрытую зону с истекшим временем
    zone = models.Zone(
        name="Закрытая зона",
        address="Адрес 1",
        is_active=False,
        closure_reason="Уборка",
        closure_end_time=now - timedelta(hours=1)
    )
    test_session.add(zone)
    await test_session.commit()
    
    # Вызываем эндпоинт переоткрытия
    response = await test_client.post(
        "/admin/zones/reopen-expired",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["id"] == zone.id
    assert data[0]["is_active"] is True
    assert data[0]["closure_reason"] is None
