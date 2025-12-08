import pytest
from datetime import datetime, timedelta, date

import models


@pytest.mark.asyncio
async def test_create_booking_by_time_with_capacity_check(test_client, test_session):
    """Тест создания брони с проверкой вместимости зоны"""
    # Создаем зону с 2 местами
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    place2 = models.Place(zone_id=zone.id, name="Place 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.commit()
    
    # Создаем первую бронь - должна пройти успешно
    target_date = date.today()
    response1 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Создаем вторую бронь - должна пройти успешно
    response2 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response2.status_code == 201
    
    # Попытка создать третью бронь - должна быть отклонена (зона переполнена)
    response3 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "3", "X-User-Role": "user"}
    )
    assert response3.status_code == 409


@pytest.mark.asyncio
async def test_create_booking_by_time_partial_overlap(test_client, test_session):
    """Тест создания брони с частичным пересечением существующих броней"""
    # Создаем зону с 1 местом
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.commit()
    
    # Создаем бронь с 10:00 до 11:00
    target_date = date.today()
    response1 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 11,
            "end_minute": 0,
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Попытка создать бронь с 10:30 до 11:30 - должна быть отклонена
    response2 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 30,
            "end_hour": 11,
            "end_minute": 30,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response2.status_code == 409
    
    # Создание брони с 11:00 до 12:00 - должно пройти успешно (нет пересечения)
    response3 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 11,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response3.status_code == 201


@pytest.mark.asyncio
async def test_create_booking_by_slot_with_capacity_check(test_client, test_session):
    """Тест создания брони по слоту с проверкой вместимости зоны"""
    # Создаем зону с 1 местом
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаем первый слот и бронь через API
    target_date = date.today()
    response1 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Попытка создать вторую бронь на то же время - должна быть отклонена (зона переполнена)
    response2 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response2.status_code == 409


@pytest.mark.asyncio
async def test_capacity_check_with_cancelled_bookings(test_client, test_session):
    """Тест, что отмененные брони не учитываются при проверке вместимости"""
    # Создаем зону с 1 местом
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаем бронь через API
    target_date = date.today()
    response1 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    booking_id = response1.json()["id"]
    
    # Отменяем бронь
    response2 = await test_client.post(
        "/bookings/cancel",
        json={"booking_id": booking_id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response2.status_code == 200
    
    # Создание новой брони на то же время - должно пройти успешно
    response3 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response3.status_code == 201


@pytest.mark.asyncio
async def test_capacity_check_with_multiple_zones(test_client, test_session):
    """Тест, что проверка вместимости работает независимо для разных зон"""
    # Создаем две зоны, каждая с 1 местом
    zone1 = models.Zone(name="Zone 1", address="Addr 1", is_active=True)
    zone2 = models.Zone(name="Zone 2", address="Addr 2", is_active=True)
    test_session.add_all([zone1, zone2])
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone1.id, name="Place 1", is_active=True)
    place2 = models.Place(zone_id=zone2.id, name="Place 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.commit()
    
    target_date = date.today()
    
    # Создаем бронь в зоне 1
    response1 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone1.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Создание брони в зоне 2 на то же время - должно пройти успешно
    response2 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone2.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "2", "X-User-Role": "user"}
    )
    assert response2.status_code == 201
    
    # Попытка создать еще одну бронь в зоне 1 - должна быть отклонена
    response3 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone1.id,
            "date": target_date.isoformat(),
            "start_hour": 10,
            "start_minute": 0,
            "end_hour": 12,
            "end_minute": 0,
        },
        headers={"X-User-Id": "3", "X-User-Role": "user"}
    )
    assert response3.status_code == 409
