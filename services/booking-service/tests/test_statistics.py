import pytest
from datetime import datetime, timedelta

import models


@pytest.mark.asyncio
async def test_get_global_statistics_empty(test_client, test_session):
    """Тест получения статистики при отсутствии броней"""
    response = await test_client.get(
        "/admin/statistics",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_active_bookings"] == 0
    assert data["total_cancelled_bookings"] == 0
    assert data["users_in_coworking_now"] == 0


@pytest.mark.asyncio
async def test_get_global_statistics_with_bookings(test_client, test_session):
    """Тест получения статистики с разными бронированиями"""
    # Создаем зону и место
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаем активное бронирование "прямо сейчас"
    now = datetime.utcnow()
    slot1 = models.Slot(
        place_id=place.id,
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(hours=1),
        is_available=False
    )
    test_session.add(slot1)
    await test_session.flush()
    
    booking1 = models.Booking(
        user_id=1,
        slot_id=slot1.id,
        status="active",
        start_time=slot1.start_time,
        end_time=slot1.end_time,
    )
    test_session.add(booking1)
    
    # Создаем активное бронирование в будущем
    slot2 = models.Slot(
        place_id=place.id,
        start_time=now + timedelta(days=1),
        end_time=now + timedelta(days=1, hours=2),
        is_available=False
    )
    test_session.add(slot2)
    await test_session.flush()
    
    booking2 = models.Booking(
        user_id=2,
        slot_id=slot2.id,
        status="active",
        start_time=slot2.start_time,
        end_time=slot2.end_time,
    )
    test_session.add(booking2)
    
    # Создаем отмененное бронирование
    slot3 = models.Slot(
        place_id=place.id,
        start_time=now + timedelta(days=2),
        end_time=now + timedelta(days=2, hours=2),
        is_available=True
    )
    test_session.add(slot3)
    await test_session.flush()
    
    booking3 = models.Booking(
        user_id=3,
        slot_id=slot3.id,
        status="cancelled",
        start_time=slot3.start_time,
        end_time=slot3.end_time,
    )
    test_session.add(booking3)
    
    await test_session.commit()
    
    # Проверяем статистику
    response = await test_client.get(
        "/admin/statistics",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_active_bookings"] == 2
    assert data["total_cancelled_bookings"] == 1
    assert data["users_in_coworking_now"] == 1  # Только user_id=1 находится сейчас в коворкинге


@pytest.mark.asyncio
async def test_get_global_statistics_multiple_users_now(test_client, test_session):
    """Тест подсчета нескольких пользователей в коворкинге прямо сейчас"""
    # Создаем зону и несколько мест
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    places = []
    for i in range(3):
        place = models.Place(zone_id=zone.id, name=f"Place {i+1}", is_active=True)
        test_session.add(place)
        await test_session.flush()
        places.append(place)
    
    now = datetime.utcnow()
    
    # Создаем активные бронирования для нескольких пользователей на разных местах
    for idx, user_id in enumerate([1, 2, 3]):
        slot = models.Slot(
            place_id=places[idx].id,  # Используем разные места
            start_time=now - timedelta(minutes=30),
            end_time=now + timedelta(minutes=30),
            is_available=False
        )
        test_session.add(slot)
        await test_session.flush()
        
        booking = models.Booking(
            user_id=user_id,
            slot_id=slot.id,
            status="active",
            start_time=slot.start_time,
            end_time=slot.end_time,
        )
        test_session.add(booking)
    
    await test_session.commit()
    
    # Проверяем статистику
    response = await test_client.get(
        "/admin/statistics",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total_active_bookings"] == 3
    assert data["users_in_coworking_now"] == 3


@pytest.mark.asyncio
async def test_get_global_statistics_non_admin_forbidden(test_client, test_session):
    """Тест, что обычный пользователь не может получить статистику"""
    response = await test_client.get(
        "/admin/statistics",
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 403
