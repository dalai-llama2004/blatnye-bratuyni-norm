"""
Тест для проверки отображения времени в истории броней.

Проверяет, что в истории броней у всех пользователей показывается время каждой брони (от и до).
"""
import pytest
from datetime import datetime, timedelta

import models


@pytest.mark.asyncio
async def test_booking_history_shows_time_range(test_client, test_session):
    """
    Тест проверяет, что история броней возвращает start_time и end_time для каждой брони.
    """
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Тестовый адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём несколько слотов с разным временем
    base_time = datetime.now() + timedelta(days=1)
    bookings_data = []
    
    for i in range(3):
        start_time = base_time + timedelta(hours=i*2)
        end_time = start_time + timedelta(hours=1)
        
        slot = models.Slot(
            place_id=place.id,
            start_time=start_time,
            end_time=end_time,
            is_available=False
        )
        test_session.add(slot)
        await test_session.flush()
        
        booking = models.Booking(
            user_id=1,
            slot_id=slot.id,
            status="active",
            zone_name=zone.name,
            zone_address=zone.address,
            start_time=start_time,
            end_time=end_time,
        )
        test_session.add(booking)
        bookings_data.append({
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        })
    
    await test_session.commit()
    
    # Запрашиваем историю броней
    response = await test_client.get(
        "/bookings/history",
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 200
    data = response.json()
    
    # Проверяем, что все брони имеют start_time и end_time
    assert len(data) == 3
    for booking in data:
        assert 'start_time' in booking
        assert 'end_time' in booking
        assert booking['start_time'] is not None
        assert booking['end_time'] is not None
        
        # Проверяем формат времени (должен быть ISO 8601)
        start = datetime.fromisoformat(booking['start_time'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(booking['end_time'].replace('Z', '+00:00'))
        
        # Проверяем, что end_time больше start_time
        assert end > start


@pytest.mark.asyncio
async def test_extended_booking_shows_time_range(test_client, test_session):
    """
    Тест проверяет, что продлённая бронь также имеет корректные start_time и end_time.
    """
    # Создаём зону и место
    zone = models.Zone(name="Тестовая зона", address="Тестовый адрес", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём два последовательных слота
    base_time = datetime.now() + timedelta(days=1)
    
    slot1 = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=1),
        is_available=False
    )
    slot2 = models.Slot(
        place_id=place.id,
        start_time=base_time + timedelta(hours=1),
        end_time=base_time + timedelta(hours=2),
        is_available=True
    )
    test_session.add_all([slot1, slot2])
    await test_session.flush()
    
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
    
    # Продлеваем бронь
    response = await test_client.post(
        f"/bookings/{booking.id}/extend",
        json={"extend_hours": 1, "extend_minutes": 0},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 200
    extended_booking = response.json()
    
    # Проверяем, что продлённая бронь имеет start_time и end_time
    assert 'start_time' in extended_booking
    assert 'end_time' in extended_booking
    assert extended_booking['start_time'] is not None
    assert extended_booking['end_time'] is not None
    
    # Проверяем, что время совпадает со вторым слотом
    start = datetime.fromisoformat(extended_booking['start_time'].replace('Z', '+00:00'))
    end = datetime.fromisoformat(extended_booking['end_time'].replace('Z', '+00:00'))
    
    # Время должно быть примерно равно slot2 (с учётом временных зон)
    assert abs((start - slot2.start_time).total_seconds()) < 60
    assert abs((end - slot2.end_time).total_seconds()) < 60
    
    # Проверяем, что в истории обе брони имеют правильное время
    history_response = await test_client.get(
        "/bookings/history",
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert history_response.status_code == 200
    history_data = history_response.json()
    
    assert len(history_data) == 2
    for booking_item in history_data:
        assert 'start_time' in booking_item
        assert 'end_time' in booking_item
        assert booking_item['start_time'] is not None
        assert booking_item['end_time'] is not None
