"""
Тесты для проверки конфликтов бронирований.

Проверяют, что пользователь не может создать:
1. Несколько броней на одно и то же время в разных зонах
2. Несколько пересекающихся по времени броней в одной зоне
"""
import pytest
from datetime import datetime, timedelta

import models


@pytest.mark.asyncio
async def test_prevent_multiple_bookings_same_time_different_zones(test_client, test_session):
    """
    Тест проверяет, что пользователь не может создать две брони на одно время в разных зонах.
    """
    # Создаём две зоны
    zone1 = models.Zone(name="Зона 1", address="Адрес 1", is_active=True)
    zone2 = models.Zone(name="Зона 2", address="Адрес 2", is_active=True)
    test_session.add_all([zone1, zone2])
    await test_session.flush()
    
    # Создаём места в каждой зоне
    place1 = models.Place(zone_id=zone1.id, name="Место 1", is_active=True)
    place2 = models.Place(zone_id=zone2.id, name="Место 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.flush()
    
    # Создаём слоты с одинаковым временем в обеих зонах
    start_time = datetime.now() + timedelta(days=1)
    end_time = start_time + timedelta(hours=2)
    
    slot1 = models.Slot(
        place_id=place1.id,
        start_time=start_time,
        end_time=end_time,
        is_available=True
    )
    slot2 = models.Slot(
        place_id=place2.id,
        start_time=start_time,
        end_time=end_time,
        is_available=True
    )
    test_session.add_all([slot1, slot2])
    await test_session.commit()
    
    # Пользователь создаёт первую бронь в зоне 1
    response1 = await test_client.post(
        "/bookings",
        json={"slot_id": slot1.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Пользователь пытается создать вторую бронь в зоне 2 на то же время
    response2 = await test_client.post(
        "/bookings",
        json={"slot_id": slot2.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    # Должна вернуться ошибка 409 (конфликт)
    assert response2.status_code == 409


@pytest.mark.asyncio
async def test_prevent_overlapping_bookings_same_zone(test_client, test_session):
    """
    Тест проверяет, что пользователь не может создать пересекающиеся по времени брони в одной зоне.
    """
    # Создаём зону и места
    zone = models.Zone(name="Зона 1", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    place2 = models.Place(zone_id=zone.id, name="Место 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.flush()
    
    # Создаём слоты с пересекающимся временем
    base_time = datetime.now() + timedelta(days=1)
    
    # Слот 1: 10:00 - 12:00
    slot1 = models.Slot(
        place_id=place1.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=2),
        is_available=True
    )
    # Слот 2: 11:00 - 13:00 (пересекается с первым)
    slot2 = models.Slot(
        place_id=place2.id,
        start_time=base_time + timedelta(hours=1),
        end_time=base_time + timedelta(hours=3),
        is_available=True
    )
    test_session.add_all([slot1, slot2])
    await test_session.commit()
    
    # Пользователь создаёт первую бронь
    response1 = await test_client.post(
        "/bookings",
        json={"slot_id": slot1.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Пользователь пытается создать вторую бронь с пересекающимся временем
    response2 = await test_client.post(
        "/bookings",
        json={"slot_id": slot2.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    # Должна вернуться ошибка 409 (конфликт)
    assert response2.status_code == 409


@pytest.mark.asyncio
async def test_allow_sequential_bookings_same_zone(test_client, test_session):
    """
    Тест проверяет, что пользователь МОЖЕТ создать последовательные брони (без пересечения) в одной зоне.
    """
    # Создаём зону и места
    zone = models.Zone(name="Зона 1", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    place2 = models.Place(zone_id=zone.id, name="Место 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.flush()
    
    # Создаём слоты с непересекающимся временем
    base_time = datetime.now() + timedelta(days=1)
    
    # Слот 1: 10:00 - 12:00
    slot1 = models.Slot(
        place_id=place1.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=2),
        is_available=True
    )
    # Слот 2: 12:00 - 14:00 (начинается когда первый заканчивается)
    slot2 = models.Slot(
        place_id=place2.id,
        start_time=base_time + timedelta(hours=2),
        end_time=base_time + timedelta(hours=4),
        is_available=True
    )
    test_session.add_all([slot1, slot2])
    await test_session.commit()
    
    # Пользователь создаёт первую бронь
    response1 = await test_client.post(
        "/bookings",
        json={"slot_id": slot1.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Пользователь создаёт вторую бронь без пересечения
    response2 = await test_client.post(
        "/bookings",
        json={"slot_id": slot2.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    # Должна быть успешная бронь
    assert response2.status_code == 201


@pytest.mark.asyncio
async def test_prevent_overlapping_bookings_by_time_range(test_client, test_session):
    """
    Тест проверяет, что при создании брони через /bookings/by-time
    также проверяется отсутствие пересечений с другими бронями пользователя.
    """
    # Создаём зону и место
    zone = models.Zone(name="Зона 1", address="Адрес 1", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Место 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот для первой брони
    base_time = datetime.now() + timedelta(days=1)
    slot1 = models.Slot(
        place_id=place.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=2),
        is_available=True
    )
    test_session.add(slot1)
    await test_session.commit()
    
    # Пользователь создаёт первую бронь
    response1 = await test_client.post(
        "/bookings",
        json={"slot_id": slot1.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response1.status_code == 201
    
    # Пользователь пытается создать вторую бронь через by-time с пересечением
    target_date = (datetime.now() + timedelta(days=1)).date()
    response2 = await test_client.post(
        "/bookings/by-time",
        json={
            "zone_id": zone.id,
            "date": target_date.isoformat(),
            "start_hour": base_time.hour + 1,  # На час позже начала первой брони
            "start_minute": 0,
            "end_hour": base_time.hour + 3,
            "end_minute": 0
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    # Должна вернуться ошибка 409 (конфликт)
    assert response2.status_code == 409


@pytest.mark.asyncio
async def test_extend_booking_with_time_conflict(test_client, test_session):
    """
    Тест проверяет, что продление брони также проверяет конфликты с другими бронями пользователя.
    """
    # Создаём две зоны
    zone1 = models.Zone(name="Зона 1", address="Адрес 1", is_active=True)
    zone2 = models.Zone(name="Зона 2", address="Адрес 2", is_active=True)
    test_session.add_all([zone1, zone2])
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone1.id, name="Место 1", is_active=True)
    place2 = models.Place(zone_id=zone2.id, name="Место 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.flush()
    
    base_time = datetime.now() + timedelta(days=1)
    
    # В зоне 1: два последовательных слота
    slot1_1 = models.Slot(
        place_id=place1.id,
        start_time=base_time,
        end_time=base_time + timedelta(hours=1),
        is_available=False
    )
    slot1_2 = models.Slot(
        place_id=place1.id,
        start_time=base_time + timedelta(hours=1),
        end_time=base_time + timedelta(hours=2),
        is_available=True
    )
    test_session.add_all([slot1_1, slot1_2])
    await test_session.flush()
    
    # В зоне 2: слот на время второго слота зоны 1
    slot2 = models.Slot(
        place_id=place2.id,
        start_time=base_time + timedelta(hours=1),
        end_time=base_time + timedelta(hours=2),
        is_available=False
    )
    test_session.add(slot2)
    await test_session.flush()
    
    # Создаём бронь в зоне 1
    booking1 = models.Booking(
        user_id=1,
        slot_id=slot1_1.id,
        status="active",
        zone_name=zone1.name,
        zone_address=zone1.address,
        start_time=slot1_1.start_time,
        end_time=slot1_1.end_time,
    )
    test_session.add(booking1)
    
    # Создаём бронь в зоне 2 на время второго слота
    booking2 = models.Booking(
        user_id=1,
        slot_id=slot2.id,
        status="active",
        zone_name=zone2.name,
        zone_address=zone2.address,
        start_time=slot2.start_time,
        end_time=slot2.end_time,
    )
    test_session.add(booking2)
    await test_session.commit()
    
    # Пытаемся продлить первую бронь
    response = await test_client.post(
        f"/bookings/{booking1.id}/extend",
        json={"extend_hours": 1, "extend_minutes": 0},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    # Должна вернуться ошибка 400, так как у пользователя уже есть бронь на это время
    assert response.status_code == 400
