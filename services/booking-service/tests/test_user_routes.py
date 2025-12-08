import pytest
from datetime import datetime, timedelta

import models


@pytest.mark.asyncio
async def test_list_zones_endpoint(test_client, test_session):
    """Test GET /zones endpoint"""
    # Create test zones
    zone1 = models.Zone(name="Zone 1", address="Addr 1", is_active=True)
    zone2 = models.Zone(name="Zone 2", address="Addr 2", is_active=True)
    test_session.add_all([zone1, zone2])
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.get("/zones")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["name"] == "Zone 1"
    assert data[1]["name"] == "Zone 2"


@pytest.mark.asyncio
async def test_list_places_in_zone_endpoint(test_client, test_session):
    """Test GET /zones/{zone_id}/places endpoint"""
    # Create zone and places
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place1 = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    place2 = models.Place(zone_id=zone.id, name="Place 2", is_active=True)
    test_session.add_all([place1, place2])
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.get(f"/zones/{zone.id}/places")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["name"] == "Place 1"


@pytest.mark.asyncio
async def test_list_slots_endpoint(test_client, test_session):
    """Test GET /places/{place_id}/slots endpoint"""
    # Create zone, place, and slots
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    target_date = datetime.now().date()
    start_time = datetime.combine(target_date, datetime.min.time()) + timedelta(hours=10)
    end_time = start_time + timedelta(hours=1)
    
    slot = models.Slot(
        place_id=place.id,
        start_time=start_time,
        end_time=end_time,
        is_available=True
    )
    test_session.add(slot)
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.get(f"/places/{place.id}/slots?date={target_date.isoformat()}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["is_available"] is True


@pytest.mark.asyncio
async def test_create_booking_endpoint(test_client, test_session):
    """Test POST /bookings endpoint"""
    # Setup
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    start_time = datetime.now() + timedelta(days=1)
    end_time = start_time + timedelta(hours=1)
    slot = models.Slot(
        place_id=place.id,
        start_time=start_time,
        end_time=end_time,
        is_available=True
    )
    test_session.add(slot)
    await test_session.commit()
    
    # Test endpoint with authentication headers
    response = await test_client.post(
        "/bookings",
        json={"slot_id": slot.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 201
    data = response.json()
    assert data["user_id"] == 1
    assert data["slot_id"] == slot.id
    assert data["status"] == "active"


@pytest.mark.asyncio
async def test_cancel_booking_endpoint(test_client, test_session):
    """Test POST /bookings/cancel endpoint"""
    # Setup
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    start_time = datetime.now() + timedelta(days=1)
    end_time = start_time + timedelta(hours=1)
    slot = models.Slot(
        place_id=place.id,
        start_time=start_time,
        end_time=end_time,
        is_available=False
    )
    test_session.add(slot)
    await test_session.flush()
    
    booking = models.Booking(user_id=1, slot_id=slot.id, status="active")
    test_session.add(booking)
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.post(
        "/bookings/cancel",
        json={"booking_id": booking.id},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "cancelled"


@pytest.mark.asyncio
async def test_booking_history_endpoint(test_client, test_session):
    """Test GET /bookings/history endpoint"""
    # Setup
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Create bookings
    for i in range(2):
        start_time = datetime.now() + timedelta(days=i+1)
        end_time = start_time + timedelta(hours=1)
        slot = models.Slot(
            place_id=place.id,
            start_time=start_time,
            end_time=end_time,
            is_available=False
        )
        test_session.add(slot)
        await test_session.flush()
        
        booking = models.Booking(user_id=1, slot_id=slot.id, status="active")
        test_session.add(booking)
    
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.get(
        "/bookings/history",
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_extend_booking_endpoint(test_client, test_session):
    """Test POST /bookings/{booking_id}/extend endpoint"""
    # Setup
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Create consecutive slots
    start_time_1 = datetime.now() + timedelta(days=1)
    end_time_1 = start_time_1 + timedelta(hours=1)
    slot_1 = models.Slot(
        place_id=place.id,
        start_time=start_time_1,
        end_time=end_time_1,
        is_available=False
    )
    test_session.add(slot_1)
    
    start_time_2 = end_time_1
    end_time_2 = start_time_2 + timedelta(hours=1)
    slot_2 = models.Slot(
        place_id=place.id,
        start_time=start_time_2,
        end_time=end_time_2,
        is_available=True
    )
    test_session.add(slot_2)
    await test_session.flush()
    
    booking = models.Booking(
        user_id=1, 
        slot_id=slot_1.id, 
        status="active",
        zone_name=zone.name,
        zone_address=zone.address,
        start_time=start_time_1,
        end_time=end_time_1,
    )
    test_session.add(booking)
    await test_session.commit()
    
    # Test endpoint
    response = await test_client.post(
        f"/bookings/{booking.id}/extend",
        json={"extend_hours": 1, "extend_minutes": 0},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["slot_id"] == slot_2.id
    assert data["status"] == "active"


@pytest.mark.asyncio
async def test_extend_booking_error_messages(test_client, test_session):
    """Тест проверяет, что при ошибке продления брони возвращается понятное сообщение"""
    from config import settings
    
    # Создаём зону и место
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Создаём слот на максимально допустимое время
    start_time = datetime.now() + timedelta(days=1)
    end_time = start_time + timedelta(hours=settings.MAX_BOOKING_HOURS)
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
    await test_session.commit()
    
    # Пытаемся продлить бронь (должно превысить лимит)
    response = await test_client.post(
        f"/bookings/{booking.id}/extend",
        json={"extend_hours": 1, "extend_minutes": 0},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    
    # Проверяем, что получили ошибку 400 с понятным сообщением
    assert response.status_code == 400
    error_data = response.json()
    assert "detail" in error_data
    # Проверяем, что сообщение содержит полезную информацию, а не [object Object]
    assert "максимальный лимит" in error_data["detail"].lower()
    assert "часов" in error_data["detail"].lower()


@pytest.mark.asyncio
async def test_extend_booking_not_found_error(test_client, test_session):
    """Тест проверяет сообщение об ошибке для несуществующего бронирования"""
    response = await test_client.post(
        "/bookings/99999/extend",
        json={"extend_hours": 1, "extend_minutes": 0},
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    
    assert response.status_code == 400
    error_data = response.json()
    assert "detail" in error_data
    assert "не найдено" in error_data["detail"].lower()
