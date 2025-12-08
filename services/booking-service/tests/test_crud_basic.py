import pytest
from datetime import datetime, timedelta

import crud
import models
import schemas


@pytest.mark.asyncio
async def test_create_and_get_zones(test_session):
    """Test creating and retrieving zones"""
    # Create a zone
    zone_data = schemas.ZoneCreate(
        name="Test Zone",
        address="Test Address",
        is_active=True,
        places_count=5
    )
    zone = await crud.create_zone(test_session, zone_data)
    
    assert zone.id is not None
    assert zone.name == "Test Zone"
    assert zone.address == "Test Address"
    assert zone.is_active is True
    
    # Get all zones
    zones = await crud.get_zones(test_session)
    assert len(zones) == 1
    assert zones[0].id == zone.id


@pytest.mark.asyncio
async def test_update_zone(test_session):
    """Test updating a zone"""
    # Create a zone
    zone_data = schemas.ZoneCreate(name="Old Name", address="Old Address", is_active=True, places_count=3)
    zone = await crud.create_zone(test_session, zone_data)
    
    # Update the zone
    update_data = schemas.ZoneUpdate(name="New Name", is_active=False)
    updated_zone = await crud.update_zone(test_session, zone.id, update_data)
    
    assert updated_zone.name == "New Name"
    assert updated_zone.address == "Old Address"  # Should remain unchanged
    assert updated_zone.is_active is False


@pytest.mark.asyncio
async def test_delete_zone(test_session):
    """Test deleting a zone"""
    # Create a zone
    zone_data = schemas.ZoneCreate(name="Test Zone", address="Test Address", is_active=True, places_count=2)
    zone = await crud.create_zone(test_session, zone_data)
    
    # Delete the zone
    result = await crud.delete_zone(test_session, zone.id)
    assert result is True
    
    # Verify it's deleted
    zones = await crud.get_zones(test_session)
    assert len(zones) == 0


@pytest.mark.asyncio
async def test_create_booking(test_session):
    """Test creating a booking"""
    # Setup: Create zone, place, and slot
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
    await test_session.flush()
    
    # Create booking
    booking_data = schemas.BookingCreate(slot_id=slot.id)
    booking = await crud.create_booking(test_session, user_id=1, booking_in=booking_data)
    
    assert booking is not None
    assert booking.user_id == 1
    assert booking.slot_id == slot.id
    assert booking.status == "active"
    
    # Verify slot is now unavailable
    await test_session.refresh(slot)
    assert slot.is_available is False


@pytest.mark.asyncio
async def test_cancel_booking(test_session):
    """Test canceling a booking"""
    # Setup: Create zone, place, slot, and booking
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
    await test_session.flush()
    
    # Cancel the booking
    cancelled = await crud.cancel_booking(test_session, user_id=1, booking_id=booking.id)
    
    assert cancelled is not None
    assert cancelled.status == "cancelled"
    
    # Verify slot is available again
    await test_session.refresh(slot)
    assert slot.is_available is True


@pytest.mark.asyncio
async def test_extend_booking(test_session):
    """Test extending a booking"""
    # Setup: Create zone, place, two consecutive slots, and a booking
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
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
    await test_session.flush()
    
    # Extend the booking (по умолчанию на 1 час)
    new_booking = await crud.extend_booking(test_session, user_id=1, booking_id=booking.id)
    
    assert new_booking is not None
    assert new_booking.slot_id == slot_2.id
    assert new_booking.status == "active"
    
    # Verify second slot is now unavailable
    await test_session.refresh(slot_2)
    assert slot_2.is_available is False


@pytest.mark.asyncio
async def test_get_booking_history(test_session):
    """Test retrieving booking history"""
    # Setup: Create zone, place, slot, and multiple bookings
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Create multiple slots and bookings
    for i in range(3):
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
        
        status = "active" if i < 2 else "cancelled"
        booking = models.Booking(user_id=1, slot_id=slot.id, status=status)
        test_session.add(booking)
    
    await test_session.flush()
    
    # Get all bookings
    all_bookings = await crud.get_booking_history(test_session, user_id=1)
    assert len(all_bookings) == 3
    
    # Filter by status
    filters = schemas.BookingHistoryFilters(status="active")
    active_bookings = await crud.get_booking_history(test_session, user_id=1, filters=filters)
    assert len(active_bookings) == 2
