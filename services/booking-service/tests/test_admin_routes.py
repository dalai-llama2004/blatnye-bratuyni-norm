import pytest
from datetime import datetime, timedelta

import models


@pytest.mark.asyncio
async def test_create_zone_admin_endpoint(test_client, test_session):
    """Test POST /admin/zones endpoint"""
    response = await test_client.post(
        "/admin/zones",
        json={
            "name": "New Zone",
            "address": "New Address",
            "is_active": True,
            "places_count": 10
        },
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "New Zone"
    assert data["address"] == "New Address"
    assert data["is_active"] is True


@pytest.mark.asyncio
async def test_create_zone_non_admin_forbidden(test_client, test_session):
    """Test that non-admin users cannot create zones"""
    response = await test_client.post(
        "/admin/zones",
        json={
            "name": "New Zone",
            "address": "New Address",
            "is_active": True,
            "places_count": 10
        },
        headers={"X-User-Id": "1", "X-User-Role": "user"}
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_update_zone_admin_endpoint(test_client, test_session):
    """Test PATCH /admin/zones/{zone_id} endpoint"""
    # Create zone first
    zone = models.Zone(name="Old Name", address="Old Address", is_active=True)
    test_session.add(zone)
    await test_session.commit()
    
    # Update zone
    response = await test_client.patch(
        f"/admin/zones/{zone.id}",
        json={
            "name": "Updated Name",
            "is_active": False
        },
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Name"
    assert data["address"] == "Old Address"  # Unchanged
    assert data["is_active"] is False


@pytest.mark.asyncio
async def test_delete_zone_admin_endpoint(test_client, test_session):
    """Test DELETE /admin/zones/{zone_id} endpoint"""
    # Create zone first
    zone = models.Zone(name="Test Zone", address="Test Address", is_active=True)
    test_session.add(zone)
    await test_session.commit()
    
    # Delete zone
    response = await test_client.delete(
        f"/admin/zones/{zone.id}",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_delete_nonexistent_zone(test_client, test_session):
    """Test deleting a zone that doesn't exist"""
    response = await test_client.delete(
        "/admin/zones/99999",
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_close_zone_admin_endpoint(test_client, test_session):
    """Test POST /admin/zones/{zone_id}/close endpoint"""
    # Create zone, place, slot, and active booking
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.flush()
    
    place = models.Place(zone_id=zone.id, name="Place 1", is_active=True)
    test_session.add(place)
    await test_session.flush()
    
    # Create future slot
    start_time = datetime.now() + timedelta(days=1)
    end_time = start_time + timedelta(hours=2)
    slot = models.Slot(
        place_id=place.id,
        start_time=start_time,
        end_time=end_time,
        is_available=False
    )
    test_session.add(slot)
    await test_session.flush()
    
    # Create active booking
    booking = models.Booking(user_id=1, slot_id=slot.id, status="active")
    test_session.add(booking)
    await test_session.commit()
    
    # Close zone
    from_time = datetime.now()
    to_time = datetime.now() + timedelta(days=2)
    
    response = await test_client.post(
        f"/admin/zones/{zone.id}/close",
        json={
            "reason": "Maintenance",
            "from_time": from_time.isoformat(),
            "to_time": to_time.isoformat()
        },
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    
    # Should return affected bookings
    assert len(data) == 1
    assert data[0]["status"] == "cancelled"
    assert data[0]["id"] == booking.id


@pytest.mark.asyncio
async def test_close_zone_no_affected_bookings(test_client, test_session):
    """Test closing zone with no active bookings"""
    # Create zone without bookings
    zone = models.Zone(name="Test Zone", address="Test Addr", is_active=True)
    test_session.add(zone)
    await test_session.commit()
    
    # Close zone
    from_time = datetime.now()
    to_time = datetime.now() + timedelta(days=2)
    
    response = await test_client.post(
        f"/admin/zones/{zone.id}/close",
        json={
            "reason": "Maintenance",
            "from_time": from_time.isoformat(),
            "to_time": to_time.isoformat()
        },
        headers={"X-User-Id": "1", "X-User-Role": "admin"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0  # No affected bookings
