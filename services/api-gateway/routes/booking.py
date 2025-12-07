from fastapi import APIRouter, Request, Depends, Response
import requests
from config import BOOKING_SERVICE_URL
from auth import get_current_user

router = APIRouter()

@router.get("/zones")
def get_zones():
    resp = requests.get(f"{BOOKING_SERVICE_URL}/zones")
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.get("/zones/{zone_id}/places")
def get_places_in_zone(zone_id: int):
    resp = requests.get(f"{BOOKING_SERVICE_URL}/zones/{zone_id}/places")
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.get("/places/{place_id}/slots")
def get_slots(place_id: int, request: Request):
    # Forward query parameters
    resp = requests.get(f"{BOOKING_SERVICE_URL}/places/{place_id}/slots", params=request.query_params)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.post("/")
async def create_booking(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    # Forward user_id and role as headers to booking service
    headers = {
        "X-User-Id": str(user.get('user_id', user.get('sub'))),
        "X-User-Role": user.get('role', 'user')
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/bookings", json=body, headers=headers)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.post("/by-time")
async def create_booking_by_time(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    headers = {
        "X-User-Id": str(user.get('user_id', user.get('sub'))),
        "X-User-Role": user.get('role', 'user')
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/bookings/by-time", json=body, headers=headers)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.post("/cancel")
async def cancel(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    headers = {
        "X-User-Id": str(user.get('user_id', user.get('sub'))),
        "X-User-Role": user.get('role', 'user')
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/bookings/cancel", json=body, headers=headers)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.get("/history")
async def booking_history(request: Request, user=Depends(get_current_user)):
    headers = {
        "X-User-Id": str(user.get('user_id', user.get('sub'))),
        "X-User-Role": user.get('role', 'user')
    }
    resp = requests.get(f"{BOOKING_SERVICE_URL}/bookings/history", params=request.query_params, headers=headers)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))

@router.post("/{booking_id}/extend")
async def extend_booking(booking_id: int, user=Depends(get_current_user)):
    headers = {
        "X-User-Id": str(user.get('user_id', user.get('sub'))),
        "X-User-Role": user.get('role', 'user')
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/bookings/{booking_id}/extend", headers=headers)
    return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type',"application/json"))