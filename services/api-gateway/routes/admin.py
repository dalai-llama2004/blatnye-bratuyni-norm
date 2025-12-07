from fastapi import APIRouter, Request, Depends, Response
import requests
from config import BOOKING_SERVICE_URL
from auth import get_current_user

router = APIRouter()

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "http://localhost:3000",
        "Access-Control-Allow-Methods": "POST, PATCH, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Credentials": "true"
    }

def proxy_response(resp):
    """
    Формирует проксирующий ответ с заголовком CORS для взаимодействия с браузером.
    """
    headers = dict(resp.headers)
    headers.update(cors_headers())  # Добавляем или перезаписываем CORS-заголовки
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=headers,
        media_type=resp.headers.get('content-type', "application/json"),
    )

# --- OPTIONS HANDLERS ---

@router.options("/zones")
async def options_zones():
    return Response(status_code=200, headers=cors_headers())

@router.options("/zones/{zone_id}")
async def options_zone(zone_id: int):
    return Response(status_code=200, headers=cors_headers())

@router.options("/zones/{zone_id}/close")
async def options_zone_close(zone_id: int):
    return Response(status_code=200, headers=cors_headers())

# --- PROXY ROUTES ---

@router.post("/zones")
async def create_zone(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    headers = {
        "X-User-Id": str(user.get("user_id", user.get("sub"))),
        "X-User-Role": user.get("role", "user"),
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/admin/zones", json=body, headers=headers)
    return proxy_response(resp)

@router.patch("/zones/{zone_id}")
async def update_zone(zone_id: int, request: Request, user=Depends(get_current_user)):
    body = await request.json()
    headers = {
        "X-User-Id": str(user.get("user_id", user.get("sub"))),
        "X-User-Role": user.get("role", "user"),
    }
    resp = requests.patch(f"{BOOKING_SERVICE_URL}/admin/zones/{zone_id}", json=body, headers=headers)
    return proxy_response(resp)

@router.delete("/zones/{zone_id}")
async def delete_zone(zone_id: int, user=Depends(get_current_user)):
    headers = {
        "X-User-Id": str(user.get("user_id", user.get("sub"))),
        "X-User-Role": user.get("role", "user"),
    }
    resp = requests.delete(f"{BOOKING_SERVICE_URL}/admin/zones/{zone_id}", headers=headers)
    return proxy_response(resp)

@router.post("/zones/{zone_id}/close")
async def close_zone(zone_id: int, request: Request, user=Depends(get_current_user)):
    body = await request.json()
    headers = {
        "X-User-Id": str(user.get("user_id", user.get("sub"))),
        "X-User-Role": user.get("role", "user"),
    }
    resp = requests.post(f"{BOOKING_SERVICE_URL}/admin/zones/{zone_id}/close", json=body, headers=headers)
    return proxy_response(resp)

@router.get("/zones/statistics")
async def get_zones_statistics(user=Depends(get_current_user)):
    headers = {
        "X-User-Id": str(user.get("user_id", user.get("sub"))),
        "X-User-Role": user.get("role", "user"),
    }
    resp = requests.get(f"{BOOKING_SERVICE_URL}/admin/zones/statistics", headers=headers)
    return proxy_response(resp)
