from __future__ import annotations

from typing import List

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    Path,
)
from sqlalchemy.ext.asyncio import AsyncSession

import crud
import schemas
from db import get_session
from security import require_admin


router = APIRouter(prefix="/admin", tags=["admin"])


@router.get(
    "/zones",
    response_model=List[schemas.ZoneOut],
    summary="Получить все зоны (включая закрытые) (admin)",
)
async def get_all_zones_endpoint(
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    """
    Возвращает все зоны, включая закрытые.
    Автоматически активирует зоны, у которых истекло время закрытия.
    """
    return await crud.get_zones(session=session, include_inactive=True)


@router.post(
    "/zones",
    response_model=schemas.ZoneOut,
    status_code=status.HTTP_201_CREATED,
    summary="Создать зону (admin)",
)
async def create_zone_endpoint(
    data: schemas.ZoneCreate,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    zone = await crud.create_zone(session=session, data=data)
    return zone


@router.patch(
    "/zones/{zone_id}",
    response_model=schemas.ZoneOut,
    summary="Обновить зону (admin)",
)
async def update_zone_endpoint(
    zone_id: int = Path(..., description="ID зоны"),
    data: schemas.ZoneUpdate = ...,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    zone = await crud.update_zone(
        session=session,
        zone_id=zone_id,
        data=data,
    )
    if zone is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Зона не найдена",
        )
    return zone


@router.delete(
    "/zones/{zone_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить зону (admin)",
)
async def delete_zone_endpoint(
    zone_id: int = Path(..., description="ID зоны"),
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    ok = await crud.delete_zone(session=session, zone_id=zone_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Зона не найдена",
        )
    # 204 — без тела
    return


@router.post(
    "/zones/{zone_id}/close",
    response_model=List[schemas.BookingOut],
    summary="Закрыть зону на обслуживание (admin)",
)
async def close_zone_endpoint(
    zone_id: int = Path(..., description="ID зоны"),
    data: schemas.ZoneCloseRequest = ...,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    affected_bookings = await crud.close_zone(
        session=session,
        zone_id=zone_id,
        data=data,
    )
    return affected_bookings


@router.get(
    "/zones/statistics",
    response_model=List[schemas.ZoneStatistics],
    summary="Получить статистику по всем зонам (admin)",
)
async def get_zones_statistics_endpoint(
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    """
    Возвращает статистику по всем зонам:
    - количество активных бронирований
    - количество отмененных бронирований
    """
    statistics = await crud.get_zones_statistics(session=session)
    return statistics


@router.get(
    "/statistics",
    response_model=schemas.GlobalStatistics,
    summary="Получить общую статистику (admin)",
)
async def get_global_statistics_endpoint(
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_admin),
):
    """
    Возвращает общую статистику:
    - общее число активных бронирований (status=active)
    - общее число отмененных бронирований (status=cancelled)
    - число пользователей "прямо сейчас" в коворкинге
    """
    statistics = await crud.get_global_statistics(session=session)
    return statistics
