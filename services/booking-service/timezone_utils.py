"""
Утилиты для работы с московским временем.
Все даты и время в проекте используют часовой пояс Europe/Moscow.
БД хранит времена в UTC (naive datetime), но при работе с API они преобразуются в московское время.
"""
from datetime import datetime, timezone
import pytz

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def now_msk() -> datetime:
    """
    Возвращает текущее время в московском часовом поясе.
    
    Returns:
        datetime: Текущее время с timezone=Europe/Moscow
    """
    return datetime.now(MOSCOW_TZ)


def now_utc() -> datetime:
    """
    Возвращает текущее время в UTC (naive datetime).
    Используется для значений по умолчанию в моделях SQLAlchemy.
    
    Returns:
        datetime: Текущее время в UTC без timezone info
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_msk(dt: datetime) -> datetime:
    """
    Преобразует datetime в московский часовой пояс.
    
    Args:
        dt: datetime объект (может быть naive или aware)
    
    Returns:
        datetime: datetime с timezone=Europe/Moscow
    """
    if dt.tzinfo is None:
        # Если naive datetime, считаем что это UTC
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(MOSCOW_TZ)


def msk_to_utc(dt: datetime) -> datetime:
    """
    Преобразует московское время в UTC.
    Используется для работы с БД, которая хранит времена в UTC.
    
    Args:
        dt: datetime объект в московском времени
    
    Returns:
        datetime: datetime в UTC без timezone info (naive)
    """
    if dt.tzinfo is None:
        # Если naive datetime, считаем что это московское время
        # is_dst=None вызовет исключение при неоднозначном времени (DST переход)
        dt = MOSCOW_TZ.localize(dt, is_dst=None)
    return dt.astimezone(pytz.UTC).replace(tzinfo=None)


def utc_to_msk(dt: datetime) -> datetime:
    """
    Преобразует UTC время в московское.
    
    Args:
        dt: datetime объект в UTC (может быть naive или aware)
    
    Returns:
        datetime: datetime с timezone=Europe/Moscow
    """
    if dt.tzinfo is None:
        # Если naive datetime, считаем что это UTC
        dt = pytz.UTC.localize(dt)
    return dt.astimezone(MOSCOW_TZ)

