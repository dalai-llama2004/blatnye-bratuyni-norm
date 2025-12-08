"""
Тесты для проверки работы с московским временем.
"""
import pytest
from datetime import datetime, timedelta, timezone
import pytz

from timezone_utils import (
    now_msk,
    now_utc,
    to_msk,
    msk_to_utc,
    utc_to_msk,
    MOSCOW_TZ,
)


def test_now_msk_returns_moscow_time():
    """Проверка, что now_msk() возвращает время в московском часовом поясе."""
    result = now_msk()
    assert result.tzinfo is not None
    assert str(result.tzinfo) == 'Europe/Moscow'
    # Проверяем, что результат близок к текущему времени
    now = datetime.now(MOSCOW_TZ)
    assert abs((result - now).total_seconds()) < 1


def test_now_utc_returns_naive_datetime():
    """Проверка, что now_utc() возвращает naive datetime в UTC."""
    result = now_utc()
    assert result.tzinfo is None
    # Проверяем, что результат близок к текущему времени UTC
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs((result - now).total_seconds()) < 1


def test_to_msk_converts_naive_utc_to_moscow():
    """Проверка преобразования naive UTC datetime в московское время."""
    # Создаем naive datetime (интерпретируется как UTC)
    utc_time = datetime(2025, 1, 15, 12, 0, 0)
    
    result = to_msk(utc_time)
    
    # Московское время должно быть на 3 часа вперед
    assert result.hour == 15
    assert result.tzinfo is not None
    assert str(result.tzinfo) == 'Europe/Moscow'


def test_to_msk_converts_aware_utc_to_moscow():
    """Проверка преобразования aware UTC datetime в московское время."""
    # Создаем aware UTC datetime
    utc_time = pytz.UTC.localize(datetime(2025, 1, 15, 12, 0, 0))
    
    result = to_msk(utc_time)
    
    # Московское время должно быть на 3 часа вперед
    assert result.hour == 15
    assert result.tzinfo is not None
    assert str(result.tzinfo) == 'Europe/Moscow'


def test_msk_to_utc_converts_naive_moscow_to_utc():
    """Проверка преобразования naive московского времени в UTC."""
    # Создаем naive datetime (интерпретируется как московское время)
    msk_time = datetime(2025, 1, 15, 15, 0, 0)
    
    result = msk_to_utc(msk_time)
    
    # UTC время должно быть на 3 часа назад
    assert result.hour == 12
    assert result.tzinfo is None


def test_msk_to_utc_converts_aware_moscow_to_utc():
    """Проверка преобразования aware московского времени в UTC."""
    # Создаем aware московское datetime
    msk_time = MOSCOW_TZ.localize(datetime(2025, 1, 15, 15, 0, 0))
    
    result = msk_to_utc(msk_time)
    
    # UTC время должно быть на 3 часа назад
    assert result.hour == 12
    assert result.tzinfo is None


def test_utc_to_msk_converts_naive_utc_to_moscow():
    """Проверка преобразования naive UTC в московское время."""
    utc_time = datetime(2025, 1, 15, 12, 0, 0)
    
    result = utc_to_msk(utc_time)
    
    assert result.hour == 15
    assert result.tzinfo is not None
    assert str(result.tzinfo) == 'Europe/Moscow'


def test_utc_to_msk_converts_aware_utc_to_moscow():
    """Проверка преобразования aware UTC в московское время."""
    utc_time = pytz.UTC.localize(datetime(2025, 1, 15, 12, 0, 0))
    
    result = utc_to_msk(utc_time)
    
    assert result.hour == 15
    assert result.tzinfo is not None
    assert str(result.tzinfo) == 'Europe/Moscow'


def test_round_trip_conversion():
    """Проверка обратной конвертации: MSK -> UTC -> MSK."""
    original = MOSCOW_TZ.localize(datetime(2025, 1, 15, 15, 30, 45))
    
    # MSK -> UTC
    utc = msk_to_utc(original)
    # UTC -> MSK
    back_to_msk = utc_to_msk(utc)
    
    # Проверяем, что время совпадает
    assert back_to_msk.year == original.year
    assert back_to_msk.month == original.month
    assert back_to_msk.day == original.day
    assert back_to_msk.hour == original.hour
    assert back_to_msk.minute == original.minute
    assert back_to_msk.second == original.second
