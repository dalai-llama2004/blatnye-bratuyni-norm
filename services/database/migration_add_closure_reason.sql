-- Миграция: добавление поля closure_reason в таблицу zones
-- Дата: 2025-12-07

-- Добавить недостающие колонки в таблицу zones (если они не существуют)
ALTER TABLE bookings.zones 
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE NOT NULL;

ALTER TABLE bookings.zones 
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW() NOT NULL;

ALTER TABLE bookings.zones 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW() NOT NULL;

-- Добавить колонку closure_reason для хранения причины закрытия зоны
ALTER TABLE bookings.zones 
ADD COLUMN IF NOT EXISTS closure_reason TEXT DEFAULT NULL;
