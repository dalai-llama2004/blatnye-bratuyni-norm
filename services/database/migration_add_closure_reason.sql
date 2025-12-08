-- Миграция: добавление поля closure_reason в таблицу zones
-- Дата: 2025-12-07

-- Добавить колонку closure_reason для хранения причины закрытия зоны
ALTER TABLE bookings.zones 
ADD COLUMN IF NOT EXISTS closure_reason TEXT DEFAULT NULL;

-- Примечание: колонка is_active и другие поля должны существовать
-- Если они не существуют, их нужно добавить вручную через отдельные миграции
