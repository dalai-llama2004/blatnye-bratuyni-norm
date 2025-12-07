# Инструкция по применению миграции closure_reason

## Описание
Эта миграция добавляет поле `closure_reason` в таблицу `zones` для хранения причины закрытия зоны.

## Применение миграции

### Вариант 1: Используя migrate.py
```bash
cd services/database
python migrate.py migration_add_closure_reason.sql
```

### Вариант 2: Напрямую через psql
```bash
psql -h <host> -U <user> -d <database> -f migration_add_closure_reason.sql
```

### Вариант 3: Через docker-compose (если БД в контейнере)
```bash
docker-compose exec postgres psql -U <user> -d <database> -f /path/to/migration_add_closure_reason.sql
```

## Проверка применения
После выполнения миграции можно проверить, что колонка добавлена:
```sql
\d bookings.zones
-- или
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'bookings' 
  AND table_name = 'zones' 
  AND column_name = 'closure_reason';
```

## Откат
Если нужно откатить миграцию:
```sql
ALTER TABLE bookings.zones DROP COLUMN IF EXISTS closure_reason;
```

## Примечания
- Миграция безопасна и может быть применена на работающей системе (использует `ADD COLUMN IF NOT EXISTS`)
- Колонка nullable, поэтому не требует значений по умолчанию для существующих записей
- После применения миграции необходимо перезапустить booking-service для применения изменений в моделях
