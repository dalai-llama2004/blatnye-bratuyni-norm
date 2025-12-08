# Database - Система бронирования коворкинга "ПУНККроссинг"

## Обзор

Централизованное управление схемами и миграциями базы данных PostgreSQL для всех микросервисов системы бронирования коворкинга.

## Технологический стек

- **База данных**: PostgreSQL 14+
- **Язык миграций**: SQL
- **Инструменты**: psycopg2 для Python миграций

## Структура проекта

```
services/database/
├── init.sql             # Инициализация БД (схемы, расширения)
├── users_schema.sql     # Схема для User Service
├── bookings_schema.sql  # Схема для Booking Service
├── migrate.py           # Скрипт для запуска миграций
├── config.py            # Конфигурация подключения
├── requirements.txt     # Python зависимости
├── Dockerfile           # Docker образ для миграций
└── README.md
```

## Схемы базы данных

### Users Schema

Используется User Service для управления пользователями.

**Таблица: users.users**
- `id` - Первичный ключ
- `name` - Имя пользователя
- `email` - Email (уникальный)
- `password_hash` - Хешированный пароль
- `confirmed` - Флаг подтверждения email
- `is_admin` - Флаг администратора
- `confirmation_code` - Код подтверждения
- `recovery_code` - Код восстановления
- `created_at` - Дата создания

### Bookings Schema

Используется Booking Service для управления зонами и бронированиями.

**Таблица: bookings.zones**
- `id` - Первичный ключ
- `name` - Название зоны (например, "Главный коворкинг")
- `address` - Адрес коворкинга
- `is_active` - Статус активности
- `closure_reason` - Причина закрытия зоны (TEXT, NULL для активных зон)
- `created_at` - Дата создания
- `updated_at` - Дата обновления

**Таблица: bookings.places**
- `id` - Первичный ключ
- `zone_id` - Внешний ключ на zones
- `name` - Название места (например, "Место 1")
- `is_active` - Статус активности
- `created_at` - Дата создания
- `updated_at` - Дата обновления

**Таблица: bookings.slots**
- `id` - Первичный ключ
- `place_id` - Внешний ключ на places
- `start_time` - Время начала
- `end_time` - Время окончания
- `is_available` - Доступность слота
- Уникальное ограничение на (place_id, start_time, end_time)
- Индекс на (place_id, start_time)

**Таблица: bookings.bookings**
- `id` - Первичный ключ
- `user_id` - ID пользователя (ссылка на users.users)
- `slot_id` - Внешний ключ на slots
- `zone_name` - Денормализованное название зоны
- `zone_address` - Денормализованный адрес зоны
- `start_time` - Денормализованное время начала
- `end_time` - Денормализованное время окончания
- `status` - Статус (active / cancelled / completed)
- `created_at` - Дата создания
- `updated_at` - Дата обновления

## Установка и запуск

### Требования

- PostgreSQL 14+
- Python 3.11+ (для скриптов миграций)

### Инициализация через Docker

При старте контейнера автоматически инициализируется БД по init.sql:

```bash
docker build -t blatnye-db .
docker run -p 5432:5432 blatnye-db
```

Рекомендуется запускать через общий docker-compose.

### Создание базы данных (без Docker)

```bash
# Создайте базу данных
createdb coworking_db

# Или через psql
psql -U postgres
CREATE DATABASE coworking_db;
```

### Применение схем

```bash
cd services/database

# Инициализация
psql -U postgres -d coworking_db -f init.sql

# Применение схем
psql -U postgres -d coworking_db -f users_schema.sql
psql -U postgres -d coworking_db -f bookings_schema.sql
```

### Использование migrate.py

```bash
cd services/database
pip install -r requirements.txt

# Установить переменные окружения
export DATABASE_URL="postgresql://user:password@localhost:5432/coworking_db"

# Запустить миграции
python migrate.py
```

## Особенности реализации

### Схемы (Namespaces)

База данных использует схемы для логического разделения:
- `users` - данные пользователей
- `bookings` - данные бронирований
- `public` - общие данные

Это позволяет:
- Изолировать данные разных сервисов
- Упростить управление правами доступа
- Избежать конфликтов имен таблиц

### Каскадное удаление

Настроены каскадные удаления для поддержания целостности:
- При удалении зоны удаляются все места
- При удалении места удаляются все слоты
- При удалении слота удаляются все бронирования

### Индексы

Созданы индексы для оптимизации запросов:
- На внешние ключи
- На часто используемые поля (email, status)
- Композитные индексы для сложных запросов

### Денормализация

В таблице bookings денормализованы данные зоны и времени для:
- Быстрого доступа без JOIN
- Сохранения исторических данных (даже если зона изменится)
- Упрощения запросов

## Интеграция с Docker

Для запуска вместе с остальными сервисами используется `docker-compose.yaml`:

```yaml
postgres:
  image: postgres:14
  container_name: coworking-db
  environment:
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: password
    POSTGRES_DB: coworking_db
  ports:
    - "5432:5432"
  volumes:
    - postgres_data:/var/lib/postgresql/data
    - ./services/database:/docker-entrypoint-initdb.d
```

## Troubleshooting

### Проблема: Не удается подключиться к БД

**Решение**:
- Проверьте, запущен ли PostgreSQL: `sudo systemctl status postgresql`
- Проверьте DATABASE_URL
- Проверьте pg_hba.conf для разрешения подключений

### Проблема: Ошибка "relation already exists"

**Решение**:
- Проверьте, не применена ли миграция уже
- Используйте `IF NOT EXISTS` в CREATE операциях
- Очистите БД: `DROP SCHEMA bookings CASCADE;`

### Проблема: Медленные запросы

**Решение**:
- Проверьте наличие индексов: `\d+ table_name`
- Используйте EXPLAIN ANALYZE для анализа
- Добавьте недостающие индексы
- Рассмотрите партиционирование для больших таблиц

## Планы развития

- [ ] Добавить версионирование миграций (Alembic/Flyway)
- [ ] Реализовать партиционирование для bookings
- [ ] Добавить materialized views для отчетов
- [ ] Настроить репликацию для отказоустойчивости
- [ ] Добавить мониторинг производительности
- [ ] Реализовать автоматические бэкапы

## Контакты и поддержка

Для вопросов и предложений создавайте issues в репозитории проекта.

---

**Версия**: 0.1.0  
**Дата последнего обновления**: 2025-12-07
