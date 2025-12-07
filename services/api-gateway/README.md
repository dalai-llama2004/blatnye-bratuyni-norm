# API Gateway - Система бронирования коворкинга "ПУНККроссинг"

## Обзор

Единая точка входа для всех клиентских запросов. API Gateway маршрутизирует обращения к микросервисам, обрабатывает JWT-авторизацию и управляет CORS.

## Технологический стек

- **Framework**: FastAPI
- **Язык**: Python 3.11+
- **HTTP клиент**: requests
- **Аутентификация**: JWT

## Структура проекта

```
services/api-gateway/
├── main.py              # Точка входа приложения
├── auth.py              # JWT авторизация
├── config.py            # Конфигурация URLs сервисов
├── routes/              # Проксирующие роуты
│   ├── user.py         # Проксирование к User Service
│   ├── booking.py      # Проксирование к Booking Service
│   ├── notification.py # Проксирование к Notification Service
│   └── admin.py        # Админские роуты
├── requirements.txt     # Python зависимости
├── Dockerfile          # Docker образ
└── README.md
```

## Основные функции

### Маршрутизация

Gateway проксирует запросы к следующим сервисам:

- `/users/*` → User Service (порт 8002)
- `/bookings/*` → Booking Service (порт 8001)
- `/notifications/*` → Notification Service (порт 8003)
- `/admin/*` → Booking Service admin endpoints

### Аутентификация и авторизация

- Проверка JWT токенов для защищенных эндпоинтов
- Извлечение user_id и role из токена
- Передача данных пользователя в заголовках к сервисам:
  - `X-User-Id` - ID пользователя
  - `X-User-Role` - Роль пользователя

### CORS

Настроен CORS для взаимодействия с фронтендом:
- Origin: `http://localhost:3000`
- Credentials: включены
- Methods: все
- Headers: все

## Эндпоинты

### Пользовательские (`/users`)

- `POST /users/register` - Регистрация
- `POST /users/confirm` - Подтверждение email
- `POST /users/login` - Вход
- `POST /users/recover` - Запрос восстановления пароля
- `POST /users/reset` - Сброс пароля

### Бронирование (`/bookings`)

- `GET /bookings/zones` - Список зон
- `GET /bookings/zones/{zone_id}/places` - Места в зоне
- `GET /bookings/places/{place_id}/slots` - Доступные слоты
- `POST /bookings/` - Создать бронирование (слот)
- `POST /bookings/by-time` - Создать бронирование (время)
- `POST /bookings/cancel` - Отменить бронирование
- `GET /bookings/history` - История бронирований
- `POST /bookings/{booking_id}/extend` - Продлить бронирование

### Администрирование (`/admin`)

- `POST /admin/zones` - Создать зону
- `PATCH /admin/zones/{zone_id}` - Обновить зону
- `DELETE /admin/zones/{zone_id}` - Удалить зону
- `POST /admin/zones/{zone_id}/close` - Закрыть зону

### Уведомления (`/notifications`)

- Проксирование к Notification Service

## Установка и запуск

### Требования

- Python 3.11+

### Установка зависимостей

```bash
cd services/api-gateway
pip install -r requirements.txt
```

### Настройка окружения

Создайте файл `.env`:

```env
USER_SERVICE_URL=http://localhost:8002
BOOKING_SERVICE_URL=http://localhost:8001
NOTIFICATION_SERVICE_URL=http://localhost:8003
JWT_SECRET=your-secret-key
```

### Режим разработки

```bash
uvicorn main:app --reload --port 8000
```

Gateway будет доступен на `http://localhost:8000`.

### Продакшн сборка

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Особенности реализации

### Проксирование запросов

Gateway не содержит бизнес-логики, а просто перенаправляет запросы:

```python
resp = requests.get(f"{BOOKING_SERVICE_URL}/zones")
return Response(content=resp.content, status_code=resp.status_code)
```

### JWT проверка

Для защищенных эндпоинтов используется dependency:

```python
@router.post("/bookings")
async def create_booking(user=Depends(get_current_user)):
    # user содержит данные из JWT
```

### OPTIONS для CORS

Для админских роутов добавлены обработчики OPTIONS для preflight запросов.

## Интеграция с Docker

Для запуска вместе с остальными сервисами используется `docker-compose.yaml`:

```yaml
api-gateway:
  build: ./services/api-gateway
  container_name: coworking-gateway
  ports:
    - "8000:8000"
  environment:
    - USER_SERVICE_URL=http://user-service:8002
    - BOOKING_SERVICE_URL=http://booking-service:8001
    - NOTIFICATION_SERVICE_URL=http://notification-service:8003
  depends_on:
    - user-service
    - booking-service
    - notification-service
```

## Troubleshooting

### Проблема: CORS ошибки

**Решение**: Проверьте настройки CORS middleware в main.py:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Проблема: 401 при запросах к защищенным эндпоинтам

**Решение**: Убедитесь, что токен передается в заголовке:

```
Authorization: Bearer <token>
```

### Проблема: Сервисы недоступны

**Решение**: Проверьте URL сервисов в config.py и что все сервисы запущены.

## Планы развития

- [ ] Добавить rate limiting
- [ ] Реализовать кэширование ответов
- [ ] Добавить логирование всех запросов
- [ ] Интегрировать metrics и monitoring
- [ ] Добавить circuit breaker для устойчивости

## Контакты и поддержка

Для вопросов и предложений создавайте issues в репозитории проекта.

---

**Версия**: 0.1.0  
**Дата последнего обновления**: 2025-12-07
