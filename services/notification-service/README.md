# Notification Service - Система бронирования коворкинга "ПУНККроссинг"

## Обзор

Микросервис для отправки уведомлений пользователям по различным каналам. Основной функционал - отправка email через SMTP. Построен на FastAPI.

## Технологический стек

- **Framework**: FastAPI
- **Язык**: Python 3.11+
- **База данных**: PostgreSQL (для хранения истории уведомлений)
- **Email**: SMTP для отправки писем
- **ORM**: SQLAlchemy

## Структура проекта

```
services/notification-service/
├── main.py              # Точка входа приложения
├── models.py            # SQLAlchemy модели (Notification)
├── schemas.py           # Pydantic схемы для валидации
├── routes.py            # API эндпоинты
├── crud.py              # CRUD операции с БД
├── mailer.py            # Логика отправки email
├── db.py                # Настройка подключения к БД
├── config.py            # Конфигурация
├── requirements.txt     # Python зависимости
├── Dockerfile           # Docker образ
└── README.md
```

## Основные функции

### Email уведомления

- **Отправка email** (`POST /notify/email`):
  - Отправка email уведомления пользователю
  - Сохранение истории отправленных уведомлений
  - Обработка ошибок отправки

### История уведомлений

- Хранение всех отправленных уведомлений
- Статусы: sent / failed
- Возможность повторной отправки при ошибках

## API интеграция

Сервис взаимодействует с другими микросервисами для отправки уведомлений:
- Booking Service → уведомления о бронированиях
- User Service → коды подтверждения и восстановления

### Эндпоинты

- `POST /notify/email` - Отправить email уведомление

#### Пример запроса

```json
POST /notify/email
Content-Type: application/json

{
  "email": "user@example.com",
  "subject": "Бронирование подтверждено",
  "text": "Ваша бронь на 15.12.2025 в 09:00 подтверждена!"
}
```

## Модели данных

### Notification (Уведомление)

- `id` - Уникальный идентификатор
- `email` - Email получателя
- `subject` - Тема письма
- `text` - Текст письма
- `status` - Статус (sent / failed)
- `sent_at` - Время отправки
- `error_message` - Сообщение об ошибке (если есть)

## Установка и запуск

### Требования

- Python 3.11+
- PostgreSQL 14+
- SMTP сервер для отправки email

### Установка зависимостей

```bash
cd services/notification-service
pip install -r requirements.txt
```

### Настройка окружения

Создайте файл `.env`:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/notifications_db
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
EMAIL_FROM=noreply@coworking.com
```

### Режим разработки

```bash
uvicorn main:app --reload --port 8003
```

Приложение будет доступно на `http://localhost:8003`.

### Продакшн сборка

```bash
uvicorn main:app --host 0.0.0.0 --port 8003
```

## Особенности реализации

### SMTP отправка

- Использование стандартной библиотеки `smtplib`
- Поддержка TLS шифрования
- Асинхронная обработка
- Retry механизм при временных ошибках

### История уведомлений

- Все отправленные уведомления сохраняются в БД
- Можно отслеживать статус доставки
- Полезно для отладки и аналитики

### Обработка ошибок

- Graceful обработка ошибок SMTP
- Сохранение информации об ошибках
- Возможность повторной отправки

## Интеграция с другими сервисами

### User Service

User Service вызывает Notification Service для отправки:
- Кодов подтверждения email
- Кодов восстановления пароля
- Приветственных писем

### Booking Service

Booking Service вызывает Notification Service для отправки:
- Подтверждений бронирования
- Уведомлений об отмене
- Напоминаний о предстоящих бронированиях

## Интеграция с Docker

Для запуска вместе с остальными сервисами используется `docker-compose.yaml`:

```yaml
notification-service:
  build: ./services/notification-service
  container_name: coworking-notifications
  ports:
    - "8003:8003"
  environment:
    - DATABASE_URL=postgresql://user:password@postgres:5432/notifications_db
    - SMTP_HOST=smtp.gmail.com
    - SMTP_PORT=587
    - SMTP_USER=${SMTP_USER}
    - SMTP_PASSWORD=${SMTP_PASSWORD}
  depends_on:
    - postgres
```

## Troubleshooting

### Проблема: Email не отправляются

**Решение**: 
- Проверьте SMTP настройки
- Для Gmail: включите "Пароли приложений"
- Проверьте firewall и доступность SMTP порта

### Проблема: Emails попадают в спам

**Решение**:
- Настройте SPF и DKIM записи для вашего домена
- Используйте валидный FROM адрес
- Добавьте unsubscribe ссылку в письма

### Проблема: Медленная отправка при большом объеме

**Решение**:
- Внедрите очередь сообщений (RabbitMQ/Redis)
- Используйте bulk email сервисы (SendGrid, AWS SES)
- Батчинг отправки

## Планы развития

- [ ] Добавить массовые рассылки (`/notify/bulk`)
- [ ] Интегрировать очередь сообщений (RabbitMQ/Redis)
- [ ] Добавить SMS уведомления
- [ ] Добавить push-уведомления
- [ ] Реализовать шаблоны писем
- [ ] Добавить scheduled отправку
- [ ] Интегрировать с внешними сервисами (SendGrid, Mailgun)
- [ ] Добавить аналитику открываемости писем

## Контакты и поддержка

Для вопросов и предложений создавайте issues в репозитории проекта.

---

**Версия**: 0.1.0  
**Дата последнего обновления**: 2025-12-07
