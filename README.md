# Fuel Bot / Web калькулятор НП

Telegram-бот и веб-приложение для ориентировочного расчёта стоимости нефтепродуктов с доставкой, аналитики по данным СПбМТСБ, подписок на цену и заявок.

## Запуск Telegram-бота

```bash
python main.py
```

Требуется `BOT_TOKEN` в `.env`.

## Запуск веб-приложения (FastAPI)

```bash
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```

Откройте http://127.0.0.1:8000/

- Публичные страницы: `/`, `/calc`, `/analytics`
- Вход по email + OTP: `/login` → `/verify` → `/cabinet`
- Админка (если задан `ADMIN_WEB_PASSWORD`): `/admin`

Фоновые проверки подписок (`price_checker`) стартуют вместе с веб-приложением. Если задан `BOT_TOKEN`, уведомления уходят в Telegram; иначе — только на email пользователей с подписками (при настроенном SMTP).

## Переменные окружения

См. `.env.example`.

## База данных

По умолчанию SQLite (`DATABASE_URL=sqlite+aiosqlite:///fuel_bot.db`). При первом запуске создаются таблицы, включая `email_otps` для OTP.

Если база уже существовала со старым ограничением `users.telegram_id NOT NULL`, выполните миграцию вручную (SQLite):

```sql
-- при необходимости
ALTER TABLE users ADD COLUMN telegram_id_new BIGINT;
UPDATE users SET telegram_id_new = telegram_id;
-- далее пересоздание таблицы или PRAGMA — по ситуации
```

(В новых установках это не требуется.)

## Импорт бюллетеней СПбМТСБ

CLI: `python import_spimex_prices_from_pdf.py --bulletins-dir ./data/bulletins`

В админке веба: `/admin/import` (после входа паролем).
