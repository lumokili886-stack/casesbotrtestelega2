# VAULT Mini App

Реализовано:
- Telegram Mini App auth (проверка `initData` подписи на backend);
- Steam OpenID привязка;
- Telegram Stars платежи (`createInvoiceLink` + `openInvoice` + webhook);
- Открытие кейсов, инвентарь, продажа предметов, продажа всего инвентаря — всё на backend.
- Закрытая desktop админ-панель на отдельном URL (`ADMIN_PATH`) с логином/паролем.

## Структура
- `cs2-miniapp.html` — фронтенд.
- `server.js` — backend API.
- `data/db.json` — локальное хранилище.
- `scripts/save_version.sh` — сохранение снепшота версии.
- `versions/<timestamp>/` — архив предыдущих версий.

## Настройка
```bash
cp .env.example .env
```

Заполните `.env`:
- `PUBLIC_BASE_URL` — публичный HTTPS домен.
- `BOT_TOKEN` — токен Telegram-бота.
- `BOT_USERNAME` — username бота без `@`.
- `TELEGRAM_WEBHOOK_SECRET` — секрет webhook.
- `STEAM_REALM` — обычно `https://your-domain.com/`.
- `ADMIN_PATH` — отдельный путь админки, например `/vault-admin-9x7k`.
- `ADMIN_LOGIN` / `ADMIN_PASSWORD` — данные для входа в админку.
- `ADMIN_SESSION_SECRET` — отдельный секрет для подписи admin cookie.
- `ADMIN_SESSION_TTL_HOURS` — время жизни сессии админа.

## Admin Panel (отдельный URL)
- Админка не встроена в Telegram Mini App и работает отдельно в браузере (desktop).
- URL: `https://your-domain.com<ADMIN_PATH>`.
- Доступ закрыт через `ADMIN_LOGIN` + `ADMIN_PASSWORD`.
- В админке доступны:
  - обзор пользователей и балансов;
  - фильтрация/поиск пользователей;
  - ручная корректировка баланса с аудитом;
  - лента операций с фильтрами (тип / user id / период / текст);
  - управление кейсами (цена и вкл/выкл);
  - управление админами (создание/удаление).

### Роли админов
- `owner` — полный доступ, может создавать/удалять админов.
- `admin` — тот же операционный доступ (пользователи, баланс, кейсы, транзакции), но без управления админами.
- `owner` bootstrap-ится из env (`ADMIN_LOGIN` + `ADMIN_PASSWORD`) при первом запуске.

## Запуск
```bash
npm install
npm start
```

## Webhook
```bash
curl -X POST "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://your-domain.com/telegram/webhook",
    "secret_token": "replace_me_optional"
  }'
```

## Версионирование перед изменениями
Перед каждой новой правкой запускайте:
```bash
npm run snapshot
```

Это сохранит текущие файлы в `versions/<timestamp>/` и обновит `VERSION`.

## Git workflow
1. `npm run snapshot`
2. правки
3. `git add .`
4. `git commit -m "vault: <описание версии>"`
5. `git push origin <branch>`
