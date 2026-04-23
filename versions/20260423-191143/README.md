# VAULT Mini App

Реализовано:
- Telegram Mini App auth (проверка `initData` подписи на backend);
- Steam OpenID привязка;
- Telegram Stars платежи (`createInvoiceLink` + `openInvoice` + webhook);
- Открытие кейсов, инвентарь, продажа предметов, продажа всего инвентаря — всё на backend.

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
