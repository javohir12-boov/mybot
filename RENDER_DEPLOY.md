# Render Deploy (AI Quez Bot)

Bu loyiha Render'ga 2 xil usulda deploy qilinadi:

1) Polling (tavsiya)  
2) Webhook (agar webhook ishlatmoqchi bo'lsangiz)

## 1) Polling (Render Background Worker)

- Render -> New -> Background Worker
- Repository: shu repo
- Build command:
  - `pip install -r requirements.txt`
- Start command:
  - `python main.py`

Environment variables (kamida):
- `BOT_TOKEN` = Telegram bot token
- `ADMIN_IDS` = admin ID'lar (vergul bilan), masalan `123,456`

Database:
- Eng oson: SQLite (default). Eslatma: Render'da fayl DB (sqlite) doimiy saqlanmasligi mumkin (redeployda yo'qoladi).
- Tavsiya: Render Postgres.
  - Render Postgres yaratib `DATABASE_URL` ni environment'ga qo'ying.
  - Render odatda `postgres://...` beradi. Bizning `config.py` avtomatik `postgresql+asyncpg://...` ga aylantiradi.

## 2) Polling (Render Web Service) + Health server

Agar Web Service sifatida deploy qilsangiz va webhook ishlatmasangiz ham bo'ladi.
- `WEBHOOK_URL` ni BO'SH qoldiring.
- Render `PORT` beradi, kod avtomatik kichik health server ochadi (`/healthz`) va bot polling ishlaydi.

## 3) Webhook (Render Web Service)

- Render -> New -> Web Service
- Build command:
  - `pip install -r requirements.txt`
- Start command:
  - `python main.py`
- Health Check Path:
  - `/healthz`

Environment variables:
- `BOT_TOKEN`
- `ADMIN_IDS`
- `WEBHOOK_URL` = `https://<service-name>.onrender.com`
- `WEBHOOK_PATH` = `/webhook` (ixtiyoriy, default shu)
- `WEBHOOK_SECRET_TOKEN` = ixtiyoriy (tavsiya), random string

Webhook'ni bot avtomatik o'rnatadi (`set_webhook`).

## AI (ixtiyoriy)

AI ishlashi uchun bittasini sozlang:
- Gemini:
  - `AI_PROVIDER=gemini`
  - `GEMINI_API_KEY=...`
  - `GEMINI_MODEL=gemini-flash-latest`

- OpenAI:
  - `AI_PROVIDER=openai`
  - `OPENAI_API_KEY=sk-...`
  - `OPENAI_MODEL=gpt-4o-mini`

## Premium / Limit (ixtiyoriy)

Free trial (1 martalik):
- `FREE_TRIAL_FILES=2`
- `FREE_TRIAL_TOPICS=1`
- `FREE_TRIAL_DAYS=1`

To'lov:
- `PAYMENT_CARD_NUMBER=...`
- `PAYMENT_CARD_HOLDER=...`

Chekni AI tekshirishi:
- `PREMIUM_RECEIPT_AI=1`
- `PREMIUM_RECEIPT_AUTOAPPROVE=1`
- `PREMIUM_RECEIPT_APPROVE_CONF=0.9`

## Tez-tez uchraydigan muammolar

- Postgres ishlatganda xato chiqsa:
  - `DATABASE_URL` to'g'ri ekanini tekshiring.
  - `asyncpg` requirements.txt da bor.

- Web Service deploy bo'lsa-yu bot ishlamay qolsa:
  - Health check path `/healthz` ekanini tekshiring.
  - Agar webhook ishlatayotgan bo'lsangiz `WEBHOOK_URL` ni to'g'ri qo'ying.
