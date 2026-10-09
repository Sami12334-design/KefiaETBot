# KefiaETBot

KefiaETBot is a Telegram bot MVP for admin-managed task campaigns, referral tracking, points, advertising requests, marketplace submissions, and withdrawal review.

## Features in this starter
- User dashboard with Daily Jobs, Invite & Earn, Promotion Center, Marketplace, Wallet, Profile, and Withdraw.
- Admin dashboard with task creation, basic platform statistics, request review, and configurable limits/prices.
- Admin-created channel join campaigns with unique invitation links and Telegram chat-member event tracking.
- Points awarded for verified unique joins, using the points value configured for the campaign.
- Advertising request intake and admin review/quote workflow foundation.
- Marketplace submissions for buying/selling supported digital assets and social-media assets, with manual review.
- SQLite persistence, environment-based configuration, and basic error logging.

## Requirements
- Python 3.10+
- A Telegram bot token from @BotFather
- A channel where the bot is an administrator with permission to create invite links
- A persistent disk for SQLite if deployed on a hosting provider

## Run locally
1. Create a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`
3. Set environment variables:
   - `BOT_TOKEN` — token from BotFather
   - `ADMIN_IDS` — comma-separated Telegram numeric user IDs, e.g. `123456789,987654321`
   - `DATABASE_PATH` — optional, defaults to `kefiaetbot.db`
4. Start the bot: `python bot.py`

## Admin commands
- `/set min_withdraw_points 1000`
- `/set usdt_etb_rate 150`
- `/set price_ad_product 500`
- `/set price_ad_members 300`
- `/set price_ad_views 200`
- `/quote_ad REQUEST_ID PRICE_ETB PAYMENT_INSTRUCTIONS` — send an ad quote to the request owner (admins only).
- `/receipt AD_REQUEST_ID` — a user submits a payment screenshot/document or transaction reference for a quoted ad.
- `/myads` — list your recent ad requests and their statuses.

Ad-payment proof is forwarded to configured admins for manual verification. The bot does not independently verify bank/mobile-money payments; admins must check the actual transaction before approving the request.
- `/set usdt_etb_rate 150`
- `/set price_ad_product 500`
- `/set price_ad_members 300`
- `/set price_ad_views 200`

Use the Admin Dashboard to create join tasks. The bot must be a channel administrator and must be allowed to create invitation links. Add it to a channel before creating a task.

## Important production notes
This is a starter implementation, not a fully audited payment or trading platform. Before accepting real money, complete and test: quote acceptance and receipt uploads, admin receipt verification, refunding points when withdrawals are rejected, detailed audit logs, rate limits/anti-fraud checks, backup/restore, privacy/terms pages, and any local legal/compliance requirements. Social-media monetization cannot be reliably verified for every platform automatically; treat it as a manual review. Never request account passwords, wallet seed phrases, or private keys. Do not advertise fake followers or fake engagement.

## Deployment
On Render or another host, configure `BOT_TOKEN` and `ADMIN_IDS` as environment variables. Use persistent storage for `DATABASE_PATH`; ephemeral filesystems can erase the SQLite database on redeploy. Keep the token secret and never commit it to GitHub.
