import os
import asyncio
import sqlite3
import json
import math
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from functools import wraps

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMemberUpdated
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ChatMemberHandler, ContextTypes, filters
)

# KefiaETBot MVP: task rewards, invite tracking, ad requests, marketplace,
# wallet/withdrawal requests, and admin review. Prices and payouts are admin-configured.
TOKEN = os.getenv("BOT_TOKEN", "").strip()
def parse_admin_ids(value):
    """Parse comma- or semicolon-separated Telegram user IDs safely."""
    admin_ids = set()
    for item in value.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            admin_ids.add(int(item))
        except ValueError:
            logging.warning("Ignoring invalid ADMIN_IDS entry: %r", item)
    return admin_ids


ADMIN_IDS = parse_admin_ids(os.getenv("ADMIN_IDS", ""))
DB_PATH = os.getenv("DATABASE_PATH", "kefiaetbot.db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("KefiaETBot")


def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
          user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
          points INTEGER NOT NULL DEFAULT 0, joined_at TEXT NOT NULL,
          referred_by INTEGER, blocked INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tasks(
          id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
          channel TEXT NOT NULL, target INTEGER NOT NULL, points INTEGER NOT NULL,
          active INTEGER NOT NULL DEFAULT 1, completed_count INTEGER NOT NULL DEFAULT 0,
          created_by INTEGER NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS task_claims(
          id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL, invite_link TEXT, status TEXT NOT NULL DEFAULT 'pending',
          created_at TEXT NOT NULL, UNIQUE(task_id,user_id),
          FOREIGN KEY(task_id) REFERENCES tasks(id)
        );
        CREATE TABLE IF NOT EXISTS invite_links(
          invite_link TEXT PRIMARY KEY, task_id INTEGER NOT NULL, owner_user_id INTEGER NOT NULL,
          created_at TEXT NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(id)
        );
        CREATE TABLE IF NOT EXISTS invite_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, invite_link TEXT NOT NULL,
          joined_user_id INTEGER NOT NULL UNIQUE, joined_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ad_requests(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, kind TEXT NOT NULL,
          details TEXT NOT NULL, duration TEXT, quoted_price REAL, receipt TEXT,
          status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS market_listings(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, action TEXT NOT NULL,
          asset_type TEXT NOT NULL, details TEXT NOT NULL, amount TEXT, price REAL,
          status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS withdrawals(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, points INTEGER NOT NULL,
          payout_method TEXT NOT NULL, payout_details TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pending_inputs(
          user_id INTEGER PRIMARY KEY, action TEXT NOT NULL, data TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS payout_details(
          user_id INTEGER PRIMARY KEY, method TEXT NOT NULL,
          account_number TEXT NOT NULL, account_name TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS crypto_orders(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
          side TEXT NOT NULL, amount_usdt REAL NOT NULL, rate_etb REAL NOT NULL,
          total_etb REAL NOT NULL, payment_method TEXT, payment_details TEXT,
          payout_method TEXT, payout_account_number TEXT, payout_account_name TEXT,
          transfer_method TEXT, transfer_destination TEXT, receipt TEXT,
          status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          admin_id INTEGER, delivery_ref TEXT
        );
        CREATE TABLE IF NOT EXISTS digital_products(
          id TEXT PRIMARY KEY, name TEXT NOT NULL, duration_months INTEGER NOT NULL DEFAULT 1,
          price REAL NOT NULL DEFAULT 0, stock INTEGER NOT NULL DEFAULT 0,
          description TEXT NOT NULL DEFAULT '', features TEXT NOT NULL DEFAULT '',
          important_note TEXT NOT NULL DEFAULT '', notice TEXT NOT NULL DEFAULT '',
          warranty TEXT NOT NULL DEFAULT '', active INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS digital_payment_gateways(
          id TEXT PRIMARY KEY, name TEXT NOT NULL, account_number TEXT NOT NULL DEFAULT '',
          account_name TEXT NOT NULL DEFAULT '', instructions TEXT NOT NULL DEFAULT '',
          warning TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS digital_orders(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
          product_id TEXT NOT NULL, product_name TEXT NOT NULL, duration_months INTEGER NOT NULL,
          price REAL NOT NULL, gateway_id TEXT NOT NULL, gateway_name TEXT NOT NULL,
          receipt_file_id TEXT NOT NULL, receipt_type TEXT NOT NULL DEFAULT 'photo',
          status TEXT NOT NULL DEFAULT 'pending_approval', created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL, admin_id INTEGER, admin_reply TEXT
        );
        """)
        # Editable database defaults: admins can replace these values without code changes.
        c.execute("""INSERT OR IGNORE INTO digital_products
          (id,name,duration_months,price,stock,description,features,important_note,notice,warranty,active,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          ("gemini_pro_18m","Gemini Pro",18,0,0,
           "You will receive a self-activation link that you can claim yourself. It works with both new and existing Gmail accounts...",
           "Feature 1\nFeature 2\nFeature 3",
           "The redeem link must be used within the time set by the admin.",
           "Activation links may expire if not claimed in time.",
           "No warranty",1,now()))
        for key, value in (
            ("buy_usdt_stock", "0"), ("buy_usdt_rate", "0"), ("buy_usdt_min", "1"),
            ("buy_usdt_max", "1000"), ("buy_usdt_bep20_min", "1"), ("buy_usdt_bybit_min", "1"),
            ("buy_usdt_processing_time", "1-2 hours"),
            ("buy_usdt_amount_template", "💵 Buy USDT | USDT ይግዙ\n\n• Available stock (ያለው መጠን): {stock} USDT\n• Rate (ተመን): 1 USDT = {rate} ETB\n• Min / Max (አነስተኛ / ከፍተኛ): {minimum} - {maximum} USDT\n\n• BEP20 minimum: {bep20_min} USDT\n\n• Bybit UID minimum: {bybit_min} USDT\n\nEnter how much USDT you want to buy:\nምን ያህል USDT መግዛት ይፈልጋሉ? (ምሳሌ: 10)"),
            ("buy_usdt_destination_template", "🔁 USDT receiving destination | USDT መቀበያ አድራሻ\n\nSend your Binance Pay ID where admin should send the USDT:\nUSDT የሚላክበትን Binance Pay ID ያስገቡ:"),
            ("buy_usdt_payment_template", "Order summary\n• USDT: {amount} USDT\n• Pay: {total} ETB\n\n{payment_icon} {payment_name}\n\nNumber: {payment_number}\nName: {payment_account_name}\n\nSend the exact ETB amount, then upload a clear {payment_name} receipt screenshot.\n{receipt_amharic}\n\nAfter payment:\n{after_payment}\n\n🔍 Required: upload a clear screenshot of the receipt/transaction.\nText-only references are not accepted.\n{payment_warning}"),
            ("buy_usdt_confirmation_template", "✅ Payment proof received!\n\n📦 Your order #{order_id} is now under review.\n⏳ We will process it as soon as possible — instantly when we are online; otherwise, please allow up to {processing_time}.\n\n🙏 Thank you for using."),
            ("buy_usdt_admin_order_template", "💵 BUY USDT ORDER #{order_id}\nUser ID: {user_id}\nAmount: {amount} USDT\nRate: {rate} ETB/USDT\nTotal: {total} ETB\nReceiving destination: {destination}\nPayment method: {payment_name}\nStatus: Pending Approval"),
            ("buy_usdt_amount_invalid", "Please enter a valid positive USDT amount."),
            ("buy_usdt_amount_range_error", "Amount must be between {minimum} and {maximum} USDT."),
            ("buy_usdt_stock_error", "Sorry, only {stock} USDT is currently available."),
            ("buy_usdt_rate_error", "The USDT buy rate is not configured yet. Please contact an admin."),
            ("buy_usdt_no_gateway", "Payment is temporarily unavailable. Please contact an admin."),
            ("buy_usdt_destination_invalid", "Please enter a valid receiving ID or address."),
            ("buy_usdt_config_error", "The admin has not configured valid Buy USDT limits. Please contact support."),
            ("buy_usdt_state_expired", "Your order session expired. Please start again."),
            ("buy_usdt_choose_gateway_prompt", "Choose your ETB payment method:"),
            ("buy_usdt_menu_button", "💵 Buy USDT | USDT ይግዙ"),
            ("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"),
            ("digital_waiting_message", "✅ Your receipt has been received. Please wait while the admin verifies your payment. You will receive your activation link shortly."),
            ("digital_no_stock_message", "This product is currently out of stock. Please check back later."),
            ("digital_no_gateway_message", "Payment is temporarily unavailable for this product. Please contact an admin."),
            ("digital_cancel_message", "Your purchase was cancelled."),
            ("digital_order_submitted_message", "Your payment receipt has been submitted for admin approval."),
            ("digital_product_details_template", "🌟 {name} {duration}m\n\n💰 Price: {price} ETB each\n📦 In stock: {stock}\n\n📝 DESCRIPTION\n{description}\n\n✨ FEATURES\n{features}\n\n📌 Important Note:\n{note}\n\n🚨 NOTICE\n{notice}\n\n🎯 Price: {price} ETB / unit\n🛡️ Warranty: {warranty}\n\nTap Buy now when you are ready."),
            ("digital_payment_template", "🌟 Amount to pay: {price} ETB\n\n🏦 {gateway_name}\n\nNumber: {account_number}\nName: {account_name}\n\nSend the exact ETB amount, then upload a clear {gateway_name} receipt screenshot.\n{instructions}\n\n📞 Payment instructions\nAfter payment, upload a clear {gateway_name} receipt screenshot. Once your payment is verified, we will send your private redeem link.\n\n🔍 Required: upload a clear screenshot of the receipt/transaction.\nText-only references are not accepted.\n{warning}")
        ):
            c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key,value))


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def is_admin(user_id):
    return user_id in ADMIN_IDS


def kb(rows):
    return InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data=data) for text, data in row] for row in rows])


def upsert_user(user):
    with db() as c:
        c.execute("""INSERT INTO users(user_id,username,first_name,joined_at)
          VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
          first_name=excluded.first_name""", (user.id, user.username or "", user.first_name or "", now()))


def home_keyboard(admin=False):
    rows = [
        [("🧩 Daily Jobs", "jobs"), ("🔗 Invite & Earn", "invite")],
        [("📣 Promote / Ads", "ads"), ("🛍 Marketplace", "market")],
        [("👛 My Wallet", "wallet"), ("💸 Withdraw Points", "withdraw")],
        [("👤 My Account", "profile")]
    ]
    if admin:
        rows.append([("🛡 Admin Dashboard", "admin")])
    return kb(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    with db() as c:
        is_new_user = c.execute("SELECT 1 FROM users WHERE user_id=?", (user.id,)).fetchone() is None
    upsert_user(user)
    # A referral is counted once per Telegram account. Reward is optional and admin-configured.
    arg = context.args[0] if context.args else ""
    if arg.startswith("ref_") and arg[4:].isdigit():
        ref = int(arg[4:])
        if ref != user.id and is_new_user:
            rewarded_points = 0
            with db() as c:
                inviter = c.execute("SELECT user_id FROM users WHERE user_id=?", (ref,)).fetchone()
                if inviter:
                    cur = c.execute("UPDATE users SET referred_by=? WHERE user_id=? AND referred_by IS NULL", (ref, user.id))
                    if cur.rowcount == 1:
                        reward = c.execute("SELECT value FROM settings WHERE key='referral_points'").fetchone()
                        if reward:
                            try: rewarded_points = max(0, int(reward["value"]))
                            except (TypeError, ValueError): rewarded_points = 0
                        if rewarded_points:
                            c.execute("UPDATE users SET points=points+? WHERE user_id=?", (rewarded_points, ref))
            if rewarded_points:
                try:
                    await context.bot.send_message(ref, f"🎉 A new user joined through your invite link! You earned {rewarded_points} points.")
                except Exception:
                    log.warning("Could not notify referrer %s", ref)
    await update.effective_message.reply_text(
        f"Welcome {user.first_name or 'there'} to KefiaETBot!\n\n"
        "Complete verified jobs, earn points, promote products, and submit marketplace requests. "
        "Rewards and prices are controlled by admins.",
        reply_markup=home_keyboard(is_admin(user.id))
    )




# Crypto marketplace configuration is stored in settings and can be changed by admins
# with /set KEY VALUE. No wallet, bank account, rate, or user payout detail is hardcoded.
PAYMENT_METHODS = {
    "cbe": "🏦 CBE Birr",
    "telebirr": "📱 Telebirr",
    "boa": "🏦 Bank of Abyssinia",
    "other": "🏦 Other Ethiopian Bank",
}
SELL_NETWORKS = {
    "bsc": "🔵 BSC (BEP20)",
    "binance_pay": "🟡 Binance ID (Pay)",
    "bitget": "🟢 Bitget ID",
    "bybit": "🟣 Bybit ID",
    "ton": "🔵 TON Network (USDT)",
}


def setting_value(key, default=None):
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def setting_enabled(key, default=False):
    return str(setting_value(key, "true" if default else "false")).strip().lower() in ("1", "true", "yes", "on", "enabled")


def set_pending(user_id, action, data=None):
    payload = json.dumps(data or {}, ensure_ascii=False)
    with db() as c:
        c.execute(
            "INSERT INTO pending_inputs(user_id,action,data) VALUES(?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,data=excluded.data",
            (user_id, action, payload),
        )


def decode_pending(data):
    try:
        value = json.loads(data or "{}")
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError):
        return {}


def enabled_buy_methods():
    methods = []
    for slug in PAYMENT_METHODS:
        if not setting_enabled(f"buy_payment_{slug}_enabled"):
            continue
        details = setting_value(f"buy_payment_{slug}_details", "").strip()
        number = setting_value(f"buy_payment_{slug}_number", "").strip()
        account_name = setting_value(f"buy_payment_{slug}_account_name", "").strip()
        if not (details or (number and account_name)):
            continue
        icon = setting_value(f"buy_payment_{slug}_icon", "").strip()
        name = setting_value(f"buy_payment_{slug}_name", "").strip() or slug.upper()
        methods.append((slug, f"{icon} {name}".strip()))
    return methods


def buy_usdt_decimal(key):
    try:
        value = Decimal(str(setting_value(key)))
        return value if value.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def buy_usdt_values(amount=None, total=None, gateway=None, order_id="", user_id="", destination=""):
    gateway = gateway or {}
    val = lambda key: setting_value(key, "") or ""
    return {
        "stock": val("buy_usdt_stock"), "rate": val("buy_usdt_rate"),
        "minimum": val("buy_usdt_min"), "maximum": val("buy_usdt_max"),
        "bep20_min": val("buy_usdt_bep20_min"), "bybit_min": val("buy_usdt_bybit_min"),
        "processing_time": val("buy_usdt_processing_time"),
        "amount": f"{Decimal(str(amount)):f}" if amount is not None else "",
        "total": f"{Decimal(str(total)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):f}" if total is not None else "",
        "payment_icon": gateway.get("icon", ""), "payment_name": gateway.get("name", ""),
        "payment_number": gateway.get("number", ""), "payment_account_name": gateway.get("account_name", ""),
        "receipt_amharic": gateway.get("receipt_amharic", ""), "after_payment": gateway.get("after_payment", ""),
        "payment_warning": gateway.get("warning", ""), "order_id": order_id, "user_id": user_id,
        "destination": destination,
    }


def buy_usdt_gateway(slug):
    return {
        "icon": setting_value(f"buy_payment_{slug}_icon", "").strip(),
        "name": setting_value(f"buy_payment_{slug}_name", "").strip() or slug.upper(),
        "number": setting_value(f"buy_payment_{slug}_number", "").strip(),
        "account_name": setting_value(f"buy_payment_{slug}_account_name", "").strip(),
        "details": setting_value(f"buy_payment_{slug}_details", "").strip(),
        "receipt_amharic": setting_value(f"buy_payment_{slug}_receipt_amharic", "").strip(),
        "after_payment": setting_value(f"buy_payment_{slug}_after_payment", "").strip(),
        "warning": setting_value(f"buy_payment_{slug}_warning", "").strip(),
    }


def buy_usdt_amount_prompt():
    return render_digital_template(setting_value("buy_usdt_amount_template", ""), buy_usdt_values())


def enabled_sell_payout_methods():
    return [(slug, label) for slug, label in PAYMENT_METHODS.items()
            if setting_enabled(f"sell_payout_{slug}_enabled")]


def enabled_sell_networks():
    return [(slug, label) for slug, label in SELL_NETWORKS.items()
            if setting_enabled(f"sell_network_{slug}_enabled")
            and setting_value(f"sell_network_{slug}_destination", "").strip()]


def rate_for(side, amount):
    if amount <= 2:
        tier = "1_2"
    elif amount <= 5:
        tier = "2_5"
    else:
        tier = "5_plus"
    raw = setting_value(f"{side}_usdt_rate_{tier}")
    try:
        rate = float(raw)
        return rate if rate > 0 else None
    except (TypeError, ValueError):
        return None


def rates_text(side):
    values = []
    for tier, label in (("1_2", "1–2 USDT"), ("2_5", "over 2–5 USDT"), ("5_plus", "over 5 USDT")):
        raw = setting_value(f"{side}_usdt_rate_{tier}")
        try:
            shown = f"{float(raw):g} ETB/USDT" if raw is not None and float(raw) > 0 else "not configured"
        except (TypeError, ValueError):
            shown = "not configured"
        values.append(f"• {label}: {shown}")
    return "\n".join(values)


async def start_crypto_flow(q, context, side):
    uid = q.from_user.id
    enabled_key = f"{side}_enabled"
    custom_message = setting_value(f"{side}_unavailable_message", "").strip()
    if not setting_enabled(enabled_key, default=True):
        if custom_message:
            await q.edit_message_text(custom_message, reply_markup=kb([[("⬅️ Dashboard", "home")]]))
            return
        # Empty custom message intentionally means: skip the unavailable notice and show the normal flow.
    if side == "buy":
        set_pending(uid, "buy_usdt_amount", {})
        await q.edit_message_text(buy_usdt_amount_prompt(),
            reply_markup=kb([[(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")]]))
        return

    with db() as c:
        saved = c.execute("SELECT method,account_number,account_name FROM payout_details WHERE user_id=?", (uid,)).fetchone()
    methods = enabled_sell_payout_methods()
    rows = []
    if saved:
        rows.append([("✅ Use Saved Payout Details", "sell_saved")])
    for slug, label in methods:
        rows.append([(label, f"sell_payout_{slug}")])
    rows.append([("❌ Cancel | አቋርጥ", "home")])
    if not methods and not saved:
        await q.edit_message_text(
            "💸 Sell USDT is not configured yet. Please check back later.",
            reply_markup=kb([[("⬅️ Marketplace", "market")], [("⬅️ Dashboard", "home")]]),
        )
        return
    await q.edit_message_text(
        "💸 Sell USDT — Step 1\n\nHow would you like to receive your ETB payout?\n"
        "ብር ይቀበሉበታል የሚፈልጉትን የክፍያ መንገድ ይምረጡ:",
        reply_markup=kb(rows),
    )


async def create_buy_usdt_order_for_message(update, context, uid, state, slug):
    gateway = buy_usdt_gateway(slug)
    amount = Decimal(str(state.get("amount", "0")))
    rate = buy_usdt_decimal("buy_usdt_rate")
    stock = buy_usdt_decimal("buy_usdt_stock")
    if rate is None or rate <= 0:
        await update.effective_message.reply_text(setting_value("buy_usdt_rate_error", "Buy rate is not configured.")); return
    if stock is None or amount <= 0 or amount > stock:
        await update.effective_message.reply_text(render_digital_template(setting_value("buy_usdt_stock_error", ""), buy_usdt_values(amount=amount))); return
    total = (amount * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    details = gateway["details"] or f"{gateway['icon']} {gateway['name']}\nNumber: {gateway['number']}\nName: {gateway['account_name']}"
    with db() as c:
        cur = c.execute(
            "INSERT INTO crypto_orders(user_id,side,amount_usdt,rate_etb,total_etb,payment_method,payment_details,transfer_destination,status,created_at,updated_at) "
            "VALUES(?,'buy',?,?,?,?,?,?,'awaiting_payment_proof',?,?)",
            (uid, float(amount), float(rate), float(total), slug, details, state["destination"], now(), now()),
        )
        order_id = cur.lastrowid
    vals = buy_usdt_values(amount, total, gateway, order_id, uid, state["destination"])
    await update.effective_message.reply_text(render_digital_template(setting_value("buy_usdt_payment_template", ""), vals),
        reply_markup=kb([[(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")]]))
    set_pending(uid, "buy_receipt", {"order_id": order_id})


async def create_buy_usdt_order(q, uid, state, slug):
    gateway = buy_usdt_gateway(slug)
    amount = Decimal(str(state.get("amount", "0")))
    rate = buy_usdt_decimal("buy_usdt_rate")
    stock = buy_usdt_decimal("buy_usdt_stock")
    if rate is None or rate <= 0:
        await q.edit_message_text(setting_value("buy_usdt_rate_error", "Buy rate is not configured."),
                                  reply_markup=kb([[("⬅️ Marketplace", "market")]]))
        return
    if stock is None or amount <= 0 or amount > stock:
        await q.edit_message_text(render_digital_template(setting_value("buy_usdt_stock_error", ""), buy_usdt_values(amount=amount)),
                                  reply_markup=kb([[("⬅️ Marketplace", "market")]]))
        return
    total = (amount * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    details = gateway["details"] or f"{gateway['icon']} {gateway['name']}\nNumber: {gateway['number']}\nName: {gateway['account_name']}"
    with db() as c:
        cur = c.execute(
            "INSERT INTO crypto_orders(user_id,side,amount_usdt,rate_etb,total_etb,payment_method,payment_details,transfer_destination,status,created_at,updated_at) "
            "VALUES(?,'buy',?,?,?,?,?,?,'awaiting_payment_proof',?,?)",
            (uid, float(amount), float(rate), float(total), slug, details, state["destination"], now(), now()),
        )
        order_id = cur.lastrowid
    vals = buy_usdt_values(amount, total, gateway, order_id, uid, state["destination"])
    await q.edit_message_text(render_digital_template(setting_value("buy_usdt_payment_template", ""), vals),
                              reply_markup=kb([[(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")]]))
    set_pending(uid, "buy_receipt", {"order_id": order_id})


async def show_sell_networks(q, payout):
    networks = enabled_sell_networks()
    if not networks:
        await q.edit_message_text(
            "No USDT deposit methods are enabled right now. Please contact an admin.",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return
    with db() as c:
        rate_lines = rates_text("sell")
        rows = [[(label, f"sell_network_{slug}")] for slug, label in networks]
        rows.append([("❌ Cancel | አቋርጥ", "home")])
        summary = (
            f"💸 Sell USDT — Choose deposit method\n\n"
            f"Your ETB payout destination\n• Method: {PAYMENT_METHODS.get(payout.get('method'), payout.get('method', '—'))}\n"
            f"• Account number: {payout.get('account_number', '—')}\n"
            f"• Account name: {payout.get('account_name', '—')}\n\n"
            f"📉 Sell USDT Rates\n{rate_lines}\n\n"
            f"How will you send the USDT?"
        )
    await q.edit_message_text(summary, reply_markup=kb(rows))


async def crypto_callback(update, context, action):
    q = update.callback_query
    uid = q.from_user.id
    if action in ("buy_usdt", "buy_asset", "sell_usdt"):
        await start_crypto_flow(q, context, "sell" if action == "sell_usdt" else "buy")
        return

    if action.startswith("buy_usdt_gateway_"):
        slug = action[len("buy_usdt_gateway_"):]
        if not setting_enabled(f"buy_payment_{slug}_enabled"):
            await q.edit_message_text(setting_value("buy_usdt_no_gateway", "Payment is unavailable."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
        state = decode_pending(pending["data"]) if pending and pending["action"] == "buy_usdt_choose_gateway" else {}
        if not state or "amount" not in state or "destination" not in state:
            await q.edit_message_text(setting_value("buy_usdt_state_expired", "Session expired."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        await create_buy_usdt_order(q, uid, state, slug)
        return

    if action == "buy_usdt_continue_destination":
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
        state = decode_pending(pending["data"]) if pending and pending["action"] == "buy_usdt_destination" else {}
        methods = enabled_buy_methods()
        if not state or not methods:
            await q.edit_message_text(setting_value("buy_usdt_no_gateway", "Payment is unavailable."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        if len(methods) == 1:
            await create_buy_usdt_order(q, uid, state, methods[0][0])
        else:
            set_pending(uid, "buy_usdt_choose_gateway", state)
            rows = [[(label, f"buy_usdt_gateway_{slug}")] for slug, label in methods]
            rows.append([(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")])
            await q.edit_message_text(setting_value("buy_usdt_choose_gateway_prompt", "Choose your ETB payment method:"), reply_markup=kb(rows))
        return

    if action.startswith("buy_method_"):
        slug = action[len("buy_method_"):]
        if slug not in PAYMENT_METHODS or not setting_enabled(f"buy_payment_{slug}_enabled"):
            await q.edit_message_text("That payment method is currently disabled.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        details = setting_value(f"buy_payment_{slug}_details", "").strip()
        if not details:
            await q.edit_message_text("Payment instructions have not been configured for this method yet.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        set_pending(uid, "buy_amount", {"method": slug, "details": details})
        await q.edit_message_text(
            f"🛒 Buy USDT — Step 2\nPayment method: {PAYMENT_METHODS[slug]}\n\n"
            f"Enter the amount of USDT you want to buy (minimum 1 USDT).\n\n"
            f"Current buy rates:\n{rates_text('buy')}",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return

    if action.startswith("sell_payout_"):
        slug = action[len("sell_payout_"):]
        if slug not in PAYMENT_METHODS or not setting_enabled(f"sell_payout_{slug}_enabled"):
            await q.edit_message_text("That payout method is currently disabled.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        set_pending(uid, "sell_account_number", {"method": slug})
        await q.edit_message_text(
            f"💸 Sell USDT — Step 2\n{PAYMENT_METHODS[slug]} details\n\n"
            "Enter the account number where you want to receive ETB. Use digits only:",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return

    if action == "sell_saved":
        with db() as c:
            row = c.execute("SELECT method,account_number,account_name FROM payout_details WHERE user_id=?", (uid,)).fetchone()
        if not row:
            await q.edit_message_text("No saved payout details were found. Please choose a payout method again.", reply_markup=kb([[("⬅️ Back", "sell_usdt")]]))
            return
        payout = {"method": row["method"], "account_number": row["account_number"], "account_name": row["account_name"]}
        set_pending(uid, "sell_network_select", payout)
        await show_sell_networks(q, payout)
        return

    if action.startswith("sell_network_"):
        slug = action[len("sell_network_"):]
        if slug not in SELL_NETWORKS or not setting_enabled(f"sell_network_{slug}_enabled"):
            await q.edit_message_text("That USDT deposit method is currently disabled.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        destination = setting_value(f"sell_network_{slug}_destination", "").strip()
        if not destination:
            await q.edit_message_text("The admin has not configured a deposit destination for this method.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
            payout = decode_pending(pending["data"]) if pending and pending["action"] == "sell_network_select" else {}
        if not payout:
            await q.edit_message_text("Your payout details were not found. Please restart Sell USDT.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        payout.update({"network": slug, "destination": destination})
        set_pending(uid, "sell_amount", payout)
        await q.edit_message_text(
            f"💸 Sell USDT — Enter amount\nDeposit method: {SELL_NETWORKS[slug]}\n"
            f"Current sell rates:\n{rates_text('sell')}\n\nEnter the amount of USDT you will send (minimum 1 USDT).",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return

    if action == "admin_crypto_orders":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        with db() as c:
            orders = c.execute(
                "SELECT id,user_id,side,amount_usdt,total_etb,status FROM crypto_orders "
                "WHERE status='pending_admin_approval' ORDER BY id LIMIT 10"
            ).fetchall()
        rows = []
        for order in orders:
            rows.append([
                (f"#{order['id']} {order['side'].upper()} {order['amount_usdt']:g} USDT · {order['total_etb']:g} ETB", f"crypto_order_view_{order['id']}")
            ])
        rows.append([("⬅️ Admin Dashboard", "admin")])
        await q.edit_message_text("🪙 Crypto orders awaiting payment verification:", reply_markup=kb(rows))
        return

    if action.startswith("crypto_order_view_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        raw_id = action.rsplit("_", 1)[-1]
        with db() as c:
            order = c.execute("SELECT * FROM crypto_orders WHERE id=?", (int(raw_id),)).fetchone()
        if not order:
            await q.edit_message_text("Order not found.")
            return
        method = order["payment_method"] or order["transfer_method"] or "—"
        details = order["payment_details"] or order["transfer_destination"] or "—"
        msg = (
            f"🪙 Crypto order #{order['id']}\nSide: {order['side'].upper()}\n"
            f"User: {order['user_id']}\nAmount: {order['amount_usdt']:g} USDT\n"
            f"ETB total: {order['total_etb']:g}\nMethod: {method}\nDestination/details: {details}\n"
            f"Status: {order['status']}"
        )
        rows = []
        if order["status"] == "pending_admin_approval":
            prefix = "buyorder" if order["side"] == "buy" else "sellorder"
            rows.append([("✅ Verify payment & continue", f"{prefix}_verify_{order['id']}"),
                         ("❌ Reject order", f"{prefix}_reject_{order['id']}")])
        rows.append([("⬅️ Crypto orders", "admin_crypto_orders")])
        await q.edit_message_text(msg, reply_markup=kb(rows))
        return

    if action.startswith(("buyorder_verify_", "sellorder_verify_")):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        order_id = int(action.rsplit("_", 1)[-1])
        with db() as c:
            order = c.execute("SELECT * FROM crypto_orders WHERE id=?", (order_id,)).fetchone()
            changed = False
            if order and order["status"] == "pending_admin_approval":
                cur = c.execute(
                    "UPDATE crypto_orders SET status='payment_verified',admin_id=?,updated_at=? "
                    "WHERE id=? AND status='pending_admin_approval'",
                    (uid, now(), order_id),
                )
                changed = cur.rowcount == 1
        if not order or not changed:
            await q.edit_message_text("This order was already handled or could not be found.")
            return
        set_pending(uid, "crypto_delivery", {"order_id": order_id})
        side_text = "USDT delivery" if order["side"] == "buy" else "ETB payout"
        try:
            await context.bot.send_message(
                order["user_id"],
                f"✅ Payment for order #{order_id} has been verified. The admin is preparing your {side_text}.",
            )
        except Exception:
            log.warning("Could not notify crypto order user %s", order["user_id"])
        await q.edit_message_text(
            f"Payment verified for order #{order_id}.\nNow send the {side_text} to the user as a text message, photo, or document. "
            "The order will be marked completed after you send it.",
            reply_markup=kb([[("⬅️ Crypto orders", "admin_crypto_orders")], [("⬅️ Admin Dashboard", "admin")]]),
        )
        return

    if action.startswith(("buyorder_reject_", "sellorder_reject_")):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        order_id = int(action.rsplit("_", 1)[-1])
        with db() as c:
            order = c.execute("SELECT user_id,status FROM crypto_orders WHERE id=?", (order_id,)).fetchone()
            changed = False
            if order and order["status"] == "pending_admin_approval":
                cur = c.execute("UPDATE crypto_orders SET status='rejected',admin_id=?,updated_at=? WHERE id=? AND status='pending_admin_approval'", (uid, now(), order_id))
                changed = cur.rowcount == 1
        if changed:
            try:
                await context.bot.send_message(order["user_id"], f"❌ Your crypto order #{order_id} was rejected by an admin. Please contact support if you need help.")
            except Exception:
                log.warning("Could not notify crypto order user %s", order["user_id"])
            await q.edit_message_text(f"Order #{order_id} rejected.", reply_markup=kb([[("⬅️ Crypto orders", "admin_crypto_orders")], [("⬅️ Admin Dashboard", "admin")]]))
        else:
            await q.edit_message_text("This order was already handled or could not be found.")
        return


async def handle_crypto_text(update, context, action, data, value):
    user = update.effective_user
    message = update.effective_message
    uid = user.id
    state = decode_pending(data)

    if action == "buy_usdt_amount":
        try:
            amount = Decimal(value.replace(",", "."))
            if not amount.is_finite() or amount <= 0 or amount.as_tuple().exponent < -8:
                raise InvalidOperation()
        except (InvalidOperation, ValueError):
            await message.reply_text(setting_value("buy_usdt_amount_invalid", "Please enter a valid positive USDT amount."))
            return True
        minimum, maximum, stock = (buy_usdt_decimal(k) for k in ("buy_usdt_min", "buy_usdt_max", "buy_usdt_stock"))
        if minimum is None or maximum is None or stock is None or minimum <= 0 or maximum < minimum or stock < 0:
            await message.reply_text(setting_value("buy_usdt_config_error", "Buy USDT limits are not configured correctly."))
            return True
        if amount < minimum or amount > maximum:
            await message.reply_text(render_digital_template(setting_value("buy_usdt_amount_range_error", ""),
                                                              buy_usdt_values(amount=amount)))
            return True
        if amount > stock:
            await message.reply_text(render_digital_template(setting_value("buy_usdt_stock_error", ""),
                                                              buy_usdt_values(amount=amount)))
            return True
        rate = buy_usdt_decimal("buy_usdt_rate")
        if rate is None or rate <= 0:
            await message.reply_text(setting_value("buy_usdt_rate_error", "Buy rate is not configured."))
            return True
        set_pending(uid, "buy_usdt_destination", {"amount": str(amount)})
        await message.reply_text(setting_value("buy_usdt_destination_template", ""),
            reply_markup=kb([[(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")]]))
        return True

    if action == "buy_usdt_destination":
        if not value or len(value) > 180:
            await message.reply_text(setting_value("buy_usdt_destination_invalid", "Enter a valid receiving ID."))
            return True
        amount = Decimal(str(state.get("amount", "0")))
        stock = buy_usdt_decimal("buy_usdt_stock")
        if stock is None or amount > stock:
            await message.reply_text(render_digital_template(setting_value("buy_usdt_stock_error", ""), buy_usdt_values(amount=amount)))
            return True
        state["destination"] = value
        methods = enabled_buy_methods()
        if not methods:
            await message.reply_text(setting_value("buy_usdt_no_gateway", "Payment is unavailable."))
            return True
        if len(methods) == 1:
            await create_buy_usdt_order_for_message(update, context, uid, state, methods[0][0])
        else:
            set_pending(uid, "buy_usdt_choose_gateway", state)
            rows = [[(label, f"buy_usdt_gateway_{slug}")] for slug, label in methods]
            rows.append([(setting_value("buy_usdt_cancel_button", "❌ Cancel | አቋርጥ"), "home")])
            await message.reply_text(setting_value("buy_usdt_choose_gateway_prompt", "Choose your ETB payment method:"), reply_markup=kb(rows))
        return True

    if action == "buy_amount":
        try:
            amount = float(value.replace(",", "."))
            if not math.isfinite(amount) or amount < 1 or amount > 100000000 or (not amount.is_integer() and len(value.split(".")[-1]) > 8):
                raise ValueError()
        except ValueError:
            await message.reply_text("Enter a valid USDT amount of at least 1 (up to 8 decimal places).")
            return True
        rate = rate_for("buy", amount)
        if not rate:
            await message.reply_text("The admin has not configured the buy rate for this amount tier yet. Please contact support.")
            return True
        total = round(amount * rate, 2)
        with db() as c:
            cur = c.execute(
                "INSERT INTO crypto_orders(user_id,side,amount_usdt,rate_etb,total_etb,payment_method,payment_details,status,created_at,updated_at) "
                "VALUES(?,'buy',?,?,?,?,?,'awaiting_payment_proof',?,?)",
                (uid, amount, rate, total, state["method"], state["details"], now(), now()),
            )
            order_id = cur.lastrowid
            c.execute(
                "UPDATE pending_inputs SET action='buy_receipt',data=? WHERE user_id=?",
                (json.dumps({"order_id": order_id}), uid),
            )
        await message.reply_text(
            f"🛒 Buy USDT — Order #{order_id}\nAmount: {amount:g} USDT\nRate: {rate:g} ETB/USDT\n"
            f"Total to pay: {total:g} ETB\n\nPayment method: {PAYMENT_METHODS.get(state['method'], state['method'])}\n"
            f"Payment instructions:\n{state['details']}\n\nAfter paying, upload a clear screenshot showing amount, recipient, status, and transaction reference. "
            "Your screenshot is reviewed manually; it does not automatically confirm payment.",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return True

    if action == "sell_account_number":
        digits = "".join(ch for ch in value if ch.isdigit())
        if not digits or digits != value.strip():
            await message.reply_text("Please enter the account number using digits only.")
            return True
        state["account_number"] = digits
        set_pending(uid, "sell_account_name", state)
        await message.reply_text(
            "Enter the account holder name exactly as registered with the bank/Telebirr:",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return True

    if action == "sell_account_name":
        if len(value) < 2 or len(value) > 120:
            await message.reply_text("Please enter the account holder name (2–120 characters).")
            return True
        state["account_name"] = value
        with db() as c:
            c.execute(
                "INSERT INTO payout_details(user_id,method,account_number,account_name,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET method=excluded.method,account_number=excluded.account_number,"
                "account_name=excluded.account_name,updated_at=excluded.updated_at",
                (uid, state["method"], state["account_number"], state["account_name"], now()),
            )
        set_pending(uid, "sell_network_select", state)
        await message.reply_text("✅ Payout details saved for next time.")
        await message.reply_text(
            f"💸 Sell USDT — Choose deposit method\n\nPayout method: {PAYMENT_METHODS.get(state['method'], state['method'])}\n"
            f"Account number: {state['account_number']}\nAccount name: {state['account_name']}\n\n"
            f"📉 Sell USDT rates:\n{rates_text('sell')}\n\nChoose how you will send USDT:",
            reply_markup=kb([[(label, f"sell_network_{slug}")] for slug, label in enabled_sell_networks()] + [[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return True

    if action == "sell_amount":
        try:
            amount = float(value.replace(",", "."))
            if not math.isfinite(amount) or amount < 1 or amount > 100000000 or (not amount.is_integer() and len(value.split(".")[-1]) > 8):
                raise ValueError()
        except ValueError:
            await message.reply_text("Enter a valid USDT amount of at least 1 (up to 8 decimal places).")
            return True
        rate = rate_for("sell", amount)
        if not rate:
            await message.reply_text("The admin has not configured the sell rate for this amount tier yet. Please contact support.")
            return True
        total = round(amount * rate, 2)
        with db() as c:
            cur = c.execute(
                "INSERT INTO crypto_orders(user_id,side,amount_usdt,rate_etb,total_etb,payout_method,payout_account_number,payout_account_name,transfer_method,transfer_destination,status,created_at,updated_at) "
                "VALUES(?,'sell',?,?,?,?,?,?,?,?,'awaiting_payment_proof',?,?)",
                (uid, amount, rate, total, state["method"], state["account_number"], state["account_name"],
                 state["network"], state["destination"], now(), now()),
            )
            order_id = cur.lastrowid
            c.execute("UPDATE pending_inputs SET action='sell_receipt',data=? WHERE user_id=?", (json.dumps({"order_id": order_id}), uid))
        await message.reply_text(
            f"💸 Sell USDT — Order #{order_id}\nAmount: {amount:g} USDT\nSell rate: {rate:g} ETB/USDT\n"
            f"Expected ETB payout: {total:g} ETB\n\nSend USDT using {SELL_NETWORKS.get(state['network'], state['network'])} to:\n"
            f"{state['destination']}\n\nAfter sending, upload a clear transfer screenshot showing amount, status, recipient, time, and transaction/order ID. "
            "The admin will verify the transfer before sending your ETB payout.",
            reply_markup=kb([[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return True

    if action == "crypto_delivery":
        if not is_admin(uid):
            await message.reply_text("Only an admin can deliver crypto orders.")
            return True
        order_id = int(state.get("order_id", 0))
        with db() as c:
            order = c.execute("SELECT user_id,side,status FROM crypto_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["status"] != "payment_verified":
            await message.reply_text("This order is not awaiting delivery.")
            return True
        try:
            await context.bot.send_message(order["user_id"], f"📦 Delivery for crypto order #{order_id}\n\n{value}")
        except Exception:
            await message.reply_text("Could not deliver the message to the user. The order remains open.")
            log.exception("Could not deliver crypto order %s to user %s", order_id, order["user_id"])
            return True
        with db() as c:
            c.execute("UPDATE crypto_orders SET status='completed',delivery_ref=?,updated_at=? WHERE id=? AND status='payment_verified'",
                      ("message:" + value[:500], now(), order_id))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await message.reply_text(f"✅ Message sent to user. Crypto order #{order_id} marked completed.")
        try:
            await context.bot.send_message(order["user_id"], f"✅ Order #{order_id} is marked completed. Contact support if you need assistance.")
        except Exception:
            pass
        return True
    return False


async def deliver_crypto_media(update, context, pending):
    message = update.effective_message
    admin = update.effective_user
    state = decode_pending(pending["data"])
    order_id = int(state.get("order_id", 0))
    with db() as c:
        order = c.execute("SELECT user_id,side,status FROM crypto_orders WHERE id=?", (order_id,)).fetchone()
    if not is_admin(admin.id) or not order or order["status"] != "payment_verified":
        await message.reply_text("This order is not awaiting admin delivery.")
        return
    caption = message.caption or f"Payment/delivery proof for crypto order #{order_id}"
    try:
        if message.photo:
            await context.bot.send_photo(order["user_id"], message.photo[-1].file_id, caption=f"📦 Delivery for crypto order #{order_id}\n{caption}")
            file_ref = "photo:" + message.photo[-1].file_id
        elif message.document:
            await context.bot.send_document(order["user_id"], message.document.file_id, caption=f"📦 Delivery for crypto order #{order_id}\n{caption}")
            file_ref = "document:" + message.document.file_id
        else:
            await message.reply_text("Please send a photo or document.")
            return
    except Exception:
        log.exception("Could not deliver crypto order %s to user %s", order_id, order["user_id"])
        await message.reply_text("Could not deliver the file to the user. The order remains open.")
        return
    with db() as c:
        c.execute("UPDATE crypto_orders SET status='completed',delivery_ref=?,updated_at=? WHERE id=? AND status='payment_verified'",
                  (file_ref, now(), order_id))
        c.execute("DELETE FROM pending_inputs WHERE user_id=?", (admin.id,))
    await message.reply_text(f"✅ Delivery sent. Crypto order #{order_id} marked completed.")
    try:
        await context.bot.send_message(order["user_id"], f"✅ Order #{order_id} is marked completed. Contact support if you need assistance.")
    except Exception:
        pass



# Dynamic digital-goods catalogue and receipt approval flow.
def digital_template(key, fallback=""):
    return setting_value(key, fallback) or ""


def digital_products(active_only=True):
    with db() as c:
        sql = "SELECT * FROM digital_products" + (" WHERE active=1" if active_only else "") + " ORDER BY name COLLATE NOCASE"
        return c.execute(sql).fetchall()


def digital_product(product_id):
    with db() as c:
        return c.execute("SELECT * FROM digital_products WHERE id=?", (product_id,)).fetchone()


def digital_gateways():
    with db() as c:
        return c.execute("SELECT * FROM digital_payment_gateways WHERE enabled=1 ORDER BY name COLLATE NOCASE").fetchall()


def render_digital_template(template, values):
    # Templates are admin-editable in SQLite. Unknown placeholders remain readable.
    class SafeValues(dict):
        def __missing__(self, key):
            return "{" + key + "}"
    try:
        return str(template).format_map(SafeValues(values))
    except Exception:
        log.exception("Invalid digital-goods message template")
        return str(template)


def digital_product_values(product):
    features = "\n".join(
        f"• {line.strip()}" for line in (product["features"] or "").splitlines() if line.strip()
    ) or "—"
    return {
        "id": product["id"], "name": product["name"],
        "duration": product["duration_months"], "price": f"{float(product['price']):g}",
        "stock": product["stock"], "description": product["description"] or "—",
        "features": features, "note": product["important_note"] or "—",
        "notice": product["notice"] or "—", "warranty": product["warranty"] or "—",
    }


async def digital_callback(update, context, action):
    q = update.callback_query
    uid = q.from_user.id
    if action.startswith("digital_product_"):
        product_id = action[len("digital_product_"):]
        product = digital_product(product_id)
        if not product or not product["active"]:
            await q.edit_message_text(digital_template("digital_no_stock_message", "Product unavailable."))
            return
        values = digital_product_values(product)
        body = render_digital_template(
            digital_template("digital_product_details_template"), values
        )
        await q.edit_message_text(body, reply_markup=kb([
            [("🌟 Buy now", f"digital_buy_{product_id}")],
            [("❌ Cancel | አቋርጥ", "digital_cancel")]
        ]))
        return

    if action == "digital_cancel":
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text(
            digital_template("digital_cancel_message", "Purchase cancelled."),
            reply_markup=kb([[("⬅️ Marketplace", "market")], [("⬅️ Dashboard", "home")]])
        )
        return

    if action.startswith("digital_buy_"):
        product_id = action[len("digital_buy_"):]
        product = digital_product(product_id)
        if not product or not product["active"]:
            await q.edit_message_text(digital_template("digital_no_stock_message", "Product unavailable."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        if int(product["stock"]) <= 0:
            await q.edit_message_text(digital_template("digital_no_stock_message", "This product is out of stock."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        gateways = digital_gateways()
        if not gateways:
            await q.edit_message_text(digital_template("digital_no_gateway_message", "Payment unavailable."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        if len(gateways) == 1:
            await show_digital_payment(q, uid, product, gateways[0])
            return
        rows = [[(g["name"], f"digital_gateway_{product_id}::{g['id']}")] for g in gateways]
        rows.append([("❌ Cancel | አቋርጥ", "digital_cancel")])
        set_pending(uid, "digital_choose_gateway", {"product_id": product_id})
        await q.edit_message_text("Choose your payment method:", reply_markup=kb(rows))
        return

    if action.startswith("digital_gateway_"):
        parts = action[len("digital_gateway_"):].split("::", 1)
        if len(parts) != 2:
            await q.edit_message_text("Invalid payment option.")
            return
        product_id, gateway_id = parts
        product = digital_product(product_id)
        with db() as c:
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=? AND enabled=1", (gateway_id,)).fetchone()
        if not product or not gateway or int(product["stock"]) <= 0:
            await q.edit_message_text(digital_template("digital_no_stock_message", "Product or payment option unavailable."),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        await show_digital_payment(q, uid, product, gateway)
        return

    if action == "admin_digital_products":
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        products = digital_products(active_only=False)
        rows = [[(f"{p['name']} · {p['duration_months']}m · {p['price']:g} ETB · stock {p['stock']}",
                  f"digital_admin_product_{p['id']}")] for p in products]
        rows.extend([
            [("📥 Pending digital orders", "admin_digital_orders")],
            [("➕ Add product instructions", "digital_admin_help")],
            [("⬅️ Admin Dashboard", "admin")]
        ])
        await q.edit_message_text(
            "Digital products are database-driven. Select a product to view its ID and editable fields.",
            reply_markup=kb(rows)
        )
        return

    if action == "digital_admin_help":
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        await q.edit_message_text(
            "Admin commands (all values are stored in the database):\n"
            "/product_add ID | NAME | MONTHS | PRICE | STOCK\n"
            "/product_set ID FIELD VALUE\n"
            "Fields: name, duration_months, price, stock, description, features, important_note, notice, warranty, active\n"
            "/gateway_set ID | NAME | ACCOUNT_NUMBER | ACCOUNT_NAME | INSTRUCTIONS | WARNING\n"
            "/gateway_toggle ID true/false\n"
            "/digital_waiting MESSAGE\n"
            "/digital_admin_chat CHAT_ID (optional review group/channel; use /digital_admin_chat none to clear)\n"
            "/digital_text KEY MESSAGE (edit templates/messages)\n"
            "For multi-line values, use the command and separate fields with | where supported."
        )
        return

    if action.startswith("digital_admin_product_"):
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        product_id = action[len("digital_admin_product_"):]
        product = digital_product(product_id)
        if not product:
            await q.edit_message_text("Product not found.")
            return
        await q.edit_message_text(
            f"Product ID: {product['id']}\nName: {product['name']}\nDuration: {product['duration_months']} months\n"
            f"Price: {product['price']:g} ETB\nStock: {product['stock']}\nActive: {product['active']}\n"
            f"Description: {product['description']}\nFeatures: {product['features']}\n"
            f"Important note: {product['important_note']}\nNotice: {product['notice']}\nWarranty: {product['warranty']}\n\n"
            f"Edit with /product_set {product_id} FIELD VALUE",
            reply_markup=kb([[("📥 Pending digital orders", "admin_digital_orders")],
                             [("⬅️ Digital Products", "admin_digital_products")]])
        )
        return

    if action == "admin_digital_orders":
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        with db() as c:
            orders = c.execute(
                "SELECT id,user_id,product_name,duration_months,price,gateway_name,status FROM digital_orders "
                "WHERE status='pending_approval' ORDER BY id DESC LIMIT 20"
            ).fetchall()
        rows = [[(f"Order #{o['id']} · {o['product_name']} {o['duration_months']}m · {o['price']:g} ETB · user {o['user_id']}",
                  f"digital_order_view_{o['id']}")] for o in orders]
        rows.append([("⬅️ Digital Products", "admin_digital_products")])
        await q.edit_message_text("Pending digital-goods orders:", reply_markup=kb(rows))
        return

    if action.startswith("digital_order_view_"):
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        try:
            order_id = int(action[len("digital_order_view_"):])
        except ValueError:
            await q.edit_message_text("Invalid order ID.")
            return
        with db() as c:
            order = c.execute("SELECT * FROM digital_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await q.edit_message_text("Order not found.")
            return
        await q.edit_message_text(
            f"Digital order #{order_id}\nUser ID: {order['user_id']}\nProduct: {order['product_name']} "
            f"{order['duration_months']}m\nPrice: {order['price']:g} ETB\nPayment: {order['gateway_name']}\n"
            f"Status: {order['status']}\nCreated: {order['created_at']}",
            reply_markup=kb([[("✉️ Reply / Send redeem link", f"digital_reply_{order_id}")],
                             [("🚫 Reject with message", f"digital_reject_{order_id}")],
                             [("⬅️ Pending orders", "admin_digital_orders")]])
        )
        return

    if action.startswith(("digital_reply_", "digital_reject_")):
        if not is_admin(uid):
            await q.edit_message_text("Admin access only.")
            return
        reject = action.startswith("digital_reject_")
        order_id = int(action.rsplit("_", 1)[1])
        with db() as c:
            order = c.execute("SELECT id,status FROM digital_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["status"] != "pending_approval":
            await q.edit_message_text("This order is not pending approval.")
            return
        set_pending(uid, "digital_admin_reply", {"order_id": order_id, "reject": reject})
        await q.edit_message_text(
            f"Send the custom {'rejection reason' if reject else 'activation link / message'} for order #{order_id} as your next text message."
        )
        return


async def show_digital_payment(q, uid, product, gateway):
    values = digital_product_values(product)
    values.update({
        "gateway_name": gateway["name"], "account_number": gateway["account_number"],
        "account_name": gateway["account_name"], "instructions": gateway["instructions"] or "—",
        "warning": gateway["warning"] or "—",
    })
    text_body = render_digital_template(digital_template("digital_payment_template"), values)
    set_pending(uid, "digital_receipt", {
        "product_id": product["id"], "gateway_id": gateway["id"],
        "product_name": product["name"], "duration_months": int(product["duration_months"]),
        "price": float(product["price"]), "gateway_name": gateway["name"],
    })
    await q.edit_message_text(text_body, reply_markup=kb([[("❌ Cancel | አቋርጥ", "digital_cancel")]]))


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    action = q.data
    if action.startswith(("digital_product_", "digital_buy_", "digital_gateway_", "digital_admin_product_",
                          "digital_order_view_", "digital_reply_", "digital_reject_")) or action in (
        "digital_cancel", "admin_digital_products", "admin_digital_orders", "digital_admin_help"
    ):
        await digital_callback(update, context, action)
        return
    if action in ("buy_usdt", "buy_asset", "sell_usdt", "sell_saved", "admin_crypto_orders") or action.startswith((
        "buy_method_", "sell_payout_", "sell_network_", "crypto_order_view_",
        "buyorder_verify_", "sellorder_verify_", "buyorder_reject_", "sellorder_reject_"
    )):
        await crypto_callback(update, context, action)
        return
    if action == "home":
        # Dashboard acts as Cancel for any unfinished text-input flow.
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("🏠 Main Dashboard", reply_markup=home_keyboard(is_admin(uid)))
    elif action == "jobs":
        with db() as c:
            tasks = c.execute("SELECT * FROM tasks WHERE active=1 AND completed_count<target ORDER BY id DESC").fetchall()
            claims = {r["task_id"]: r["status"] for r in c.execute("SELECT task_id,status FROM task_claims WHERE user_id=?", (uid,)).fetchall()}
        if not tasks:
            await q.edit_message_text("🧩 No jobs are available right now. Please check back later.", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
        rows = []
        for t in tasks[:20]:
            status = claims.get(t["id"])
            label = f"{'⏳ ' if status else '✅ '}{t['title']} · {t['points']} pts · {t['completed_count']}/{t['target']}"
            rows.append([(label[:60], f"task_{t['id']}")])
        rows.append([("⬅️ Dashboard", "home")])
        await q.edit_message_text("🧩 Available Jobs\nChoose a job to view its rules and progress:", reply_markup=kb(rows))
    elif action.startswith("task_"):
        tid = int(action.split("_",1)[1])
        with db() as c:
            t = c.execute("SELECT * FROM tasks WHERE id=? AND active=1", (tid,)).fetchone()
            claim = c.execute("SELECT status,invite_link FROM task_claims WHERE task_id=? AND user_id=?", (tid,uid)).fetchone()
        if not t:
            await q.edit_message_text("This task is no longer available.", reply_markup=kb([[("⬅️ Jobs","jobs")]])); return
        status = claim["status"] if claim else "not started"
        invite = claim["invite_link"] if claim else ""
        if not claim and t["completed_count"] < t["target"]:
            try:
                link_obj = await context.bot.create_chat_invite_link(
                    chat_id=t["channel"],
                    name=f"kefia-task-{tid}-user-{uid}"
                )
                invite = link_obj.invite_link
                with db() as c:
                    c.execute("INSERT OR IGNORE INTO task_claims(task_id,user_id,invite_link,status,created_at) VALUES(?,?,?,'pending',?)",
                              (tid,uid,invite,now()))
                    c.execute("INSERT OR IGNORE INTO invite_links(invite_link,task_id,owner_user_id,created_at) VALUES(?,?,?,?)",
                              (invite,tid,uid,now()))
            except Exception:
                log.exception("Could not create invite link for task %s", tid)
                await q.edit_message_text(
                    "⚠️ This task is temporarily unavailable. The bot needs admin permission to create invitation links in the target channel.",
                    reply_markup=kb([[("⬅️ Jobs","jobs")]])
                ); return
        msg = (f"🧩 {t['title']}\nChannel: {t['channel']}\nReward: {t['points']} points\n"
               f"Target: {t['target']} verified joins\nProgress: {t['completed_count']}/{t['target']}\n"
               f"Your status: {status}")
        if invite:
            msg += f"\n\nYour unique invitation link (share it with real users):\n{invite}"
        msg += "\n\nOnly new, unique joins tracked by Telegram count. No self-referrals or fake accounts."
        await q.edit_message_text(msg, reply_markup=kb([[("🔄 Refresh Progress",f"task_{tid}")],[("⬅️ Jobs","jobs")]]))
    elif action == "invite":
        bot = await context.bot.get_me()
        link = f"https://t.me/{bot.username}?start=ref_{uid}"
        with db() as c:
            invited = c.execute("SELECT COUNT(*) n FROM users WHERE referred_by=?", (uid,)).fetchone()["n"]
        await q.edit_message_text(
            f"🔗 Invite & Earn\nYour personal bot invite link:\n{link}\n\n"
            f"Registered referrals: {invited}\n\nReferral points are only added when an active reward campaign is configured and the referral is verified.",
            reply_markup=kb([[("⬅️ Dashboard","home")]])
        )
    elif action == "wallet":
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            pending = c.execute("SELECT COUNT(*) n FROM withdrawals WHERE user_id=? AND status='pending'", (uid,)).fetchone()["n"]
        await q.edit_message_text(f"👛 Wallet\nAvailable points: {u['points'] if u else 0}\nPending withdrawals: {pending}\nPoints have no cash value until the admin sets a conversion and withdrawal policy.", reply_markup=kb([[("💸 Withdraw","withdraw")],[("⬅️ Dashboard","home")]]))
    elif action == "profile":
        with db() as c:
            u = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        await q.edit_message_text(f"👤 Account\nName: {q.from_user.full_name}\nUser ID: {uid}\nPoints: {u['points'] if u else 0}\nMember since: {u['joined_at'][:10] if u else '—'}", reply_markup=kb([[("⬅️ Dashboard","home")]]))
    elif action == "withdraw":
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
        minimum_points = int(minimum["value"]) if minimum else 1000
        points = u["points"] if u else 0
        if points < minimum_points:
            await q.edit_message_text(f"💸 Withdrawal unavailable yet.\nYour points: {points}\nMinimum: {minimum_points} points.\nAdmins can change this limit.", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,'withdraw','') ON CONFLICT(user_id) DO UPDATE SET action='withdraw',data=''", (uid,))
        await q.edit_message_text("Enter withdrawal method and details in one message (example: Telebirr, account/phone). Your request will be reviewed by an admin.", reply_markup=kb([[("Cancel","home")]]))
    elif action == "ads":
        await q.edit_message_text("📣 Promotion Center\nChoose a promotion service:", reply_markup=kb([
            [("🚀 Promote my product","ad_product")],
            [("👥 Get channel members","ad_members")],
            [("👁️ Get views / reach","ad_views")],
            [("📋 My ad requests","my_ads")],
            [("⬅️ Dashboard","home")]
        ]))
    elif action in ("ad_product","ad_members","ad_views"):
        labels = {"ad_product":"Product promotion","ad_members":"Channel member campaign","ad_views":"Views / reach campaign"}
        with db() as c:
            price = c.execute("SELECT value FROM settings WHERE key=?", (f"price_{action}",)).fetchone()
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,data=excluded.data",
                      (uid, action, labels[action]))
        price_text = f"Admin-set starting price: {price['value']} ETB" if price else "Price: awaiting admin configuration"
        await q.edit_message_text(f"📣 {labels[action]}\n{price_text}\n\nSend your product/channel link, what you want promoted, and desired duration (e.g. 1 day). Admin will confirm the final quote before you pay.", reply_markup=kb([[("Cancel","home")]]))
    elif action == "my_ads":
        with db() as c:
            rows = c.execute("SELECT id,kind,status,quoted_price FROM ad_requests WHERE user_id=? ORDER BY id DESC LIMIT 10", (uid,)).fetchall()
        msg = "📋 Your ad requests\n" + ("\n".join(f"#{r['id']} · {r['kind']} · {r['status']} · {r['quoted_price'] if r['quoted_price'] is not None else 'quote pending'} ETB" for r in rows) if rows else "No ad requests yet.")
        await q.edit_message_text(msg, reply_markup=kb([[("⬅️ Promotions","ads")],[("⬅️ Dashboard","home")]]))
    elif action == "market":
        rows = [
            [(setting_value("buy_usdt_menu_button", "💵 Buy USDT | USDT ይግዙ"),"buy_usdt")],
            [("📲 Buy social-media promotion/accounts","buy_social")],
            [("💸 Sell USDT | USDT ይሽጡ","sell_usdt")],
            [("📤 Sell a social-media asset","sell_social")],
            [("📋 My listings","my_market")],
        ]
        for product in digital_products():
            label = f"🌟 {product['name']} {product['duration_months']}m ({product['price']:g} ETB)"
            rows.append([(label[:60], f"digital_product_{product['id']}")])
        rows.append([("⬅️ Dashboard","home")])
        await q.edit_message_text("🛍 Marketplace — choose what you want to do:", reply_markup=kb(rows))
    elif action in ("buy_social","sell_social"):
        labels = {"buy_social":"buy a listed social-media service/asset",
                  "sell_social":"submit a social-media asset for review"}
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,data=excluded.data",
                      (uid,action,labels[action]))
        await q.edit_message_text(f"🛍 You selected: {labels[action]}.\n\nSend details in one message: asset/service, amount, link (if applicable), and your expected price. Social-media monetization and ownership are manually reviewed; never send passwords, seed phrases, or private keys.", reply_markup=kb([[("Cancel","home")]]))
    elif action == "admin":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        await q.edit_message_text("🛡 Admin Dashboard\nManage tasks, review payouts and listings, configure prices, and inspect platform statistics.", reply_markup=kb([
            [("➕ Create join task","admin_new_task"),("📊 Statistics","admin_stats")],
            [("📥 Review requests","admin_queue"),("🪙 Crypto orders","admin_crypto_orders")],
            [("⚙️ Set prices / limits","admin_settings")],
            [("🌟 Digital Products / Orders","admin_digital_products")],
            [("⬅️ Dashboard","home")]
        ]))
    elif action == "admin_new_task":
        if not is_admin(uid): return
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,'admin_task_title','') ON CONFLICT(user_id) DO UPDATE SET action='admin_task_title',data=''", (uid,))
        await q.edit_message_text("Send task title, target channel username/ID, target number of verified joins, and points reward separated by |\nExample: Join Channel | @ExampleChannel | 50 | 20\nThe bot must be an administrator in the channel with invite-link permissions.", reply_markup=kb([[("Cancel","admin")]]))
    elif action == "admin_stats":
        if not is_admin(uid): return
        with db() as c:
            users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            tasks = c.execute("SELECT COUNT(*) n FROM tasks").fetchone()["n"]
            ads = c.execute("SELECT COUNT(*) n FROM ad_requests WHERE status='pending'").fetchone()["n"]
            wd = c.execute("SELECT COUNT(*) n FROM withdrawals WHERE status='pending'").fetchone()["n"]
            listings = c.execute("SELECT COUNT(*) n FROM market_listings WHERE status='pending'").fetchone()["n"]
            joins = c.execute("SELECT COUNT(*) n FROM invite_events").fetchone()["n"]
        await q.edit_message_text(f"📊 Platform Statistics\nUsers: {users}\nTasks: {tasks}\nTracked unique joins: {joins}\nPending ad requests: {ads}\nPending withdrawals: {wd}\nPending marketplace listings: {listings}", reply_markup=kb([[("⬅️ Admin Dashboard","admin")]]))
    elif action == "admin_settings":
        if not is_admin(uid): return
        with db() as c:
            settings = c.execute("SELECT key,value FROM settings ORDER BY key").fetchall()
        msg = "⚙️ Current settings\n" + ("\n".join(f"{r['key']} = {r['value']}" for r in settings) if settings else "No custom settings configured.")
        msg += ("\n\nUse /set KEY VALUE to update settings. Crypto examples:\n"
                "/set buy_usdt_rate_1_2 150\n/set buy_usdt_rate_2_5 148\n/set buy_usdt_rate_5_plus 145\n"
                "/set buy_payment_cbe_enabled true\n/set buy_payment_cbe_details CBE account details here\n"
                "/set sell_payout_telebirr_enabled true\n/set sell_network_bsc_enabled true\n"
                "/set sell_network_bsc_destination YOUR_ADDRESS\n/set buy_enabled false\n"
                "/set buy_unavailable_message Currently unavailable\n"
                "Use value 'none' to clear an unavailable message. See the admin docs/code for supported setting keys.")
        await q.edit_message_text(msg, reply_markup=kb([[("⬅️ Admin Dashboard","admin")]]))
    elif action == "admin_queue":
        if not is_admin(uid): return
        with db() as c:
            ads = c.execute("SELECT id,user_id,kind,status FROM ad_requests WHERE status IN ('pending','receipt_submitted') ORDER BY id LIMIT 8").fetchall()
            wds = c.execute("SELECT id,user_id,points,status FROM withdrawals WHERE status='pending' ORDER BY id LIMIT 8").fetchall()
            mks = c.execute("SELECT id,user_id,action,asset_type,status FROM market_listings WHERE status='pending' ORDER BY id LIMIT 8").fetchall()
        rows = []
        for x in ads: rows.append([(f"Approve ad #{x['id']} · user {x['user_id']}",f"approve_ad_{x['id']}"),( "Reject",f"reject_ad_{x['id']}")])
        for x in wds: rows.append([(f"Approve withdrawal #{x['id']} · {x['points']} pts",f"approve_wd_{x['id']}"),("Reject",f"reject_wd_{x['id']}")])
        for x in mks: rows.append([(f"Approve listing #{x['id']} · {x['asset_type']}",f"approve_mk_{x['id']}"),("Reject",f"reject_mk_{x['id']}")])
        rows.append([("⬅️ Admin Dashboard","admin")])
        await q.edit_message_text("📥 Pending requests. Approval updates status; confirm any real payment manually before marking it paid.", reply_markup=kb(rows))
    elif action.startswith(("approve_ad_","reject_ad_","approve_wd_","reject_wd_","approve_mk_","reject_mk_")):
        if not is_admin(uid): return
        verb, typ, rawid = action.split("_",2)
        table = {"ad":"ad_requests","wd":"withdrawals","mk":"market_listings"}[typ]
        status = "approved" if verb == "approve" else "rejected"
        with db() as c:
            select_extra = ", points" if typ == "wd" else ""
            row = c.execute(f"SELECT user_id, status{select_extra} FROM {table} WHERE id=?", (int(rawid),)).fetchone()
            changed = False
            allowed_statuses = ("pending", "receipt_submitted") if typ == "ad" else ("pending",)
            if row and row["status"] in allowed_statuses:
                placeholders = ",".join("?" for _ in allowed_statuses)
                cur = c.execute(f"UPDATE {table} SET status=? WHERE id=? AND status IN ({placeholders})", (status, int(rawid), *allowed_statuses))
                changed = cur.rowcount == 1
                # Return reserved points only once if an admin rejects a withdrawal.
                if changed and typ == "wd" and verb == "reject":
                    c.execute("UPDATE users SET points=points+? WHERE user_id=?", (row["points"], row["user_id"]))
        if row and changed:
            notice = f"Your {typ} request #{rawid} was {status} by an admin."
            if typ == "wd" and verb == "reject":
                notice += f" Your {row['points']} reserved points have been returned to your wallet."
            try: await context.bot.send_message(row["user_id"], notice)
            except Exception: log.warning("Could not notify user %s about request %s", row["user_id"], rawid)
        elif row:
            status = row["status"]
        await q.edit_message_text(f"Request #{rawid}: {status}.", reply_markup=kb([[("⬅️ Review queue","admin_queue")],[("⬅️ Admin Dashboard","admin")]]))
    else:
        await q.edit_message_text("This option is not available yet. Please try again later.", reply_markup=kb([[("⬅️ Dashboard","home")]]))


async def quote_ad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Admin only."); return
    if len(context.args) < 3:
        await update.effective_message.reply_text("Usage: /quote_ad REQUEST_ID PRICE_ETB PAYMENT_INSTRUCTIONS"); return
    try:
        request_id = int(context.args[0]); price = float(context.args[1])
        if request_id < 1 or price < 0: raise ValueError()
    except ValueError:
        await update.effective_message.reply_text("Request ID must be a positive whole number and price must be zero or more."); return
    instructions = " ".join(context.args[2:])
    with db() as c:
        row = c.execute("SELECT user_id,status,kind FROM ad_requests WHERE id=?", (request_id,)).fetchone()
        if not row or row["status"] not in ("pending", "quoted"):
            await update.effective_message.reply_text("Ad request not found or it is no longer awaiting a quote."); return
        c.execute("UPDATE ad_requests SET quoted_price=?,status='quoted' WHERE id=?", (price, request_id))
    try:
        await context.bot.send_message(row["user_id"], f"📣 Quote for ad request #{request_id}\nService: {row['kind']}\nPrice: {price:g} ETB\nPayment instructions: {instructions}\n\nAfter paying, send /receipt {request_id} and upload a screenshot or send the transaction reference. Do not pay if anything looks suspicious; contact an admin first.")
    except Exception:
        log.warning("Could not send quote to user %s", row["user_id"])
    await update.effective_message.reply_text(f"Quote saved for ad request #{request_id}.")


async def receipt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /receipt AD_REQUEST_ID"); return
    request_id = int(context.args[0])
    with db() as c:
        row = c.execute("SELECT status FROM ad_requests WHERE id=? AND user_id=?", (request_id, user.id)).fetchone()
        if not row or row["status"] != "quoted":
            await update.effective_message.reply_text("I couldn't find a quoted ad request for your account. Check /myads or contact an admin."); return
        c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,'ad_receipt',?) ON CONFLICT(user_id) DO UPDATE SET action='ad_receipt',data=excluded.data", (user.id, str(request_id)))
    await update.effective_message.reply_text("Send your payment screenshot as a photo/document, or send the transaction reference as text. This only submits proof for admin review; it does not automatically confirm payment.")


async def my_ads_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db() as c:
        rows = c.execute("SELECT id,kind,status,quoted_price FROM ad_requests WHERE user_id=? ORDER BY id DESC LIMIT 10", (update.effective_user.id,)).fetchall()
    msg = "📋 Your ad requests\n" + ("\n".join(f"#{r['id']} · {r['kind']} · {r['status']} · {r['quoted_price'] if r['quoted_price'] is not None else 'quote pending'} ETB" for r in rows) if rows else "No ad requests yet.")
    await update.effective_message.reply_text(msg)


async def handle_receipt_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return
    # Receipt screenshot for a digital product. Only photo/document proofs are accepted.
    with db() as c:
        digital_pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if digital_pending and digital_pending["action"] == "digital_receipt":
        state = decode_pending(digital_pending["data"])
        product = digital_product(state.get("product_id", ""))
        with db() as c:
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=? AND enabled=1", (state.get("gateway_id", ""),)).fetchone()
        order_product_name = str(state.get("product_name") or (product["name"] if product else ""))
        order_duration = int(state.get("duration_months") or (product["duration_months"] if product else 1))
        order_price = float(state.get("price", product["price"] if product else 0))
        order_gateway_name = str(state.get("gateway_name") or (gateway["name"] if gateway else ""))
        if not product or not gateway or int(product["stock"]) <= 0:
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(digital_template("digital_no_stock_message", "Product or payment option unavailable."))
            return
        if message.photo:
            receipt_file_id, receipt_type = message.photo[-1].file_id, "photo"
        elif message.document:
            receipt_file_id, receipt_type = message.document.file_id, "document"
        else:
            await message.reply_text("Please upload a receipt screenshot as a photo or document.")
            return
        with db() as c:
            reserved = c.execute(
                "UPDATE digital_products SET stock=stock-1,updated_at=? WHERE id=? AND active=1 AND stock>0",
                (now(), product["id"])
            )
            if reserved.rowcount != 1:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text(digital_template("digital_no_stock_message", "This product is out of stock."))
                return
            cur = c.execute(
                "INSERT INTO digital_orders(user_id,product_id,product_name,duration_months,price,gateway_id,gateway_name,receipt_file_id,receipt_type,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,'pending_approval',?,?)",
                (user.id, product["id"], order_product_name, order_duration, order_price,
                 gateway["id"], order_gateway_name, receipt_file_id, receipt_type, now(), now())
            )
            order_id = cur.lastrowid
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(digital_template("digital_waiting_message", "Receipt received; waiting for admin review."))
        caption = (
            f"🌟 DIGITAL ORDER #{order_id}\nUser ID: {user.id}\nProduct: {order_product_name} "
            f"{order_duration}m\nPrice: {order_price:g} ETB\nGateway: {order_gateway_name}\n"
            "Status: Pending Approval"
        )
        configured_review_chat = digital_template("digital_admin_chat_id").strip()
        try:
            review_targets = [int(configured_review_chat)] if configured_review_chat else sorted(ADMIN_IDS)
        except ValueError:
            review_targets = sorted(ADMIN_IDS)
            log.error("Invalid digital_admin_chat_id; falling back to private admin chats")
        for target_chat in review_targets:
            try:
                markup = kb([[(f"Review order #{order_id}", f"digital_order_view_{order_id}")]])
                if receipt_type == "photo":
                    await context.bot.send_photo(target_chat, receipt_file_id, caption=caption, reply_markup=markup)
                else:
                    await context.bot.send_document(target_chat, receipt_file_id, caption=caption, reply_markup=markup)
            except Exception:
                log.exception("Could not forward digital order %s receipt to review chat %s", order_id, target_chat)
        return
    with db() as c:
        pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()

    if pending and pending["action"] == "crypto_delivery":
        await deliver_crypto_media(update, context, pending)
        return

    if pending and pending["action"] in ("buy_receipt", "sell_receipt"):
        state = decode_pending(pending["data"])
        order_id = int(state.get("order_id", 0))
        if message.photo:
            receipt = "photo:" + message.photo[-1].file_id
        elif message.document:
            receipt = "document:" + message.document.file_id
        else:
            await message.reply_text("Please upload a screenshot as a photo or document.")
            return
        with db() as c:
            order = c.execute(
                "SELECT * FROM crypto_orders WHERE id=? AND user_id=? AND status='awaiting_payment_proof'",
                (order_id, user.id),
            ).fetchone()
            if not order:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("This order is no longer waiting for payment proof.")
                return
            c.execute(
                "UPDATE crypto_orders SET receipt=?,status='pending_admin_approval',updated_at=? "
                "WHERE id=? AND user_id=?",
                (receipt, now(), order_id, user.id),
            )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        if order["side"] == "buy":
            vals = buy_usdt_values(amount=Decimal(str(order["amount_usdt"])), total=Decimal(str(order["total_etb"])),
                                   order_id=order_id, user_id=user.id, destination=order["transfer_destination"] or "",
                                   gateway={"name": order["payment_method"] or ""})
            await message.reply_text(render_digital_template(setting_value("buy_usdt_confirmation_template", ""), vals))
            caption = render_digital_template(setting_value("buy_usdt_admin_order_template", ""), vals)
        else:
            await message.reply_text(f"✅ Screenshot received for order #{order_id}. Admin will verify the actual transfer before completing your order.")
            caption = (f"🪙 {order['side'].upper()} USDT order #{order_id}\nUser: {user.id}\n"
                       f"Amount: {order['amount_usdt']:g} USDT\nETB total: {order['total_etb']:g}\n"
                       "Status: pending admin approval. Verify the real transaction independently.")
        configured_review_chat = setting_value("buy_usdt_admin_chat_id", "").strip() if order["side"] == "buy" else ""
        try:
            review_targets = [int(configured_review_chat)] if configured_review_chat else sorted(ADMIN_IDS)
        except ValueError:
            review_targets = sorted(ADMIN_IDS)
            log.error("Invalid Buy USDT review chat ID; using configured admin accounts")
        for aid in review_targets:
            try:
                markup = kb([[("🔎 Review order", f"crypto_order_view_{order_id}")]])
                if message.photo:
                    await context.bot.send_photo(aid, message.photo[-1].file_id, caption=caption, reply_markup=markup)
                else:
                    await context.bot.send_document(aid, message.document.file_id, caption=caption, reply_markup=markup)
            except Exception:
                log.warning("Could not forward crypto order %s proof to admin %s", order_id, aid)
        return

    if not pending or pending["action"] != "ad_receipt":
        return
    try:
        request_id = int(pending["data"])
    except (TypeError, ValueError):
        await message.reply_text("That ad receipt request is invalid. Please start again.")
        return
    with db() as c:
        owned = c.execute(
            "SELECT id FROM ad_requests WHERE id=? AND user_id=? AND status='quoted'",
            (request_id, user.id),
        ).fetchone()
        if not owned:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That ad request is no longer waiting for payment proof.")
            return
        if message.photo:
            receipt = "photo:" + message.photo[-1].file_id
        elif message.document:
            receipt = "document:" + message.document.file_id
        else:
            await message.reply_text("Please send a photo or document as payment proof.")
            return
        c.execute(
            "UPDATE ad_requests SET receipt=?,status='receipt_submitted' WHERE id=? AND user_id=?",
            (receipt, request_id, user.id),
        )
        c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
    await message.reply_text("✅ Payment proof submitted. An admin will verify it manually before confirming the ad.")
    caption = f"🧾 Payment proof for ad request #{request_id} from user {user.id}. Verify payment independently before approval."
    for aid in ADMIN_IDS:
        try:
            if message.photo:
                await context.bot.send_photo(aid, photo=message.photo[-1].file_id, caption=caption)
            elif message.document:
                await context.bot.send_document(aid, document=message.document.file_id, caption=caption)
        except Exception:
            log.warning("Could not forward receipt for ad request %s to admin %s", request_id, aid)
    await notify_admins(
        context,
        f"🧾 Payment proof submitted for ad request #{request_id} by user {user.id}. "
        "Review it in the Admin Dashboard; verify payment independently.",
    )


async def set_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Admin only."); return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /set key value"); return
    key, value = context.args[0], " ".join(context.args[1:])
    allowed = {"min_withdraw_points","usdt_etb_rate","price_ad_product","price_ad_members","price_ad_views","referral_points"}
    dynamic = (
        key in {"buy_enabled", "sell_enabled", "buy_unavailable_message", "sell_unavailable_message"}
        or key.startswith(("buy_payment_", "sell_payout_", "sell_network_", "buy_usdt_"))
        or key in {"buy_usdt_rate_1_2", "buy_usdt_rate_2_5", "buy_usdt_rate_5_plus",
                   "sell_usdt_rate_1_2", "sell_usdt_rate_2_5", "sell_usdt_rate_5_plus"}
    )
    if key not in allowed and not dynamic:
        await update.effective_message.reply_text(
            "Unsupported setting. Use rate keys buy_usdt_rate_1_2 / buy_usdt_rate_2_5 / buy_usdt_rate_5_plus "
            "and the corresponding sell_usdt_rate_* keys, plus buy_payment_*, sell_payout_*, sell_network_*, "
            "buy_enabled, sell_enabled, buy_unavailable_message, or sell_unavailable_message."
        ); return
    clearable_rate = key in {"buy_usdt_rate_1_2", "buy_usdt_rate_2_5", "buy_usdt_rate_5_plus",
                            "sell_usdt_rate_1_2", "sell_usdt_rate_2_5", "sell_usdt_rate_5_plus"}
    clearable_detail = key.startswith(("buy_payment_", "sell_network_")) and key.endswith(("_details", "_destination"))
    if value.strip().lower() == "none" and (clearable_rate or clearable_detail):
        with db() as c:
            c.execute("DELETE FROM settings WHERE key=?", (key,))
        await update.effective_message.reply_text(f"Deleted setting {key}.")
        return
    if key.endswith("_enabled") or key in ("buy_enabled", "sell_enabled"):
        normalized = value.strip().lower()
        if normalized not in ("true", "false", "1", "0", "yes", "no", "on", "off", "enabled", "disabled"):
            await update.effective_message.reply_text("For enable/disable settings use true or false."); return
        value = "true" if normalized in ("true", "1", "yes", "on", "enabled") else "false"
    elif key.endswith(("_rate_1_2", "_rate_2_5", "_rate_5_plus")) or key in {"buy_usdt_rate", "buy_usdt_min", "buy_usdt_max", "buy_usdt_stock", "buy_usdt_bep20_min", "buy_usdt_bybit_min"} or key.startswith("price_") or key == "usdt_etb_rate":
        try:
            number = Decimal(value)
            if not number.is_finite() or number < 0 or (key in {"buy_usdt_rate", "buy_usdt_min"} and number == 0): raise ValueError()
            if key == "buy_usdt_max" and buy_usdt_decimal("buy_usdt_min") is not None and number < buy_usdt_decimal("buy_usdt_min"): raise ValueError()
        except (ValueError, InvalidOperation):
            await update.effective_message.reply_text("Rates/minimum must be positive; stock, maximum and thresholds must be valid non-negative numbers."); return
    elif key == "min_withdraw_points":
        try:
            if int(value) < 1: raise ValueError()
        except ValueError:
            await update.effective_message.reply_text("Minimum withdrawal points must be a positive whole number."); return
    elif key in ("buy_unavailable_message", "sell_unavailable_message") and value.strip().lower() == "none":
        value = ""
    elif not value.strip():
        await update.effective_message.reply_text("Setting value cannot be empty."); return
    with db() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,value))
    await update.effective_message.reply_text(f"Updated {key} = {value if value else '(cleared)'}")


async def send_digital_admin_reply(update, context, order_id, reply_text, reject=False):
    admin_id = update.effective_user.id
    if not is_admin(admin_id):
        await update.effective_message.reply_text("Admin access only.")
        return
    with db() as c:
        order = c.execute("SELECT * FROM digital_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["status"] != "pending_approval":
            await update.effective_message.reply_text("Order not found or already processed.")
            return
    status = "rejected" if reject else "completed"
    try:
        await context.bot.send_message(order["user_id"], reply_text)
    except Exception:
        log.exception("Could not deliver admin reply for digital order %s", order_id)
        await update.effective_message.reply_text("The message could not be delivered. The order remains pending; correct the user's chat issue and try again.")
        return
    with db() as c:
        cur = c.execute("UPDATE digital_orders SET status=?,admin_id=?,admin_reply=?,updated_at=? WHERE id=? AND status='pending_approval'",
                        (status, admin_id, reply_text, now(), order_id))
        if cur.rowcount != 1:
            await update.effective_message.reply_text("The message was sent, but this order was processed concurrently. Please review its status.")
            return
        if reject:
            c.execute("UPDATE digital_products SET stock=stock+1,updated_at=? WHERE id=?", (now(), order["product_id"]))
    await update.effective_message.reply_text(f"Message sent to user {order['user_id']}; order #{order_id} marked {status}.")


async def handle_digital_admin_command(update, context, value):
    msg = update.effective_message
    if value.startswith("/product_add "):
        raw = value[len("/product_add "):]
        parts = [p.strip() for p in raw.split("|", 4)]
        if len(parts) != 5 or not parts[0] or not parts[1]:
            await msg.reply_text("Format: /product_add ID | NAME | MONTHS | PRICE | STOCK")
            return True
        try:
            months, price, stock = int(parts[2]), float(parts[3]), int(parts[4])
            if months < 1 or price < 0 or stock < 0 or not math.isfinite(price): raise ValueError()
        except ValueError:
            await msg.reply_text("Months must be positive; price and stock must be zero or greater.")
            return True
        with db() as c:
            c.execute(
                "INSERT INTO digital_products(id,name,duration_months,price,stock,updated_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET name=excluded.name,duration_months=excluded.duration_months,"
                "price=excluded.price,stock=excluded.stock,updated_at=excluded.updated_at",
                (parts[0],parts[1],months,price,stock,now())
            )
        await msg.reply_text("Digital product added/updated. Set its description/features with /product_set.")
        return True
    if value.startswith("/product_set "):
        parts = value[len("/product_set "):].split(" ", 2)
        if len(parts) != 3:
            await msg.reply_text("Format: /product_set ID FIELD VALUE")
            return True
        product_id, field, raw_value = parts
        allowed = {"name","duration_months","price","stock","description","features","important_note","notice","warranty","active"}
        if field not in allowed:
            await msg.reply_text("Field must be one of: " + ", ".join(sorted(allowed)))
            return True
        if field in {"duration_months","stock"}:
            try:
                parsed = int(raw_value)
                if parsed < (1 if field == "duration_months" else 0): raise ValueError()
            except ValueError:
                await msg.reply_text("Duration must be at least 1 month; stock must be zero or greater.")
                return True
            raw_value = parsed
        elif field == "price":
            try:
                parsed = float(raw_value)
                if parsed < 0 or not math.isfinite(parsed): raise ValueError()
            except ValueError:
                await msg.reply_text("Price must be zero or greater.")
                return True
            raw_value = parsed
        elif field == "active":
            flag = raw_value.lower()
            if flag not in {"true","false","1","0","yes","no","on","off"}:
                await msg.reply_text("Active must be true or false.")
                return True
            raw_value = 1 if flag in {"true","1","yes","on"} else 0
        with db() as c:
            cur = c.execute(f"UPDATE digital_products SET {field}=?,updated_at=? WHERE id=?", (raw_value,now(),product_id))
        await msg.reply_text("Product updated." if cur.rowcount else "Product ID not found.")
        return True
    if value.startswith("/gateway_set "):
        parts = [p.strip() for p in value[len("/gateway_set "):].split("|", 5)]
        if len(parts) != 6 or not parts[0] or not parts[1]:
            await msg.reply_text("Format: /gateway_set ID | NAME | ACCOUNT_NUMBER | ACCOUNT_NAME | INSTRUCTIONS | WARNING")
            return True
        with db() as c:
            c.execute("INSERT INTO digital_payment_gateways(id,name,account_number,account_name,instructions,warning,enabled,updated_at) "
                      "VALUES(?,?,?,?,?,?,1,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,account_number=excluded.account_number,"
                      "account_name=excluded.account_name,instructions=excluded.instructions,warning=excluded.warning,updated_at=excluded.updated_at",
                      (*parts, now()))
        await msg.reply_text("Payment gateway saved and enabled.")
        return True
    if value.startswith("/gateway_toggle "):
        parts = value.split()
        if len(parts) != 3 or parts[2].lower() not in {"true","false","1","0","yes","no","on","off"}:
            await msg.reply_text("Format: /gateway_toggle ID true/false")
            return True
        enabled = 1 if parts[2].lower() in {"true","1","yes","on"} else 0
        with db() as c:
            cur = c.execute("UPDATE digital_payment_gateways SET enabled=?,updated_at=? WHERE id=?", (enabled,now(),parts[1]))
        await msg.reply_text("Gateway updated." if cur.rowcount else "Gateway ID not found.")
        return True
    if value.startswith("/digital_waiting "):
        new_text = value[len("/digital_waiting "):].strip()
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('digital_waiting_message',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (new_text,))
        await msg.reply_text("Digital-order waiting message updated.")
        return True
    if value.startswith("/digital_admin_chat ") and value.strip().lower() != "/digital_admin_chat none":
        chat_value = value[len("/digital_admin_chat "):].strip()
        try:
            int(chat_value)
        except ValueError:
            await msg.reply_text("Provide a numeric Telegram user, group, or channel chat ID.")
            return True
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('digital_admin_chat_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (chat_value,))
        await msg.reply_text("Digital order review destination saved. Use /digital_admin_chat none to clear it and send receipts to each configured admin.")
        return True
    if value == "/digital_admin_chat none":
        with db() as c:
            c.execute("DELETE FROM settings WHERE key='digital_admin_chat_id'")
        await msg.reply_text("Digital order review destination cleared.")
        return True
    if value.startswith("/digital_text "):
        parts = value[len("/digital_text "):].split(" ", 1)
        if len(parts) != 2 or not parts[1].strip():
            await msg.reply_text("Format: /digital_text KEY MESSAGE")
            return True
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (parts[0],parts[1]))
        await msg.reply_text("Database message/template updated.")
        return True
    return False


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text: return
    value = message.text.strip()

    # Admin can reply directly to the forwarded receipt in the private admin chat.
    if is_admin(user.id) and message.reply_to_message:
        caption = message.reply_to_message.caption or ""
        import re
        match = re.search(r"DIGITAL ORDER #([0-9]+)", caption)
        if match and value:
            await send_digital_admin_reply(update, context, int(match.group(1)), value, reject=False)
            return

    # Database-backed admin commands for products, gateways and editable user messages.
    if is_admin(user.id) and await handle_digital_admin_command(update, context, value):
        return

    with db() as c:
        p = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if not p:
        await message.reply_text("Use the dashboard buttons to get started.", reply_markup=home_keyboard(is_admin(user.id))); return
    action, data = p["action"], p["data"]
    if action == "digital_admin_reply":
        state = decode_pending(data)
        await send_digital_admin_reply(update, context, int(state.get("order_id", 0)), value,
                                       reject=bool(state.get("reject", False)))
        return
    if await handle_crypto_text(update, context, action, data, value):
        return
    if action == "admin_task_title":
        if not is_admin(user.id): return
        parts = [x.strip() for x in value.split("|")]
        if len(parts) != 4 or not parts[0] or not parts[1]:
            await update.effective_message.reply_text("Format: Task title | @channel | target joins | points"); return
        try:
            target, points = int(parts[2]), int(parts[3])
            if target < 1 or points < 1: raise ValueError()
        except ValueError:
            await update.effective_message.reply_text("Target and points must be positive whole numbers."); return
        try:
            chat = await context.bot.get_chat(parts[1])
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status not in ("administrator","creator"):
                await update.effective_message.reply_text("Please make the bot an administrator in that channel first."); return
            test = await context.bot.create_chat_invite_link(chat.id, member_limit=1, name="KefiaETBot-permission-check")
            await context.bot.revoke_chat_invite_link(chat.id, test.invite_link)
        except Exception:
            await update.effective_message.reply_text("Cannot access/create invite links for that channel. Check the channel ID and bot admin permissions."); return
        with db() as c:
            c.execute("INSERT INTO tasks(title,channel,target,points,created_by,created_at) VALUES(?,?,?,?,?,?)",
                      (parts[0],str(chat.id),target,points,user.id,now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text("✅ Task created. Users can now claim it from Daily Jobs.", reply_markup=home_keyboard(True))
    elif action == "withdraw":
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (user.id,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
            minp = int(minimum["value"]) if minimum else 1000
            if not u or u["points"] < minp:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await update.effective_message.reply_text("Your points are below the withdrawal minimum."); return
            c.execute("INSERT INTO withdrawals(user_id,points,payout_method,payout_details,created_at) VALUES(?,?,?,?,?)",
                      (user.id,u["points"],"user-provided",value,now()))
            c.execute("UPDATE users SET points=0 WHERE user_id=?", (user.id,))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text("✅ Withdrawal request submitted for admin review. Points are reserved until the request is approved or rejected.")
        await notify_admins(context, f"💸 New withdrawal request from {user.id}. Review in Admin Dashboard.")
    elif action == "ad_receipt":
        request_id = int(data)
        with db() as c:
            cur = c.execute("UPDATE ad_requests SET receipt=?,status='receipt_submitted' WHERE id=? AND user_id=? AND status='quoted'", (value, request_id, user.id))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        if cur.rowcount:
            await update.effective_message.reply_text("✅ Transaction reference submitted. An admin will verify it manually.")
            await notify_admins(context, f"🧾 Transaction reference submitted for ad request #{request_id} by user {user.id}. Reference: {value}")
        else:
            await update.effective_message.reply_text("That ad request is no longer waiting for payment proof.")
    elif action.startswith("ad_"):
        with db() as c:
            c.execute("INSERT INTO ad_requests(user_id,kind,details,duration,created_at) VALUES(?,?,?,?,?)",(user.id,data,value,"user specified",now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text("✅ Ad request submitted. An admin will review your details and send the final quote and payment instructions. Do not pay until the quote is confirmed.")
        await notify_admins(context, f"📣 New ad request from user {user.id}: {data}\nDetails: {value}")
    elif action in ("buy_asset","buy_social","sell_usdt","sell_social"):
        with db() as c:
            c.execute("INSERT INTO market_listings(user_id,action,asset_type,details,created_at) VALUES(?,?,?,?,?)",(user.id,action,data,value,now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text("✅ Request submitted. It is pending manual review. Never send account passwords, USDT seed phrases, or private keys.")
        await notify_admins(context, f"🛍 New marketplace request from user {user.id}: {data}\nDetails: {value}")
    else:
        await update.effective_message.reply_text("I couldn't identify that request. Please open the dashboard and try again.")


async def notify_admins(context, message):
    for aid in ADMIN_IDS:
        try: await context.bot.send_message(aid, message)
        except Exception: log.warning("Could not notify admin %s", aid)


async def track_channel_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu or not cmu.invite_link:
        return
    if cmu.new_chat_member.status not in ("member", "administrator", "creator"):
        return
    if cmu.old_chat_member.status in ("member", "administrator", "creator", "restricted"):
        return
    link = cmu.invite_link.invite_link
    with db() as c:
        mapping = c.execute(
            "SELECT task_id,owner_user_id FROM invite_links WHERE invite_link=?", (link,)
        ).fetchone()
        if not mapping:
            return
        joined_id = cmu.new_chat_member.user.id
        if joined_id == mapping["owner_user_id"]:
            return
        try:
            c.execute(
                "INSERT INTO invite_events(invite_link,joined_user_id,joined_at) VALUES(?,?,?)",
                (link, joined_id, now())
            )
        except sqlite3.IntegrityError:
            return
        task = c.execute(
            "SELECT * FROM tasks WHERE id=? AND active=1", (mapping["task_id"],)
        ).fetchone()
        if not task or task["completed_count"] >= task["target"]:
            return
        # The admin-configured points value is the reward for each unique verified join.
        c.execute("UPDATE tasks SET completed_count=completed_count+1 WHERE id=?", (task["id"],))
        c.execute("UPDATE users SET points=points+? WHERE user_id=?",
                  (task["points"], mapping["owner_user_id"]))
        new_count = task["completed_count"] + 1
        c.execute("UPDATE task_claims SET status='in progress' WHERE task_id=? AND user_id=?",
                  (task["id"], mapping["owner_user_id"]))
        completed = new_count >= task["target"]
        if completed:
            c.execute("UPDATE tasks SET active=0 WHERE id=?", (task["id"],))
            c.execute("UPDATE task_claims SET status='completed' WHERE task_id=?", (task["id"],))
    message = "🎉 Verified join recorded! You earned " + str(task["points"]) + " points.\n"
    message += "Campaign progress: " + str(new_count) + "/" + str(task["target"]) + "."
    if completed:
        message += "\nThe campaign target has been reached and the task is now closed."
    try:
        await context.bot.send_message(mapping["owner_user_id"], message)
    except Exception:
        log.warning("Could not notify task owner %s", mapping["owner_user_id"])


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled update error", exc_info=context.error)


class HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP endpoint so Render Web Service can detect an open port."""

    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"KefiaETBot is running")

    def log_message(self, format, *args):
        # Avoid noisy per-request logs.
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, name="render-health-server", daemon=True)
    thread.start()
    log.info("Health endpoint listening on port %s", port)
    return server


def main():
    if not TOKEN:
        raise RuntimeError("Set BOT_TOKEN environment variable.")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS is empty. Admin dashboard and approvals will be unavailable.")
    init_db()
    health_server = start_health_server()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_setting))
    app.add_handler(CommandHandler("quote_ad", quote_ad))
    app.add_handler(CommandHandler("receipt", receipt_command))
    app.add_handler(CommandHandler("myads", my_ads_command))
    app.add_handler(CallbackQueryHandler(menu))
    app.add_handler(ChatMemberHandler(track_channel_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_receipt_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)
    log.info("KefiaETBot starting")
    # Python 3.14 no longer implicitly creates a current event loop here.
    # Set one explicitly for python-telegram-bot's run_polling lifecycle.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        app.run_polling(
            allowed_updates=["message", "callback_query", "chat_member"],
            close_loop=False,
        )
    finally:
        health_server.shutdown()
        health_server.server_close()
        if not loop.is_closed():
            loop.close()
        asyncio.set_event_loop(None)


if __name__ == "__main__":
    main()
