import os
import asyncio
import sqlite3
import re
import json
import math
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from functools import wraps

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg import IntegrityError as PostgreSQLIntegrityError
except ImportError:  # SQLite remains available when DATABASE_URL is not configured.
    psycopg = None
    dict_row = None
    PostgreSQLIntegrityError = None

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMemberUpdated
)
from telegram.ext import (
    Application, ApplicationHandlerStop, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, TypeHandler, ContextTypes, filters
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
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_INTEGRITY_ERROR = (sqlite3.IntegrityError, PostgreSQLIntegrityError) if PostgreSQLIntegrityError else (sqlite3.IntegrityError,)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("KefiaETBot")


class PostgreSQLCursor:
    """Small SQLite-style cursor adapter for the bot's existing query code."""
    def __init__(self, cursor):
        self._cursor = cursor
        self._inserted_id = None

    def execute(self, sql, params=None):
        self._inserted_id = None
        sql = _translate_sql(sql)
        match = re.match(r"^\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.I)
        if match and match.group(1).lower() in {
            "tasks", "task_claims", "invite_events", "ad_requests",
            "promoter_join_events", "promoter_ad_submissions", "market_listings", "withdrawals",
            "crypto_orders", "digital_orders",
        } and not re.search(r"\bRETURNING\b", sql, re.I):
            sql = sql.rstrip().rstrip(";") + " RETURNING id"
        self._cursor.execute(sql, params or ())
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        if self._inserted_id is None:
            row = self._cursor.fetchone()
            if row:
                self._inserted_id = row.get("id") if hasattr(row, "get") else row[0]
        return self._inserted_id


class PostgreSQLConnection:
    """Expose the subset of sqlite3.Connection used by this bot."""
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._connection.close()
        return False

    def execute(self, sql, params=None):
        cursor = PostgreSQLCursor(self._connection.cursor())
        return cursor.execute(sql, params)

    def executescript(self, script):
        # The initialization schema contains independent CREATE TABLE statements.
        for statement in script.split(";"):
            if statement.strip():
                statement = re.sub(
                    r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
                    "BIGSERIAL PRIMARY KEY", statement, flags=re.I
                )
                statement = re.sub(r"\bINTEGER\b", "BIGINT", statement, flags=re.I)
                self.execute(statement)
        return self

    def close(self):
        self._connection.close()


def _translate_sql(sql):
    """Translate the small set of SQLite SQL dialect features used by this bot."""
    statement = sql.strip()
    if re.match(r"^PRAGMA\s+foreign_keys\s*=", statement, re.I):
        return "SELECT 1"
    info = re.match(r"^PRAGMA\s+table_info\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*;?$", statement, re.I)
    if info:
        table_name = info.group(1).lower()
        return (
            "SELECT column_name AS name FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table_name}' ORDER BY ordinal_position"
        )
    statement = re.sub(r"\bCOLLATE\s+NOCASE\b", "", statement, flags=re.I)
    ignored_insert = re.match(r"^INSERT\s+OR\s+IGNORE\s+INTO\b", statement, re.I)
    if ignored_insert:
        statement = re.sub(r"^INSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", statement, count=1, flags=re.I)
        statement = statement.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    # Convert positional SQLite placeholders without touching quoted text.
    result = []
    quote = None
    i = 0
    while i < len(statement):
        ch = statement[i]
        if quote:
            result.append(ch)
            if ch == quote:
                if i + 1 < len(statement) and statement[i + 1] == quote:
                    result.append(statement[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"'):
            quote = ch
            result.append(ch)
        elif ch == "?":
            result.append("%s")
        else:
            result.append(ch)
        i += 1
    return "".join(result)


def migrate_legacy_sqlite(c):
    """Copy an existing SQLite database into PostgreSQL once, before default seeding."""
    if not DATABASE_URL:
        return
    c.execute(
        "CREATE TABLE IF NOT EXISTS _sqlite_migration_state "
        "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    state = c.execute(
        "SELECT value FROM _sqlite_migration_state WHERE key='legacy_sqlite_v1'"
    ).fetchone()
    if state:
        log.info("Legacy SQLite migration already marked complete.")
        return
    if not os.path.isfile(DB_PATH) or os.path.getsize(DB_PATH) == 0:
        log.warning(
            "No legacy SQLite database found at %s. Starting with a fresh PostgreSQL database as configured.",
            DB_PATH,
        )
        return
    source = None
    try:
        source = sqlite3.connect(DB_PATH)
        source.row_factory = sqlite3.Row
        source_tables = [
            row["name"] for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
            ).fetchall()
        ]
        if not source_tables:
            log.warning(
                "Legacy SQLite file at %s has no application tables. Starting with a fresh PostgreSQL database.",
                DB_PATH,
            )
            return
        auto_id_tables = (
            "tasks", "task_claims", "invite_events", "ad_requests",
            "promoter_join_events", "market_listings", "withdrawals",
            "crypto_orders", "digital_orders",
        )
        copied_rows = 0
        for table in source_tables:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
                continue
            target_columns = [
                row["column_name"] for row in c.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
                    (table.lower(),),
                ).fetchall()
            ]
            if not target_columns:
                continue
            source_columns = [row["name"] for row in source.execute(f'PRAGMA table_info("{table}")').fetchall()]
            columns = [name for name in source_columns if name in target_columns]
            if not columns:
                continue
            quoted_columns = ", ".join('"' + name + '"' for name in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = (
                f'INSERT INTO "{table}" ({quoted_columns}) VALUES ({placeholders}) '
                "ON CONFLICT DO NOTHING"
            )
            rows = source.execute(f'SELECT {quoted_columns} FROM "{table}"').fetchall()
            for row in rows:
                c.execute(insert_sql, tuple(row[name] for name in columns))
                copied_rows += 1
        # Align generated IDs with imported rows to prevent duplicate-ID failures.
        for table in auto_id_tables:
            c.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"GREATEST(COALESCE(MAX(id), 1), 1), COUNT(*) > 0) FROM {table}"
            )
        c.execute(
            "INSERT INTO _sqlite_migration_state(key,value) "
            "VALUES('legacy_sqlite_v1', %s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (f"imported_rows={copied_rows}",),
        )
        log.info("Imported %s legacy SQLite rows into PostgreSQL.", copied_rows)
    except Exception:
        log.exception("Legacy SQLite migration failed; startup will stop to avoid silently losing data.")
        raise
    finally:
        if source is not None:
            source.close()


def db():
    if DATABASE_URL:
        if psycopg is None:
            raise RuntimeError(
                "DATABASE_URL is set but psycopg is missing. Add psycopg[binary] to requirements.txt."
            )
        connection = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15)
        return PostgreSQLConnection(connection)
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
        CREATE TABLE IF NOT EXISTS referral_rewards(
          id INTEGER PRIMARY KEY AUTOINCREMENT, inviter_user_id INTEGER NOT NULL,
          invited_user_id INTEGER NOT NULL UNIQUE, points_awarded INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ad_requests(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, kind TEXT NOT NULL,
          details TEXT NOT NULL, duration TEXT, quoted_price REAL, receipt TEXT,
          status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS promoter_ad_submissions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
          platforms TEXT NOT NULL, content_type TEXT NOT NULL, followers INTEGER NOT NULL,
          channel_link TEXT NOT NULL, price_day REAL, price_week REAL, price_month REAL,
          status TEXT NOT NULL DEFAULT 'pending', admin_note TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS promoter_profiles(
          user_id INTEGER PRIMARY KEY, method TEXT NOT NULL, account_number TEXT NOT NULL,
          account_name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
          target_count INTEGER NOT NULL DEFAULT 100, points_per_join INTEGER NOT NULL DEFAULT 1,
          completed_count INTEGER NOT NULL DEFAULT 0, invite_link TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS promoter_join_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, promoter_user_id INTEGER NOT NULL,
          joined_user_id INTEGER NOT NULL UNIQUE, invite_link TEXT NOT NULL, joined_at TEXT NOT NULL
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
        CREATE TABLE IF NOT EXISTS marketplace_messages(
          id INTEGER PRIMARY KEY AUTOINCREMENT, listing_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL, sender_role TEXT NOT NULL,
          message_type TEXT NOT NULL DEFAULT 'text', message_text TEXT NOT NULL DEFAULT '',
          file_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
        );
        """)
        # Marketplace account listings: add columns safely for existing SQLite databases.
        market_columns = {row["name"] for row in c.execute("PRAGMA table_info(market_listings)").fetchall()}
        for column, declaration in (
            ("short_description", "TEXT NOT NULL DEFAULT ''"),
            ("listing_active", "INTEGER NOT NULL DEFAULT 0"),
            ("purchase_status", "TEXT NOT NULL DEFAULT 'available'"),
            ("buyer_user_id", "INTEGER"),
            ("payment_method", "TEXT NOT NULL DEFAULT ''"),
            ("receipt_file_id", "TEXT NOT NULL DEFAULT ''"),
            ("receipt_type", "TEXT NOT NULL DEFAULT ''"),
            ("admin_note", "TEXT NOT NULL DEFAULT ''"),
        ):
            if column not in market_columns:
                c.execute(f"ALTER TABLE market_listings ADD COLUMN {column} {declaration}")
        # Existing task databases gain an optional cap on how many users may claim each task.
        task_columns = {row["name"] for row in c.execute("PRAGMA table_info(tasks)").fetchall()}
        if "participant_limit" not in task_columns:
            c.execute("ALTER TABLE tasks ADD COLUMN participant_limit INTEGER NOT NULL DEFAULT 0")

        # Import the old database before seeding defaults, so saved admin settings win.
        if DATABASE_URL:
            migrate_legacy_sqlite(c)

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
        # Keep the seeded Gemini Pro placeholder out of the customer catalogue until
        # an admin configures both a real price and available stock.
        c.execute(
            "UPDATE digital_products SET active=0,updated_at=? WHERE id='gemini_pro_18m' AND price=0 AND stock=0",
            (now(),)
        )
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
            ("digital_invalid_payment_message", "That payment option is no longer available. Please return to the marketplace and choose again."),
            ("digital_cancel_message", "Your purchase was cancelled."),
            ("digital_order_submitted_message", "✅ Your receipt has been submitted! Your order is now under review. We’ll message you after the admin checks it."),
            ("digital_buy_button", "🌟 Buy now"),
            ("digital_cancel_button", "❌ Cancel | አቋርጥ"),
            ("digital_market_button", "⬅️ Marketplace"),
            ("digital_choose_gateway_prompt", "💳 Choose your payment method:"),
            ("digital_market_title", "🛍 Marketplace — choose what you want to do:"),
            ("digital_market_product_button_template", "🌟 {name} · {duration} months · {price} ETB · stock {stock}"),
            ("digital_receipt_upload_prompt", "📸 After paying, upload a clear payment receipt screenshot as a photo or document."),
            ("digital_product_details_template", "🌟 {name} {duration}m\n\n💰 Price: {price} ETB each\n📦 In stock: {stock}\n\n📝 DESCRIPTION\n{description}\n\n✨ FEATURES\n{features}\n\n📌 Important Note:\n{note}\n\n🚨 NOTICE\n{notice}\n\n🎯 Price: {price} ETB / unit\n🛡️ Warranty: {warranty}\n\nTap Buy now when you are ready."),
            ("digital_payment_template", "🌟 Amount to pay: {price} ETB\n\n🏦 {gateway_name}\n\nNumber: {account_number}\nName: {account_name}\n\nSend the exact ETB amount, then upload a clear {gateway_name} receipt screenshot.\n{instructions}\n\n📞 Payment instructions\nAfter payment, upload a clear {gateway_name} receipt screenshot. Once your payment is verified, we will send your private redeem link.\n\n🔍 Required: upload a clear screenshot of the receipt/transaction.\nText-only references are not accepted.\n{warning}"),
            ("invite_earn_active", "1"),
            ("invite_earn_message", "🔗 INVITE & EARN\n\nInvite friends to KefiaETBot using your personal link. When a new user starts the bot through your link, you earn {reward_points} points.\n\n📌 Referral count: {referrals}\n⭐ Referral points earned: {earned_points}\n👛 Current wallet: {wallet_points} points\n💱 Conversion: {points_per_birr} points = 1 ETB\n💵 Estimated wallet value: {wallet_birr} ETB\n\nMinimum withdrawal: {minimum_points} points. Withdrawals are reviewed by admins."),
            ("invite_earn_unavailable_message", "🔗 Invite & Earn is temporarily unavailable. Please check back later."),
            ("invite_earn_unavailable_photo", ""),
            ("points_per_birr", "100"),
            ("promoter_ads_active", "1"),
            ("promoter_withdraw_min_points", "1000"),
            ("promoter_withdraw_require_target", "1"),
            ("promoter_chat_enabled", "1"),
            ("promoter_ads_submission_limit", "0"),
            ("promoter_question_platforms", "Which social media platforms do you have? Select at least one."),
            ("promoter_question_content", "What type of content do you publish on your channel?"),
            ("promoter_question_followers", "How many followers or subscribers do you have? Enter a whole number."),
            ("promoter_question_link", "Send your public channel or profile link."),
            ("promoter_question_prices", "Set your advertising price in ETB for at least one period: per day, per week, or per month."),
            ("promoter_rules", "📣 PROMOTER PROGRAM RULES\n\n1. Share only your unique invite link provided by KefiaETBot.\n2. Only real, unique people who join the configured channel through your link count.\n3. Self-joins, duplicate accounts, fake members, and paid/fraudulent joins do not count.\n4. Your progress and points are tracked by the bot.\n5. Once you reach the campaign target, you may request a withdrawal of your available points.\n6. Provide accurate Telebirr or CBE account details. Admins verify activity and payments.\n7. Do not spam or mislead people. Violations may result in disqualification.\n\nTap Agree & Confirm only if you accept these rules."),
            ("promoter_channel", ""),
            ("promoter_target", "100"),
            ("promoter_points_per_join", "1"),
            ("advertiser_active", "1"),
            ("advertiser_rules", "📣 ADVERTISING CONDITIONS\n\n1. Admins review every request before publication.\n2. Use only the payment details shown by the bot.\n3. Illegal, deceptive, or prohibited products are not accepted.\n4. Advertising starts only after payment and final approval are verified."),
            ("advertiser_channels", "Telegram, TikTok, YouTube, Instagram, Facebook"),
            ("advertiser_payment_info", "Payment information has not been configured yet. Please wait for an admin."),
            ("advertiser_unavailable_message", "Advertiser service is temporarily unavailable. Please check back later."),
            ("advertiser_price_day", ""),
            ("advertiser_price_week", ""),
            ("advertiser_price_month", ""),
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
        [("🧩 Daily Tasks", "jobs"), ("🔗 Invite & Earn", "invite")],
        [("📣 Promote / Ads", "ads"), ("🛍 Marketplace", "market")],
        [("👛 My Wallet", "wallet"), ("💸 Withdraw Points", "withdraw")],
        [("👤 My Account", "profile")]
    ]
    if admin:
        rows.append([("🛡 Admin Dashboard", "admin")])
    contact_username = str(setting_value("contact_admin_username", os.getenv("CONTACT_ADMIN_USERNAME", "")) or "").strip()
    contact_username = contact_username.removeprefix("@")
    if contact_username.lower().startswith("https://t.me/"):
        contact_username = contact_username.split("t.me/", 1)[1].split("/", 1)[0].split("?", 1)[0]
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", contact_username):
        rows.append([("📞 Contact Admin", "https://t.me/" + contact_username)])
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(text, url=data) if data.startswith("https://") else InlineKeyboardButton(text, callback_data=data)
             for text, data in row]
            for row in rows
        ])
    rows.append([("📞 Contact Admin", "contact_admin")])
    return kb(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    with db() as c:
        is_new_user = c.execute("SELECT 1 FROM users WHERE user_id=?", (user.id,)).fetchone() is None
    upsert_user(user)
    # A referral is counted once per Telegram account. Reward is optional and admin-configured.
    arg = context.args[0] if context.args else ""
    if (str(setting_value("invite_earn_active", "1")).strip().lower() in {"1", "true", "yes", "on"}
            and arg.startswith("ref_") and arg[4:].isdigit()):
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
                        c.execute(
                            "INSERT OR IGNORE INTO referral_rewards(inviter_user_id,invited_user_id,points_awarded,created_at) VALUES(?,?,?,?)",
                            (ref, user.id, rewarded_points, now())
                        )
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

def default_task_message_template():
    """Customer-facing task message. The personal invite link marker is mandatory and system-filled."""
    return (
        "🧩 DAILY TASK · {title}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🎁 Reward: {points} points per verified join\n"
        "📣 Channel: {channel}\n"
        "📈 Campaign progress: {completed_count}/{target} joins\n"
        "👥 Task participants: {participants}\n"
        "🧭 Your status: {status}\n\n"
        "🔗 YOUR PERSONAL INVITE LINK\n"
        "{{PERSONAL_INVITE_LINK}}\n\n"
        "Share this link with real people. Your link is created automatically for you. "
        "Only new, unique joins verified by Telegram count toward your reward. "
        "Self-joins, duplicate accounts and fake members do not count."
    )


def required_channel_config():
    """Database settings override Render environment defaults."""
    channel = setting_value("force_join_channel")
    if channel is None:
        channel = os.getenv("FORCE_JOIN_CHANNEL", "").strip()
    channel = str(channel or "").strip()
    if channel.lower() in {"", "off", "none", "disabled", "false"}:
        return "", ""
    join_url = setting_value("force_join_url")
    if join_url is None:
        join_url = os.getenv("FORCE_JOIN_URL", "").strip()
    join_url = str(join_url or "").strip()
    if not join_url and channel.startswith("@"):
        join_url = "https://t.me/" + channel[1:]
    elif not join_url and not channel.lstrip("-").isdigit() and "t.me/" in channel:
        join_url = channel if channel.startswith(("https://", "http://")) else "https://" + channel
    return channel, join_url


async def enforce_required_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Block non-admin users from using the bot until they join the configured channel."""
    # This gate is only for private user messages and callback buttons; leave
    # channel/group membership updates available to their dedicated handlers.
    if not update.effective_message and not update.callback_query:
        return
    user = update.effective_user
    if not user or is_admin(user.id):
        return

    channel, join_url = required_channel_config()
    if not channel:
        return

    query = update.callback_query
    if query and query.data != "force_join_check":
        # Don't let other callback actions run until membership is verified.
        pass

    try:
        member = await context.bot.get_chat_member(channel, user.id)
        joined = member.status in ("member", "administrator", "creator") or bool(
            getattr(member, "is_member", False)
        )
    except Exception:
        log.exception("Could not verify required-channel membership for user_id=%s channel=%r", user.id, channel)
        message_text = (
            "⚠️ I couldn't verify your channel membership. Please contact the bot administrator.\n"
            "The channel may be private, the channel ID/username may be incorrect, or the bot may need admin access."
        )
        if query:
            await query.answer()
            try:
                await query.edit_message_text(message_text)
            except Exception:
                pass
        elif update.effective_message:
            await update.effective_message.reply_text(message_text)
        raise ApplicationHandlerStop

    if joined:
        if query and query.data == "force_join_check":
            await query.answer("Membership verified! Welcome.")
            await query.edit_message_text(
                f"✅ You're a member of the required channel. Welcome, {user.first_name or 'there'}!",
                reply_markup=home_keyboard(is_admin(user.id)),
            )
            raise ApplicationHandlerStop
        return

    if query:
        await query.answer()
        text_body = "🔒 To use KefiaETBot, please join our required Telegram channel first, then tap “I've joined”."
        buttons = []
        if join_url:
            buttons.append([InlineKeyboardButton("📢 Join Required Channel", url=join_url)])
        buttons.append([InlineKeyboardButton("✅ I've joined — Check", callback_data="force_join_check")])
        try:
            await query.edit_message_text(text_body, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            await context.bot.send_message(
                chat_id=user.id, text=text_body, reply_markup=InlineKeyboardMarkup(buttons)
            )
    elif update.effective_message:
        text_body = "🔒 To use KefiaETBot, please join our required Telegram channel first, then tap “I've joined”."
        buttons = []
        if join_url:
            buttons.append([InlineKeyboardButton("📢 Join Required Channel", url=join_url)])
        buttons.append([InlineKeyboardButton("✅ I've joined — Check", callback_data="force_join_check")])
        await update.effective_message.reply_text(text_body, reply_markup=InlineKeyboardMarkup(buttons))
    raise ApplicationHandlerStop


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
    return [(slug, setting_value(f"sell_network_{slug}_label", label).strip() or label)
            for slug, label in SELL_NETWORKS.items()
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
        await q.edit_message_text(
            custom_message or ("💸 Sell USDT is temporarily unavailable. Please try again later." if side == "sell" else "Buy USDT is temporarily unavailable. Please try again later."),
            reply_markup=kb([[("⬅️ Dashboard", "home")]])
        )
        return
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
        setting_value("sell_payout_prompt", "💸 Sell USDT\n\nHow would you like to receive your ETB payout?\nብር ይቀበሉበት የሚፈልጉትን የክፍያ መንገድ (ባንክ ወይም Telebirr) ይምረጡ:\n\nChoose CBE, Telebirr, Bank of Abyssinia, or another Ethiopian bank."),
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
        reserved = c.execute(
            "UPDATE settings SET value=CAST(value AS REAL)-? WHERE key='buy_usdt_stock' AND CAST(value AS REAL)>=?",
            (str(amount), str(amount))
        )
        if reserved.rowcount != 1:
            await update.effective_message.reply_text(render_digital_template(
                setting_value("buy_usdt_stock_error", ""), buy_usdt_values(amount=amount)))
            return
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
        reserved = c.execute(
            "UPDATE settings SET value=CAST(value AS REAL)-? WHERE key='buy_usdt_stock' AND CAST(value AS REAL)>=?",
            (str(amount), str(amount))
        )
        if reserved.rowcount != 1:
            await q.edit_message_text(render_digital_template(setting_value("buy_usdt_stock_error", ""), buy_usdt_values(amount=amount)),
                                      reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
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
        payout_summary = f"• Method: {PAYMENT_METHODS.get(payout.get('method'), payout.get('method', '—'))}\n• Phone/account number: {payout.get('account_number', '—')}\n• Account name: {payout.get('account_name', '—')}"
        template = setting_value("sell_deposit_prompt", "💸 Step 3 of 4 — Choose USDT deposit method | ደረጃ 3 — የUSDT ማስገቢያ መንገድ ይምረጡ\n\nYour ETB payout destination | የብር መቀበያ መረጃዎ\n{payout_summary}\n\n📈 Sell USDT Rates\n{rates}\n\nHow will you send the USDT? USDT ለአድሚኑ በምን መንገድ ይልካሉ?")
        summary = render_digital_template(template, {"payout_summary":payout_summary,"method":PAYMENT_METHODS.get(payout.get('method'),payout.get('method','—')),"account_number":payout.get('account_number','—'),"account_name":payout.get('account_name','—'),"rates":rate_lines})
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

    if action == "admin_sell_usdt":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            total=c.execute("SELECT COUNT(*) n FROM crypto_orders WHERE side='sell'").fetchone()["n"]
            opened=c.execute("SELECT COUNT(*) n FROM crypto_orders WHERE side='sell' AND status IN ('awaiting_payment_proof','pending_admin_approval','payment_verified')").fetchone()["n"]
        await q.edit_message_text(f"💸 SELL USDT MANAGEMENT\n\nOrders: {total}\nOpen orders: {opened}\nStatus: {'ENABLED' if setting_enabled('sell_enabled',True) else 'DISABLED'}",reply_markup=kb([[("📋 All orders","sell_admin_orders_all"),("⏳ Open orders","sell_admin_orders_pending")],[("🟢 Enable / 🔴 Disable","sell_admin_toggle")],[("💱 Tier rates","sell_admin_rates"),("🏦 Payout methods","sell_admin_payouts")],[("📥 USDT deposit methods / IDs","sell_admin_networks")],[("✏️ Edit Sell Form","sell_admin_messages")],[("⬅️ Admin Dashboard","admin")]])); return
    if action == "sell_admin_toggle":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        val="false" if setting_enabled("sell_enabled",True) else "true"
        with db() as c: c.execute("INSERT INTO settings(key,value) VALUES('sell_enabled',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(val,))
        await q.edit_message_text("Sell USDT "+("enabled." if val=="true" else "disabled for new orders."),reply_markup=kb([[("⬅️ Sell USDT Management","admin_sell_usdt")]])); return
    if action == "sell_admin_rates":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        rows=[[(f"{label}: {setting_value('sell_usdt_rate_'+tier,'not set')} ETB/USDT",f"sell_admin_edit_sell_usdt_rate_{tier}")] for tier,label in (("1_2","1–2 USDT"),("2_5","Over 2–5 USDT"),("5_plus","Over 5 USDT"))]
        rows.append([("⬅️ Back","admin_sell_usdt")]); await q.edit_message_text("💱 Set the ETB-per-USDT rate for each tier. Rates must be positive.",reply_markup=kb(rows)); return
    if action == "sell_admin_payouts":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        rows=[[(f"{'🟢 ON' if setting_enabled('sell_payout_'+slug+'_enabled') else '⚪ OFF'} · {label}",f"sell_admin_toggle_sell_payout_{slug}_enabled")] for slug,label in PAYMENT_METHODS.items()]
        rows.append([("⬅️ Back","admin_sell_usdt")]); await q.edit_message_text("🏦 Enable or disable the ETB payout methods customers may choose.",reply_markup=kb(rows)); return
    if action == "sell_admin_networks":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        rows=[]
        for slug,label in SELL_NETWORKS.items():
            configured=bool(setting_value(f"sell_network_{slug}_destination","").strip())
            rows.append([(f"{'🟢' if setting_enabled(f'sell_network_{slug}_enabled') else '⚪'} {label} · {'ID set' if configured else 'ID missing'}",f"sell_admin_network_{slug}")])
        rows.append([("⬅️ Back","admin_sell_usdt")]); await q.edit_message_text("📥 Configure each customer-facing deposit button. Set its button title and put the complete account/address information and payment instructions in Destination details. A method appears to users only when enabled and details are saved.",reply_markup=kb(rows)); return
    if action.startswith("sell_admin_network_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        slug=action.removeprefix("sell_admin_network_")
        if slug not in SELL_NETWORKS: await q.edit_message_text("Unknown method."); return
        dest=setting_value(f"sell_network_{slug}_destination","") or ""
        button_label=setting_value(f"sell_network_{slug}_label",SELL_NETWORKS[slug]) or SELL_NETWORKS[slug]
        await q.edit_message_text(
            f"{SELL_NETWORKS[slug]}\nCustomer button: {button_label}\nStatus: {'Enabled' if setting_enabled(f'sell_network_{slug}_enabled') else 'Disabled'}\nDestination details:\n{dest or 'Not set'}",
            reply_markup=kb([
                [("✏️ Edit customer button title","sell_admin_edit_sell_network_"+slug+"_label")],
                [("✏️ Edit destination details","sell_admin_edit_sell_network_"+slug+"_destination")],
                [("🟢 Enable / 🔴 Disable","sell_admin_toggle_sell_network_"+slug+"_enabled")],
                [("⬅️ Back","sell_admin_networks")]
            ])
        ); return
    if action == "sell_admin_messages":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        items=[("Unavailable notice","sell_unavailable_message"),("Payout method question","sell_payout_prompt"),("Account number prompt","sell_account_number_prompt"),("Account holder prompt","sell_account_name_prompt"),("Step 3 question / rates / instructions","sell_deposit_prompt"),("Deposit account details screen","sell_network_details_prompt"),("USDT amount prompt","sell_amount_prompt"),("Final payment + screenshot instructions","sell_receipt_prompt"),("Proof received confirmation","sell_confirmation_message")]
        rows=[[(f"{label} · {len(setting_value(key,'') or '')} chars",f"sell_admin_edit_{key}")] for label,key in items]; rows.append([("⬅️ Back","admin_sell_usdt")])
        await q.edit_message_text("📝 Edit customer-visible Sell USDT text. Placeholders: {method}, {account_number}, {account_name}, {payout_summary}, {rates}, {network}, {destination}, {amount}, {rate}, {total}, {order_id}.",reply_markup=kb(rows)); return
    if action.startswith("sell_admin_toggle_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        key=action.removeprefix("sell_admin_toggle_")
        if not key.endswith("_enabled") or not key.startswith(("sell_payout_","sell_network_")): await q.edit_message_text("Unknown setting."); return
        val="false" if setting_enabled(key) else "true"
        with db() as c: c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,val))
        await q.edit_message_text(f"✅ {key} {'enabled' if val=='true' else 'disabled'}.",reply_markup=kb([[("🏦 Payout methods","sell_admin_payouts")],[("📥 Deposit methods","sell_admin_networks")],[("⬅️ Back","admin_sell_usdt")]])); return
    if action.startswith("sell_admin_edit_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        key=action.removeprefix("sell_admin_edit_"); allowed={"sell_usdt_rate_1_2","sell_usdt_rate_2_5","sell_usdt_rate_5_plus","sell_unavailable_message","sell_payout_prompt","sell_account_number_prompt","sell_account_name_prompt","sell_deposit_prompt","sell_network_details_prompt","sell_amount_prompt","sell_receipt_prompt","sell_confirmation_message"}
        if key.startswith("sell_network_") and key.endswith(("_destination", "_label")): allowed.add(key)
        if key not in allowed: await q.edit_message_text("Setting not editable here."); return
        set_pending(uid,"sell_admin_setting",{"key":key}); await q.edit_message_text(f"✏️ Edit {key}\nCurrent value:\n{setting_value(key,'(not set)')}\n\nSend the new value. Send 'off' to clear optional text. Rates must be positive numbers.",reply_markup=kb([[("❌ Cancel","admin_sell_usdt")]])); return
    if action in ("sell_admin_orders_all","sell_admin_orders_pending"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        where="AND status IN ('awaiting_payment_proof','pending_admin_approval','payment_verified')" if action.endswith("pending") else ""
        with db() as c: orders=c.execute(f"SELECT id,user_id,amount_usdt,total_etb,status FROM crypto_orders WHERE side='sell' {where} ORDER BY id DESC LIMIT 30").fetchall()
        rows=[[(f"#{o['id']} · {o['amount_usdt']:g} USDT · {o['total_etb']:g} ETB · {o['status']}",f"sell_admin_order_{o['id']}")] for o in orders]; rows.append([("⬅️ Back","admin_sell_usdt")])
        await q.edit_message_text("💸 SELL USDT ORDERS — select one to inspect or reply.",reply_markup=kb(rows)); return
    if action.startswith("sell_admin_order_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        try: oid=int(action.rsplit("_",1)[1])
        except ValueError: await q.edit_message_text("Invalid order."); return
        with db() as c: order=c.execute("SELECT * FROM crypto_orders WHERE id=? AND side='sell'",(oid,)).fetchone()
        if not order: await q.edit_message_text("Order not found."); return
        body=f"💸 SELL USDT ORDER #{oid}\nUser: {order['user_id']}\nAmount: {order['amount_usdt']:g} USDT\nRate: {order['rate_etb']:g} ETB/USDT\nPayout: {order['total_etb']:g} ETB\nPayout method: {PAYMENT_METHODS.get(order['payout_method'],order['payout_method'] or '—')}\nPayout number: {order['payout_account_number'] or '—'}\nHolder: {order['payout_account_name'] or '—'}\nUSDT method: {SELL_NETWORKS.get(order['transfer_method'],order['transfer_method'] or '—')}\nDestination: {order['transfer_destination'] or '—'}\nStatus: {order['status']}"
        rows=[[("💬 Reply (text/photo/document)",f"sell_admin_reply_{oid}")]]
        if order["status"]=="pending_admin_approval": rows.append([("✅ Verify transfer",f"sellorder_verify_{oid}"),("❌ Reject",f"sellorder_reject_{oid}")])
        rows.extend([[("⬅️ All orders","sell_admin_orders_all")],[("⬅️ Sell USDT Management","admin_sell_usdt")]])
        await q.edit_message_text(body,reply_markup=kb(rows))
        if order["receipt"]:
            try:
                kind,fid=order["receipt"].split(":",1)
                if kind=="photo": await context.bot.send_photo(uid,fid,caption=f"Transfer proof for order #{oid}")
                elif kind=="document": await context.bot.send_document(uid,fid,caption=f"Transfer proof for order #{oid}")
            except Exception: log.warning("Could not display sell order proof %s",oid)
        return
    if action.startswith("sell_admin_reply_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        oid=int(action.rsplit("_",1)[1])
        with db() as c: order=c.execute("SELECT user_id FROM crypto_orders WHERE id=? AND side='sell'",(oid,)).fetchone()
        if not order: await q.edit_message_text("Order not found."); return
        set_pending(uid,"sell_admin_chat",{"order_id":oid,"target_user_id":order["user_id"]}); await q.edit_message_text(f"Send your text, photo, or document reply for Sell USDT order #{oid}."); return
    if action.startswith("sell_user_chat_"):
        try: oid=int(action.rsplit("_",1)[1])
        except ValueError: await q.edit_message_text("Invalid order."); return
        with db() as c: order=c.execute("SELECT id FROM crypto_orders WHERE id=? AND user_id=? AND side='sell'",(oid,uid)).fetchone()
        if not order: await q.edit_message_text("Order not found."); return
        set_pending(uid,"sell_user_chat",{"order_id":oid}); await q.edit_message_text(f"💬 Chat with admin about Sell USDT order #{oid}. Send text, photo, or document.",reply_markup=kb([[("❌ Cancel","home")]])); return
    if action.startswith("sell_payout_"):
        slug = action[len("sell_payout_"):]
        if slug not in PAYMENT_METHODS or not setting_enabled(f"sell_payout_{slug}_enabled"):
            await q.edit_message_text("That payout method is currently disabled.", reply_markup=kb([[("⬅️ Marketplace", "market")]]))
            return
        set_pending(uid, "sell_account_number", {"method": slug})
        await q.edit_message_text(
            setting_value("sell_account_number_prompt", "💸 Step 2 — Enter the account/phone number where you want to receive ETB.") + f"\n\n{PAYMENT_METHODS[slug]}",
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

    if action.startswith("sell_network_continue_"):
        slug = action[len("sell_network_continue_"):]
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
            payout = decode_pending(pending["data"]) if pending and pending["action"] == "sell_network_confirm" else {}
        if slug not in SELL_NETWORKS or not payout or payout.get("network") != slug:
            await q.edit_message_text("Your session expired. Please restart Sell USDT.", reply_markup=kb([[("⬅️ Marketplace","market")]]))
            return
        set_pending(uid, "sell_amount", payout)
        amount_prompt = setting_value("sell_amount_prompt", "💸 Enter the amount of USDT you will send.\nDeposit method: {network}\nCurrent sell rates:\n{rates}\n\nMinimum 1 USDT.")
        amount_prompt = render_digital_template(amount_prompt, {"network":setting_value(f"sell_network_{slug}_label", SELL_NETWORKS[slug]),"rates":rates_text("sell")})
        await q.edit_message_text(amount_prompt, reply_markup=kb([[("❌ Cancel | አቋርጥ","home")]]))
        return

    if action.startswith("sell_network_"):
        slug = action[len("sell_network_"):]
        if slug not in SELL_NETWORKS or not setting_enabled(f"sell_network_{slug}_enabled"):
            await q.edit_message_text("That USDT deposit method is currently disabled.", reply_markup=kb([[("⬅️ Marketplace","market")]]))
            return
        destination = setting_value(f"sell_network_{slug}_destination", "").strip()
        if not destination:
            await q.edit_message_text("The admin has not configured destination details for this method.", reply_markup=kb([[("⬅️ Marketplace","market")]]))
            return
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
            payout = decode_pending(pending["data"]) if pending and pending["action"] == "sell_network_select" else {}
        if not payout:
            await q.edit_message_text("Your payout details were not found. Please restart Sell USDT.", reply_markup=kb([[("⬅️ Marketplace","market")]]))
            return
        payout.update({"network":slug, "destination":destination})
        set_pending(uid, "sell_network_confirm", payout)
        network_label = setting_value(f"sell_network_{slug}_label", SELL_NETWORKS[slug]).strip() or SELL_NETWORKS[slug]
        detail_template = setting_value(
            "sell_network_details_prompt",
            "💸 USDT deposit details\n\nMethod: {network}\n\n{destination}\n\nAfter completing the payment, send a clear screenshot of the transfer. Keep this information for your records."
        )
        detail_text = render_digital_template(detail_template, {
            "network":network_label, "destination":destination,
            "payout_summary":"", "rates":rates_text("sell")
        })
        await q.edit_message_text(detail_text, reply_markup=kb([
            [("➡️ Continue / Enter USDT amount","sell_network_continue_"+slug)],
            [("❌ Cancel | አቋርጥ","home")]
        ]))
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
            f"Receiving destination: {order['transfer_destination'] or '—'}\nStatus: {order['status']}"
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
            order = c.execute("SELECT user_id,status,side,amount_usdt FROM crypto_orders WHERE id=?", (order_id,)).fetchone()
            changed = False
            if order and order["status"] == "pending_admin_approval":
                cur = c.execute("UPDATE crypto_orders SET status='rejected',admin_id=?,updated_at=? WHERE id=? AND status='pending_admin_approval'", (uid, now(), order_id))
                changed = cur.rowcount == 1
                if changed and order["side"] == "buy":
                    c.execute("UPDATE settings SET value=CAST(value AS REAL)+? WHERE key='buy_usdt_stock'", (str(order["amount_usdt"]),))
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

    if action == "sell_network_select":
        await message.reply_text("Please tap one of the USDT deposit buttons above. I will show the exact account/address information before asking for the amount.")
        return True

    if action == "sell_network_confirm":
        await message.reply_text("Please tap the Continue button to enter the USDT amount, or cancel and restart the form.")
        return True

    if action == "sell_account_number":
        digits = "".join(ch for ch in value if ch.isdigit())
        if not digits or digits != value.strip():
            await message.reply_text("Please enter the account number using digits only.")
            return True
        state["account_number"] = digits
        set_pending(uid, "sell_account_name", state)
        await message.reply_text(
            setting_value("sell_account_name_prompt", "👤 Enter the account holder name exactly as registered with the bank/Telebirr:"),
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
        payout_summary = f"• Method: {PAYMENT_METHODS.get(state['method'], state['method'])}\n• Phone/account number: {state['account_number']}\n• Account name: {state['account_name']}"
        deposit_prompt = setting_value("sell_deposit_prompt", "💸 Step 3 of 4 — Choose USDT deposit method | ደረጃ 3 — የUSDT ማስገቢያ መንገድ ይምረጡ\n\nYour ETB payout destination | የብር መቀበያ መረጃዎ\n{payout_summary}\n\n📈 Sell USDT Rates\n{rates}\n\nHow will you send the USDT?")
        deposit_prompt = render_digital_template(deposit_prompt, {"payout_summary":payout_summary,"method":PAYMENT_METHODS.get(state['method'],state['method']),"account_number":state['account_number'],"account_name":state['account_name'],"rates":rates_text("sell")})
        await message.reply_text(deposit_prompt,
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
        receipt_prompt = setting_value("sell_receipt_prompt", "💸 Step 4 of 4 — {network}\n\nSend USDT to this Binance Pay ID / destination: {destination}\n\nAmount: {amount} USDT\nRate: {rate} ETB/USDT\nExpected ETB payout: {total} ETB\n\nUpload a clear transfer screenshot showing amount, status, recipient, time, and order ID.")
        receipt_prompt = render_digital_template(receipt_prompt, {"order_id":order_id,"amount":f"{amount:g}","rate":f"{rate:g}","total":f"{total:g}","network":SELL_NETWORKS.get(state['network'],state['network']),"destination":state['destination']})
        await message.reply_text(receipt_prompt,
            reply_markup=kb([[("💬 Message admin about this order",f"sell_user_chat_{order_id}")],[("❌ Cancel | አቋርጥ", "home")]]),
        )
        return True

    if action == "sell_admin_setting":
        if not is_admin(uid): await message.reply_text("⛔ Admin access only."); return True
        key=str(state.get("key","")); allowed={"sell_usdt_rate_1_2","sell_usdt_rate_2_5","sell_usdt_rate_5_plus","sell_unavailable_message","sell_payout_prompt","sell_account_number_prompt","sell_account_name_prompt","sell_deposit_prompt","sell_network_details_prompt","sell_amount_prompt","sell_receipt_prompt","sell_confirmation_message"}
        if key.startswith("sell_network_") and key.endswith(("_destination", "_label")): allowed.add(key)
        if key not in allowed: await message.reply_text("Invalid setting."); return True
        saved="" if value.lower() in {"off","none"} else value
        if key.startswith("sell_usdt_rate_"):
            try:
                n=Decimal(saved)
                if not n.is_finite() or n<=0: raise ValueError()
            except (ValueError,InvalidOperation): await message.reply_text("Enter a valid rate greater than zero."); return True
        if len(saved)>3500: await message.reply_text("Keep the value under 3,500 characters."); return True
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,saved)); c.execute("DELETE FROM pending_inputs WHERE user_id=?",(uid,))
        await message.reply_text(f"✅ Saved {key}.",reply_markup=kb([[("💸 Sell USDT Management","admin_sell_usdt")],[("📝 Customer messages","sell_admin_messages")]])); return True
    if action == "sell_admin_chat":
        if not is_admin(uid): await message.reply_text("⛔ Admin access only."); return True
        oid=int(state.get("order_id",0)); target=int(state.get("target_user_id",0))
        try:
            if message.photo: await context.bot.send_photo(target,message.photo[-1].file_id,caption=f"📩 Admin · Sell USDT order #{oid}\n{message.caption or ''}"[:1024])
            elif message.document: await context.bot.send_document(target,message.document.file_id,caption=f"📩 Admin · Sell USDT order #{oid}\n{message.caption or ''}"[:1024])
            elif value: await context.bot.send_message(target,f"📩 Admin · Sell USDT order #{oid}\n\n{value}")
            else: await message.reply_text("Send text, photo, or document."); return True
        except Exception: await message.reply_text("Could not deliver reply."); return True
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(uid,))
        await message.reply_text("✅ Reply delivered to user."); return True
    if action == "sell_user_chat":
        oid=int(state.get("order_id",0))
        with db() as c: order=c.execute("SELECT id FROM crypto_orders WHERE id=? AND user_id=? AND side='sell'",(oid,uid)).fetchone()
        if not order: await message.reply_text("Order not found."); return True
        note=f"💬 SELL USDT USER MESSAGE · order #{oid}\nUser ID: {uid}\n"; delivered=False
        for aid in ADMIN_IDS:
            try:
                if message.photo: await context.bot.send_photo(aid,message.photo[-1].file_id,caption=(note+(message.caption or ""))[:1024])
                elif message.document: await context.bot.send_document(aid,message.document.file_id,caption=(note+(message.caption or ""))[:1024])
                elif value: await context.bot.send_message(aid,note+"\n"+value)
                else: continue
                await context.bot.send_message(aid,"Reply to this user:",reply_markup=kb([[("💬 Reply to user",f"sell_admin_reply_{oid}")],[("📋 Open order",f"sell_admin_order_{oid}")]])); delivered=True
            except Exception: log.exception("Could not forward Sell USDT message")
        if not delivered: await message.reply_text("Could not deliver the message to admin."); return True
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(uid,))
        await message.reply_text("✅ Message sent to admin. Replies arrive here."); return True

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


def _digital_gateway_is_enabled(value):
    """Normalize enabled flags from SQLite and PostgreSQL rows."""
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def digital_gateways():
    """Return enabled Gemini gateways, falling back to shared configured payment methods."""
    # Read rows first and normalize the enabled flag in Python. This avoids a
    # backend/type mismatch making an enabled gateway disappear from checkout.
    with db() as c:
        configured_rows = c.execute(
            "SELECT * FROM digital_payment_gateways ORDER BY name COLLATE NOCASE"
        ).fetchall()
    configured = [row for row in configured_rows if _digital_gateway_is_enabled(row["enabled"])]
    if configured:
        return configured

    # The bot also has shared Telebirr/CBE settings used by other purchase flows.
    # Reuse only methods that are explicitly enabled and have payment details.
    fallback = []
    try:
        for slug, label in enabled_buy_methods():
            gateway = buy_usdt_gateway(slug)
            fallback.append({
                "id": slug,
                "name": gateway.get("name") or label,
                "account_number": gateway.get("number") or gateway.get("details") or "",
                "account_name": gateway.get("account_name") or "",
                "instructions": gateway.get("details") or gateway.get("receipt_amharic") or gateway.get("after_payment") or "",
                "warning": gateway.get("warning") or "",
                "enabled": 1,
            })
    except Exception:
        log.exception("Could not load shared payment methods for Gemini Pro")
    return fallback


def digital_gateway(gateway_id):
    """Look up an enabled Gemini gateway, or an enabled shared payment method."""
    with db() as c:
        gateway = c.execute(
            "SELECT * FROM digital_payment_gateways WHERE id=?", (gateway_id,)
        ).fetchone()
    if gateway and _digital_gateway_is_enabled(gateway["enabled"]):
        return gateway
    for item in digital_gateways():
        if item["id"] == gateway_id:
            return item
    return None


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
    # Any admin navigation away from an unfinished digital-product form acts as Cancel.
    # This prevents the next ordinary chat message from being mistaken for a product field.
    if action == "admin_digital_products" or action == "admin_digital_orders" or action.startswith("digital_admin_"):
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='digital_admin_wizard'", (uid,))
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
            [(digital_template("digital_buy_button", "🌟 Buy now"), f"digital_buy_{product_id}")],
            [(digital_template("digital_cancel_button", "❌ Cancel | አቋርጥ"), "digital_cancel")]
        ]))
        return

    if action == "digital_cancel":
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text(
            digital_template("digital_cancel_message", "Purchase cancelled."),
            reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")], [("⬅️ Dashboard", "home")]])
        )
        return

    if action.startswith("digital_buy_"):
        product_id = action[len("digital_buy_"):]
        product = digital_product(product_id)
        if not product or not product["active"]:
            await q.edit_message_text(digital_template("digital_no_stock_message", "Product unavailable."),
                                      reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")]]))
            return
        if int(product["stock"]) <= 0:
            await q.edit_message_text(digital_template("digital_no_stock_message", "This product is out of stock."),
                                      reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")]]))
            return
        gateways = digital_gateways()
        if not gateways:
            await q.edit_message_text(digital_template("digital_no_gateway_message", "Payment unavailable."),
                                      reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")]]))
            return
        if len(gateways) == 1:
            await show_digital_payment(q, uid, product, gateways[0])
            return
        rows = [[(g["name"], f"digital_gateway_{product_id}::{g['id']}")] for g in gateways]
        rows.append([(digital_template("digital_cancel_button", "❌ Cancel | አቋርጥ"), "digital_cancel")])
        set_pending(uid, "digital_choose_gateway", {"product_id": product_id})
        await q.edit_message_text(digital_template("digital_choose_gateway_prompt", "💳 Choose your payment method:"), reply_markup=kb(rows))
        return

    if action.startswith("digital_gateway_"):
        parts = action[len("digital_gateway_"):].split("::", 1)
        if len(parts) != 2:
            await q.edit_message_text(digital_template("digital_invalid_payment_message", "That payment option is no longer available. Please return to the marketplace and choose again."), reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")]]))
            return
        product_id, gateway_id = parts
        product = digital_product(product_id)
        gateway = digital_gateway(gateway_id)
        if not product or not gateway or int(product["stock"]) <= 0:
            await q.edit_message_text(digital_template("digital_no_stock_message", "Product or payment option unavailable."),
                                      reply_markup=kb([[(digital_template("digital_market_button", "⬅️ Marketplace"), "market")]]))
            return
        await show_digital_payment(q, uid, product, gateway)
        return

    if action == "admin_digital_products":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        products = digital_products(active_only=False)
        rows = [
            [("➕ Add a new product", "digital_admin_add_product")],
            [("💳 Payment methods", "digital_admin_gateways")],
            [("📝 Customer-facing text", "digital_admin_messages")],
            [("📥 Review pending orders", "admin_digital_orders")],
        ]
        for p in products[:40]:
            status = "🟢 Live" if p["active"] else "⚪ Hidden"
            rows.append([(f"{status} {p['name']} · {p['price']:g} ETB · stock {p['stock']}", f"digital_admin_product_{p['id']}")])
        rows.append([("⬅️ Admin Dashboard", "admin")])
        await q.edit_message_text(
            "🌟 Gemini Pro & Digital Products\n\n"
            "Manage what customers see, one step at a time. Choose a product to edit it, or add a new one. "
            "Only active products appear in the customer marketplace.",
            reply_markup=kb(rows)
        )
        return

    if action == "digital_admin_help":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        await q.edit_message_text(
            "🛠 Advanced setup commands (optional)\n\n"
            "/product_add ID | NAME | MONTHS | PRICE | STOCK\n"
            "/product_set ID FIELD VALUE\n"
            "/gateway_set ID | NAME | ACCOUNT_NUMBER | ACCOUNT_NAME | INSTRUCTIONS | WARNING\n"
            "/gateway_toggle ID true/false\n"
            "/digital_text KEY MESSAGE\n\n"
            "You can manage products, payment methods, and customer-facing messages with the buttons instead.",
            reply_markup=kb([[("⬅️ Digital Products", "admin_digital_products")]])
        )
        return

    if action == "digital_admin_add_product":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        set_pending(uid, "digital_admin_wizard", {"mode":"add_product", "step":"id", "values":{}})
        await q.edit_message_text(
            "➕ Add a product (step 1 of 10)\n\n"
            "Send a short unique ID using English letters, numbers, and underscores.\nExample: gemini_pro_1m",
            reply_markup=kb([[("❌ Cancel", "admin_digital_products")]])
        )
        return

    if action == "digital_admin_gateways":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            gateways = c.execute("SELECT * FROM digital_payment_gateways ORDER BY name COLLATE NOCASE").fetchall()
        rows = [[("➕ Add payment method", "digital_admin_add_gateway")]]
        for g in gateways[:30]:
            rows.append([(f"{'🟢' if g['enabled'] else '⚪'} {g['name']}", f"digital_admin_gateway_{g['id']}")])
        rows.extend([[("⬅️ Digital Products", "admin_digital_products")]])
        await q.edit_message_text(
            "💳 Payment Methods\n\nOnly enabled methods are shown to customers. Select one to update its details or status.",
            reply_markup=kb(rows)
        )
        return

    if action == "digital_admin_add_gateway":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        set_pending(uid, "digital_admin_wizard", {"mode":"add_gateway", "step":"id", "values":{}})
        await q.edit_message_text(
            "➕ Add payment method (step 1 of 6)\n\nSend a short unique ID. Example: telebirr",
            reply_markup=kb([[("❌ Cancel", "digital_admin_gateways")]])
        )
        return

    if action == "digital_admin_messages":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        message_settings = [
            ("🛍 Product details shown to customers", "digital_product_details_template"),
            ("💳 Payment instructions shown to customers", "digital_payment_template"),
            ("⏳ Receipt received / waiting for review", "digital_waiting_message"),
            ("🚫 Out-of-stock message", "digital_no_stock_message"),
            ("⚠️ No-payment-method message", "digital_no_gateway_message"),
            ("⚠️ Invalid payment option message", "digital_invalid_payment_message"),
            ("❌ Purchase-cancelled message", "digital_cancel_message"),
            ("🛒 Buy button label", "digital_buy_button"),
            ("❌ Cancel button label", "digital_cancel_button"),
            ("⬅️ Marketplace button label", "digital_market_button"),
            ("💳 Choose-payment prompt", "digital_choose_gateway_prompt"),
            ("🛍 Marketplace heading", "digital_market_title"),
            ("🏷 Product listing label template", "digital_market_product_button_template"),
            ("📸 Receipt upload instructions", "digital_receipt_upload_prompt"),
        ]
        rows = [[(label, f"digital_admin_message_{key}")] for label,key in message_settings]
        rows.append([("⬅️ Digital Products", "admin_digital_products")])
        await q.edit_message_text(
            "📝 Customer-facing text\n\nChoose one message or button label to edit. These changes affect what customers see. "
            "You can use multiple lines in message templates.",
            reply_markup=kb(rows)
        )
        return

    if action.startswith("digital_admin_message_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        key = action[len("digital_admin_message_"):]
        allowed = {
            "digital_product_details_template", "digital_payment_template", "digital_waiting_message",
            "digital_no_stock_message", "digital_no_gateway_message", "digital_invalid_payment_message", "digital_cancel_message",
            "digital_buy_button", "digital_cancel_button",
            "digital_market_button", "digital_choose_gateway_prompt", "digital_market_title",
            "digital_market_product_button_template", "digital_receipt_upload_prompt"
        }
        if key not in allowed:
            await q.edit_message_text("This message is not editable here."); return
        current = digital_template(key, "")
        if not current:
            current = "(not set)"
        set_pending(uid, "digital_admin_wizard", {"mode":"edit_message", "key":key})
        await q.edit_message_text(
            f"✏️ Edit customer-facing text\n\nSetting: {key}\n\nCurrent value:\n{current[:2500]}\n\n"
            "Send the new text in your next message. For product/payment templates, keep placeholders such as "
            "{name}, {price}, {stock}, {gateway_name}, {account_number}, and {instructions} if you want those details displayed.",
            reply_markup=kb([[("❌ Cancel", "digital_admin_messages")]])
        )
        return

    if action.startswith("digital_admin_gateway_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        gateway_id = action[len("digital_admin_gateway_"):]
        with db() as c:
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=?", (gateway_id,)).fetchone()
        if not gateway:
            await q.edit_message_text("Payment method not found.", reply_markup=kb([[("⬅️ Payment methods","digital_admin_gateways")]])); return
        rows = []
        for field,label in [
            ("name","Name shown to customers"), ("account_number","Account number / payment ID"),
            ("account_name","Account holder name"), ("instructions","Payment instructions"),
            ("warning","Warning / extra information")
        ]:
            current = str(gateway[field] or "—").replace("\n"," ")
            rows.append([(f"✏️ {label}: {current[:18]}", f"digital_admin_gatewayfield_{gateway_id}::{field}")])
        rows.append([("🔁 Enable / hide from customers", f"digital_admin_gatewaytoggle_{gateway_id}")])
        rows.extend([[("⬅️ Payment methods","digital_admin_gateways")], [("⬅️ Digital Products","admin_digital_products")]])
        await q.edit_message_text(
            f"💳 {gateway['name']}\nStatus: {'Enabled — customers can use it' if gateway['enabled'] else 'Disabled — hidden from customers'}\n\n"
            "Choose one detail to change, or toggle visibility.",
            reply_markup=kb(rows)
        )
        return

    if action.startswith("digital_admin_gatewayfield_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        parts = action[len("digital_admin_gatewayfield_"):].split("::",1)
        if len(parts) != 2 or parts[1] not in {"name","account_number","account_name","instructions","warning"}:
            await q.edit_message_text("Invalid payment-method field."); return
        gateway_id, field = parts
        with db() as c:
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=?", (gateway_id,)).fetchone()
        if not gateway:
            await q.edit_message_text("Payment method not found."); return
        set_pending(uid, "digital_admin_wizard", {"mode":"edit_gateway", "gateway_id":gateway_id, "field":field})
        await q.edit_message_text(
            f"✏️ Update {field.replace('_',' ').title()}\n\nCurrent value: {gateway[field] or '—'}\n\nSend the new value in your next message.",
            reply_markup=kb([[("❌ Cancel","digital_admin_gateways")]])
        )
        return

    if action.startswith("digital_admin_gatewaytoggle_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        gateway_id = action[len("digital_admin_gatewaytoggle_"):]
        with db() as c:
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=?", (gateway_id,)).fetchone()
            if gateway:
                new_status = 0 if gateway["enabled"] else 1
                c.execute("UPDATE digital_payment_gateways SET enabled=?,updated_at=? WHERE id=?", (new_status,now(),gateway_id))
        if not gateway:
            await q.edit_message_text("Payment method not found."); return
        await q.edit_message_text(
            f"✅ {gateway['name']} is now {'enabled and visible to customers' if new_status else 'disabled and hidden from customers'}.",
            reply_markup=kb([[("⬅️ Payment method settings",f"digital_admin_gateway_{gateway_id}")], [("⬅️ Payment methods","digital_admin_gateways")]])
        )
        return

    if action.startswith("digital_admin_product_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        product_id = action[len("digital_admin_product_"):]
        product = digital_product(product_id)
        if not product:
            await q.edit_message_text("Product not found.", reply_markup=kb([[("⬅️ Digital Products","admin_digital_products")]])); return
        rows = []
        for field,label in [
            ("name","Product name"), ("duration_months","Duration in months"), ("price","Price in ETB"),
            ("stock","Available stock"), ("description","Description"), ("features","Features (one per line)"),
            ("important_note","Important note"), ("notice","Notice"), ("warranty","Warranty text")
        ]:
            current = str(product[field] or "—").replace("\n"," ")
            rows.append([(f"✏️ {label}: {current[:18]}", f"digital_admin_productfield_{product_id}::{field}")])
        rows.append([(f"{'🙈 Hide' if product['active'] else '🟢 Publish'} product", f"digital_admin_producttoggle_{product_id}")])
        rows.extend([[("⬅️ All products","admin_digital_products")], [("⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text(
            f"🌟 {product['name']}\nID: {product['id']}\nPrice: {product['price']:g} ETB · Duration: {product['duration_months']} months\n"
            f"Stock: {product['stock']} · Status: {'Live' if product['active'] else 'Hidden'}\n\n"
            "Choose one field to update. Changes are saved immediately and reflected in the customer marketplace.",
            reply_markup=kb(rows)
        )
        return

    if action.startswith("digital_admin_productfield_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        parts = action[len("digital_admin_productfield_"):].split("::",1)
        allowed_fields = {"name","duration_months","price","stock","description","features","important_note","notice","warranty"}
        if len(parts) != 2 or parts[1] not in allowed_fields:
            await q.edit_message_text("Invalid product field."); return
        product_id, field = parts
        product = digital_product(product_id)
        if not product:
            await q.edit_message_text("Product not found."); return
        set_pending(uid, "digital_admin_wizard", {"mode":"edit_product", "product_id":product_id, "field":field})
        current = str(product[field] or "—")
        await q.edit_message_text(
            f"✏️ Update {field.replace('_',' ').title()}\n\nCurrent value:\n{current[:1800]}\n\n"
            + ("Send a positive whole number." if field == "duration_months" else
               "Send a whole number of 0 or more." if field == "stock" else
               "Send a number of 0 or more." if field == "price" else
               "Send the new text. For features, put each feature on a separate line."),
            reply_markup=kb([[("❌ Cancel","digital_admin_product_"+product_id)]])
        )
        return

    if action.startswith("digital_admin_producttoggle_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        product_id = action[len("digital_admin_producttoggle_"):]
        with db() as c:
            product = c.execute("SELECT * FROM digital_products WHERE id=?", (product_id,)).fetchone()
            if product:
                new_status = 0 if product["active"] else 1
                c.execute("UPDATE digital_products SET active=?,updated_at=? WHERE id=?", (new_status,now(),product_id))
        if not product:
            await q.edit_message_text("Product not found."); return
        await q.edit_message_text(
            f"✅ {product['name']} is now {'hidden from' if not new_status else 'visible in'} the customer marketplace.",
            reply_markup=kb([[("⬅️ Product settings",f"digital_admin_product_{product_id}")], [("⬅️ All products","admin_digital_products")]])
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
        heading = "📥 Pending orders — choose an order to review:" if orders else "✅ No pending digital orders right now. New receipt submissions will appear here."
        await q.edit_message_text(heading, reply_markup=kb(rows))
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
        order_caption = (
            f"Digital order #{order_id}\nUser ID: {order['user_id']}\nProduct: {order['product_name']} "
            f"{order['duration_months']}m\nPrice: {order['price']:g} ETB\nPayment: {order['gateway_name']}\n"
            f"Status: {order['status']}\nCreated: {order['created_at']}"
        )
        order_markup = kb([[("✉️ Reply / Send redeem link", f"digital_reply_{order_id}")],
                           [("🚫 Reject with message", f"digital_reject_{order_id}")],
                           [("⬅️ Pending orders", "admin_digital_orders")]])
        # Keep the actual customer receipt attached to the review details.
        # If opened from a text-only order list, send the saved receipt as a new media message.
        if q.message.photo or q.message.document:
            await q.edit_message_caption(caption=order_caption, reply_markup=order_markup)
        elif order["receipt_type"] == "photo":
            await context.bot.send_photo(
                chat_id=q.message.chat_id,
                photo=order["receipt_file_id"],
                caption=order_caption,
                reply_markup=order_markup,
            )
        else:
            await context.bot.send_document(
                chat_id=q.message.chat_id,
                document=order["receipt_file_id"],
                caption=order_caption,
                reply_markup=order_markup,
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
    text_body += "\n\n" + digital_template("digital_receipt_upload_prompt", "📸 After paying, upload a clear payment receipt screenshot as a photo or document.")
    set_pending(uid, "digital_receipt", {
        "product_id": product["id"], "gateway_id": gateway["id"],
        "product_name": product["name"], "duration_months": int(product["duration_months"]),
        "price": float(product["price"]), "gateway_name": gateway["name"],
    })
    await q.edit_message_text(text_body, reply_markup=kb([[(digital_template("digital_cancel_button", "❌ Cancel | አቋርጥ"), "digital_cancel")]]))


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    action = q.data
    if action.startswith(("digital_product_", "digital_buy_", "digital_gateway_", "digital_admin_",
                          "digital_order_view_", "digital_reply_", "digital_reject_")) or action in (
        "digital_cancel", "admin_digital_products", "admin_digital_orders", "digital_admin_help"
    ):
        await digital_callback(update, context, action)
        return
    if action in ("buy_usdt", "buy_asset", "sell_usdt", "sell_saved", "admin_crypto_orders", "buy_usdt_continue_destination", "admin_sell_usdt", "sell_admin_toggle", "sell_admin_rates", "sell_admin_payouts", "sell_admin_networks", "sell_admin_messages", "sell_admin_orders_all", "sell_admin_orders_pending") or action.startswith((
        "buy_usdt_gateway_", "buy_method_", "sell_payout_", "sell_network_", "crypto_order_view_", "buyorder_verify_", "sellorder_verify_", "buyorder_reject_", "sellorder_reject_", "sell_admin_network_", "sell_admin_toggle_", "sell_admin_edit_", "sell_admin_order_", "sell_admin_reply_", "sell_user_chat_"
    )):
        await crypto_callback(update, context, action)
        return
    if action == "home":
        # Dashboard acts as Cancel for unfinished flows. Release Buy USDT stock reservations if payment was not submitted.
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
            if pending and pending["action"] == "buy_receipt":
                pending_state = decode_pending(pending["data"])
                try:
                    pending_order_id = int(pending_state.get("order_id", 0))
                except (TypeError, ValueError):
                    pending_order_id = 0
                pending_order = c.execute(
                    "SELECT amount_usdt,status FROM crypto_orders WHERE id=? AND user_id=? AND side='buy'",
                    (pending_order_id, uid)
                ).fetchone()
                if pending_order and pending_order["status"] == "awaiting_payment_proof":
                    c.execute(
                        "UPDATE crypto_orders SET status='cancelled',updated_at=? WHERE id=? AND status='awaiting_payment_proof'",
                        (now(), pending_order_id)
                    )
                    c.execute(
                        "UPDATE settings SET value=CAST(value AS REAL)+? WHERE key='buy_usdt_stock'",
                        (str(pending_order["amount_usdt"]),)
                    )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("🏠 Main Dashboard", reply_markup=home_keyboard(is_admin(uid)))
    elif action == "jobs":
        with db() as c:
            tasks = c.execute("SELECT * FROM tasks WHERE active=1 AND completed_count<target ORDER BY id DESC").fetchall()
            claims = {r["task_id"]: r["status"] for r in c.execute("SELECT task_id,status FROM task_claims WHERE user_id=?", (uid,)).fetchall()}
            claim_counts = {r["task_id"]: r["n"] for r in c.execute("SELECT task_id,COUNT(*) n FROM task_claims GROUP BY task_id").fetchall()}
        rows = []
        for t in tasks:
            already_claimed = t["id"] in claims
            participant_limit = int(t["participant_limit"] or 0)
            claimed_users = int(claim_counts.get(t["id"], 0))
            if not already_claimed and participant_limit > 0 and claimed_users >= participant_limit:
                continue
            status = claims.get(t["id"])
            spots = "∞" if participant_limit == 0 else str(max(0, participant_limit - claimed_users))
            label = f"{'🟢' if status else '✨'} {t['title']} · {t['points']} pts · {t['completed_count']}/{t['target']} · {spots} spots"
            rows.append([(label[:60], f"task_{t['id']}")])
            if len(rows) >= 20:
                break
        if not rows:
            await q.edit_message_text("🧩 No tasks are open right now. Check back soon for new opportunities.", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
        rows.append([("🏆 Task leaderboard", "public_task_leaderboard")])
        rows.append([("⬅️ Dashboard", "home")])
        await q.edit_message_text("🧩 DAILY TASKS\n\nChoose a task to see its reward, progress and your personal invite link.\n\n🏆 The leaderboard ranks participants by verified joins.", reply_markup=kb(rows))
    elif action == "public_task_leaderboard":
        with db() as c:
            leaders = c.execute(
                "SELECT u.user_id,u.username,u.first_name,COUNT(e.id) AS joins_count,"
                "COUNT(DISTINCT tc.task_id) AS tasks_joined "
                "FROM task_claims tc JOIN users u ON u.user_id=tc.user_id "
                "LEFT JOIN invite_links l ON l.task_id=tc.task_id AND l.owner_user_id=tc.user_id "
                "LEFT JOIN invite_events e ON e.invite_link=l.invite_link "
                "GROUP BY u.user_id,u.username,u.first_name "
                "ORDER BY joins_count DESC,tasks_joined DESC,u.user_id ASC LIMIT 20"
            ).fetchall()
        lines = ["🏆 DAILY TASK LEADERBOARD", "Top participants · ranked by verified joins", ""]
        if leaders:
            for rank, person in enumerate(leaders, 1):
                name = ("@" + person["username"]) if person["username"] else (person["first_name"] or f"User {person['user_id']}")
                lines.append(f"{rank}. {name[:26]} — {int(person['joins_count'] or 0)} joins · {int(person['tasks_joined'] or 0)} tasks")
        else:
            lines.append("No participants have joined a task yet. Be the first!")
        await q.edit_message_text("\n".join(lines)[:3900], reply_markup=kb([
            [("🧩 Daily Tasks", "jobs"), ("🏠 Dashboard", "home")]
        ]))
    elif action.startswith("public_task_leaderboard_"):
        try:
            tid = int(action.rsplit("_", 1)[1])
        except (TypeError, ValueError):
            await q.edit_message_text("Invalid task.", reply_markup=kb([[("🧩 Daily Tasks", "jobs")]])); return
        with db() as c:
            task = c.execute("SELECT id,title FROM tasks WHERE id=?", (tid,)).fetchone()
            leaders = c.execute(
                "SELECT u.user_id,u.username,u.first_name,COUNT(e.id) AS joins_count,tc.status "
                "FROM task_claims tc JOIN users u ON u.user_id=tc.user_id "
                "LEFT JOIN invite_links l ON l.task_id=tc.task_id AND l.owner_user_id=tc.user_id "
                "LEFT JOIN invite_events e ON e.invite_link=l.invite_link "
                "WHERE tc.task_id=? GROUP BY u.user_id,u.username,u.first_name,tc.status "
                "ORDER BY joins_count DESC,u.user_id ASC LIMIT 20", (tid,)
            ).fetchall()
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("🧩 Daily Tasks", "jobs")]])); return
        lines = [f"🏆 TASK LEADERBOARD · {task['title']}", "Participants ranked by verified joins", ""]
        if leaders:
            for rank, person in enumerate(leaders, 1):
                name = ("@" + person["username"]) if person["username"] else (person["first_name"] or f"User {person['user_id']}")
                lines.append(f"{rank}. {name[:26]} — {int(person['joins_count'] or 0)} joins · {person['status']}")
        else:
            lines.append("No participants have claimed this task yet.")
        await q.edit_message_text("\n".join(lines)[:3900], reply_markup=kb([
            [("🔄 Refresh", f"public_task_leaderboard_{tid}")],
            [("🧩 Daily Tasks", "jobs"), ("🏠 Dashboard", "home")]
        ]))
    elif action.startswith("task_"):
        try:
            tid = int(action.split("_",1)[1])
        except (TypeError, ValueError):
            await q.edit_message_text("That task button is invalid.", reply_markup=kb([[("⬅️ Daily Tasks","jobs")]])); return
        with db() as c:
            t = c.execute("SELECT * FROM tasks WHERE id=? AND active=1", (tid,)).fetchone()
            claim = c.execute("SELECT status,invite_link FROM task_claims WHERE task_id=? AND user_id=?", (tid,uid)).fetchone()
            claimed_users = c.execute("SELECT COUNT(*) n FROM task_claims WHERE task_id=?", (tid,)).fetchone()["n"]
        if not t:
            await q.edit_message_text("This task is no longer available.", reply_markup=kb([[("⬅️ Daily Tasks","jobs")]])); return
        status = claim["status"] if claim else "Ready to claim"
        invite = claim["invite_link"] if claim else ""
        participant_limit = int(t["participant_limit"] or 0)
        if not claim and participant_limit > 0 and claimed_users >= participant_limit:
            await q.edit_message_text("👥 This task has reached its participant limit. Please choose another task.", reply_markup=kb([[("⬅️ Daily Tasks","jobs")]])); return
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
                    claim = c.execute("SELECT status,invite_link FROM task_claims WHERE task_id=? AND user_id=?", (tid,uid)).fetchone()
                status = claim["status"] if claim else "pending"
                invite = claim["invite_link"] if claim else invite
            except Exception:
                log.exception("Could not create invite link for task %s", tid)
                await q.edit_message_text(
                    "⚠️ We couldn't assign this task right now. The bot needs permission to create invite links in the target channel. Please try again later.",
                    reply_markup=kb([[("⬅️ Daily Tasks","jobs")]])
                ); return
        with db() as c:
            current = c.execute("SELECT COUNT(*) n FROM task_claims WHERE task_id=?", (tid,)).fetchone()["n"]
        slots = "Unlimited" if participant_limit == 0 else f"{current}/{participant_limit} users assigned"
        template = setting_value(f"task_message_template_{tid}", default_task_message_template())
        replacements = {
            "{title}": str(t["title"]),
            "{points}": str(t["points"]),
            "{channel}": str(t["channel"]),
            "{completed_count}": str(t["completed_count"]),
            "{target}": str(t["target"]),
            "{participants}": str(slots),
            "{status}": str(status),
            "{{PERSONAL_INVITE_LINK}}": str(invite if invite else "Not assigned"),
        }
        for token, replacement in replacements.items():
            template = template.replace(token, replacement)
        msg = template
        task_markup = kb([
            [("🔄 Refresh progress",f"task_{tid}")],
            [("🏆 Task leaderboard",f"public_task_leaderboard_{tid}")],
            [("🧩 More tasks","jobs"),("🏠 Dashboard","home")]
        ])
        media_raw = setting_value(f"task_message_media_{tid}", "")
        try:
            media_info = json.loads(media_raw) if media_raw else None
        except (TypeError, ValueError):
            media_info = None
        if isinstance(media_info, dict) and media_info.get("file_id") and media_info.get("kind") in {"photo","video","document"}:
            # Render the task template in the caption where Telegram's 1024-char caption
            # limit allows it. Long templates remain as a separate full text message.
            media_caption = msg if len(msg) <= 1024 else None
            try:
                send_kwargs = {"chat_id": uid, "reply_markup": task_markup}
                if media_info["kind"] == "photo":
                    await context.bot.send_photo(photo=media_info["file_id"], caption=media_caption, **send_kwargs)
                elif media_info["kind"] == "video":
                    await context.bot.send_video(video=media_info["file_id"], caption=media_caption, **send_kwargs)
                else:
                    await context.bot.send_document(document=media_info["file_id"], caption=media_caption, **send_kwargs)
                try:
                    await q.message.delete()
                except Exception:
                    pass
                if len(msg) > 1024:
                    await context.bot.send_message(chat_id=uid, text=msg, reply_markup=task_markup)
                return
            except Exception:
                log.exception("Could not send configured media for task %s", tid)
        await q.edit_message_text(msg, reply_markup=task_markup)
    elif action == "invite":
        if str(setting_value("invite_earn_active", "1")).strip().lower() not in {"1", "true", "yes", "on"}:
            unavailable_message = str(setting_value(
                "invite_earn_unavailable_message",
                "🔗 Invite & Earn is temporarily unavailable. Please check back later."
            ) or "").strip()
            unavailable_photo = str(setting_value("invite_earn_unavailable_photo", "") or "").strip()
            back_markup = kb([[("⬅️ Dashboard", "home")]])
            if unavailable_photo:
                try:
                    await q.message.reply_photo(
                        photo=unavailable_photo,
                        caption=unavailable_message[:1000] or None,
                        reply_markup=back_markup
                    )
                except Exception:
                    log.exception("Could not show configured Invite & Earn unavailable image")
                    await q.edit_message_text(
                        unavailable_message or "Invite & Earn is temporarily unavailable.",
                        reply_markup=back_markup
                    )
            else:
                await q.edit_message_text(
                    unavailable_message or "Invite & Earn is temporarily unavailable.",
                    reply_markup=back_markup
                )
            return
        bot = await context.bot.get_me()
        link = f"https://t.me/{bot.username}?start=ref_{uid}"
        with db() as c:
            invited = c.execute("SELECT COUNT(*) n FROM users WHERE referred_by=?", (uid,)).fetchone()["n"]
            rewards = c.execute("SELECT COALESCE(SUM(points_awarded),0) n FROM referral_rewards WHERE inviter_user_id=?", (uid,)).fetchone()["n"]
            user_row = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            pending = c.execute("SELECT COUNT(*) n FROM withdrawals WHERE user_id=? AND status='pending'", (uid,)).fetchone()["n"]
        wallet_points = int(user_row["points"] or 0) if user_row else 0
        reward_points = int(setting_value("referral_points", "0") or 0)
        minimum_points = int(setting_value("min_withdraw_points", "1000") or 1000)
        points_per_birr = max(1, int(setting_value("points_per_birr", "100") or 100))
        custom_message = str(setting_value("invite_earn_message", "") or "")
        template = custom_message or "🔗 INVITE & EARN\n\nInvite friends to KefiaETBot using your personal link. When a new user starts the bot through your link, you earn {reward_points} points.\n\n📌 Referral count: {referrals}\n⭐ Referral points earned: {earned_points}\n👛 Current wallet: {wallet_points} points\n💱 Conversion: {points_per_birr} points = 1 ETB\n💵 Estimated wallet value: {wallet_birr} ETB\n\nMinimum withdrawal: {minimum_points} points. Withdrawals are reviewed by admins."
        replacements = {
            "{link}": link, "{reward_points}": str(reward_points), "{referrals}": str(invited),
            "{earned_points}": str(int(rewards or 0)), "{wallet_points}": str(wallet_points),
            "{points_per_birr}": str(points_per_birr), "{wallet_birr}": f"{wallet_points / points_per_birr:.2f}",
            "{minimum_points}": str(minimum_points), "{pending_withdrawals}": str(pending),
        }
        for token, value in replacements.items():
            template = template.replace(token, value)
        if "{link}" not in custom_message:
            template += f"\n\n🔗 Your personal invite link:\n{link}"
        await q.edit_message_text(template, reply_markup=kb([
            [("🔄 Refresh statistics","invite")], [("👛 My Wallet","wallet"),("💸 Withdraw","withdraw")], [("⬅️ Dashboard","home")]
        ]))
    elif action.startswith("withdraw_method_"):
        method_slug = action.removeprefix("withdraw_method_")
        method = {"telebirr": "Telebirr", "cbe": "CBE"}.get(method_slug)
        if not method:
            await q.edit_message_text("Invalid withdrawal method. Please try again.", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
            pending = c.execute("SELECT 1 FROM withdrawals WHERE user_id=? AND status='pending' LIMIT 1", (uid,)).fetchone()
            points_now = int(u["points"]) if u else 0
            min_now = int(minimum["value"]) if minimum else 1000
            if pending:
                await q.edit_message_text("⏳ You already have a pending withdrawal. Please wait for admin review.", reply_markup=kb([[("👛 My Wallet","wallet")]])); return
            if points_now < min_now:
                await q.edit_message_text(f"💸 Your points are now below the withdrawal minimum.\nPoints: {points_now}\nMinimum: {min_now}", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?, 'withdraw_account_number', ?) ON CONFLICT(user_id) DO UPDATE SET action='withdraw_account_number',data=excluded.data",
                      (uid, json.dumps({"method": method})))
        await q.edit_message_text(f"✅ Payout method: {method}\n\nNow enter your {('Telebirr phone number' if method == 'Telebirr' else 'CBE account number')}:",
                                  reply_markup=kb([[("❌ Cancel","home")]]))
    elif action.startswith("withdraw_delivery_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: withdrawal_id = int(action.removeprefix("withdraw_delivery_"))
        except ValueError:
            await q.edit_message_text("Invalid withdrawal request."); return
        with db() as c:
            wd = c.execute("SELECT user_id,status FROM withdrawals WHERE id=?", (withdrawal_id,)).fetchone()
            if not wd or wd["status"] not in ("pending", "approved"):
                await q.edit_message_text("This withdrawal is not available for delivery."); return
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?, 'withdraw_delivery', ?) ON CONFLICT(user_id) DO UPDATE SET action='withdraw_delivery',data=excluded.data",
                      (uid, json.dumps({"withdrawal_id": withdrawal_id, "target_user_id": wd["user_id"]})))
        await q.edit_message_text(f"📨 Send the payout message for withdrawal #{withdrawal_id} as text, or send a photo/document with an optional caption. It will be delivered to the user.",
                                  reply_markup=kb([[("⬅️ Admin Dashboard","admin")]]))
    elif action == "wallet":
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            pending = c.execute("SELECT COUNT(*) n FROM withdrawals WHERE user_id=? AND status='pending'", (uid,)).fetchone()["n"]
        await q.edit_message_text(f"👛 Wallet\nAvailable points: {u['points'] if u else 0}\nPending withdrawals: {pending}\nPoints have no cash value until the admin sets a conversion and withdrawal policy.", reply_markup=kb([[("💸 Withdraw","withdraw")],[("⬅️ Dashboard","home")]]))
    elif action == "contact_admin":
        contact_username = str(setting_value("contact_admin_username", os.getenv("CONTACT_ADMIN_USERNAME", "")) or "").strip()
        contact_username = contact_username.removeprefix("@")
        if contact_username.lower().startswith("https://t.me/"):
            contact_username = contact_username.split("t.me/", 1)[1].split("/", 1)[0].split("?", 1)[0]
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", contact_username):
            await q.edit_message_text(
                "📞 Contact Admin",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👤 Open Admin Profile", url="https://t.me/" + contact_username)],
                                                   [InlineKeyboardButton("⬅️ Dashboard", callback_data="home")]])
            )
        else:
            await q.edit_message_text(
                "📞 Admin contact has not been configured yet. Please check back soon.",
                reply_markup=kb([[("⬅️ Dashboard","home")]])
            )
    elif action == "profile":
        with db() as c:
            u = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        await q.edit_message_text(f"👤 Account\nName: {q.from_user.full_name}\nUser ID: {uid}\nPoints: {u['points'] if u else 0}\nMember since: {u['joined_at'][:10] if u else '—'}", reply_markup=kb([[("⬅️ Dashboard","home")]]))
    elif action == "promoter_chat":
        if not setting_enabled("promoter_chat_enabled", True):
            await q.edit_message_text("Promoter support chat is temporarily unavailable.", reply_markup=kb([[( "⬅️ Promotion Center","ads")]])); return
        set_pending(uid, "promoter_user_chat", {})
        await q.edit_message_text("💬 CHAT WITH ADMIN\n\nSend any text, photo, or document here and it will be forwarded to the admin team. You can send multiple messages. Send /done when you want to leave chat mode.", reply_markup=kb([[( "✅ Finish chat","promoter_chat_done")],[( "⬅️ Promoter dashboard","promoter_stats")]]))
    elif action == "promoter_chat_done":
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='promoter_user_chat'", (uid,))
        await q.edit_message_text("Chat mode closed. You can contact the admin again anytime from your Promoter Dashboard.", reply_markup=kb([[( "💬 Chat with admin","promoter_chat")],[( "⬅️ Promoter dashboard","promoter_stats")]]))
    elif action == "promoter_withdraw_start":
        with db() as c:
            profile=c.execute("SELECT * FROM promoter_profiles WHERE user_id=?", (uid,)).fetchone()
            u=c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            joined=c.execute("SELECT COUNT(*) n FROM promoter_join_events WHERE promoter_user_id=?", (uid,)).fetchone()["n"]
        if not profile:
            await q.edit_message_text("Register for the promoter program first.", reply_markup=kb([[( "📢 Start promoter","promoter_start")]])); return
        points=int(u["points"] or 0) if u else 0
        target_required=setting_enabled("promoter_withdraw_require_target", True)
        target=int(profile["target_count"]); completed=int(profile["completed_count"])
        minimum=max(0,int(setting_value("promoter_withdraw_min_points","1000") or 0))
        locked=(target_required and completed<target) or points<max(1,minimum)
        status=(f"Verified referral joins: {completed}/{target}\nPoints available: {points}\nMinimum points to withdraw: {minimum if minimum else 'No minimum'}\nReferral target required: {'Yes' if target_required else 'No'}")
        await q.edit_message_text("💸 PROMOTER WITHDRAWAL\n\n"+status+"\n\nChoose or update your payout method. Save your details now; withdrawal will be available when you meet the admin's rules.", reply_markup=kb([[( "📱 Telebirr","promoter_withdraw_method_telebirr")],[( "🏦 CBE Birr","promoter_withdraw_method_cbe")],[( "💸 Request withdrawal now","withdraw") if not locked and profile["account_number"] and profile["account_name"] else ( "🔒 Not eligible yet","promoter_withdraw_start")],[( "⬅️ Promoter dashboard","promoter_stats")]]))
    elif action in ("promoter_withdraw_method_telebirr","promoter_withdraw_method_cbe"):
        method="Telebirr" if action.endswith("telebirr") else "CBE Birr"
        set_pending(uid,"promoter_withdraw_account_number",{"method":method})
        if method=="Telebirr":
            prompt="📱 Enter your Telebirr phone number. Use 09 followed by 8 digits (10 digits total), or +251 followed by 9 digits (12 characters total)."
        else:
            prompt="🏦 Enter your CBE account number using exactly 13 digits (numbers only)."
        await q.edit_message_text(prompt,reply_markup=kb([[( "❌ Cancel","promoter_withdraw_start")]]))
    elif action == "withdraw":
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            # One-time recovery for task rewards that were announced but not credited
            # because the wallet row did not exist when Telegram confirmed the join.
            # Never auto-recover if this user has any withdrawal history.
            if u and int(u["points"] or 0) == 0:
                prior_withdrawal = c.execute("SELECT 1 FROM withdrawals WHERE user_id=? LIMIT 1", (uid,)).fetchone()
                if not prior_withdrawal:
                    earned = c.execute(
                        "SELECT COALESCE(SUM(t.points),0) AS total FROM invite_events e "
                        "JOIN invite_links l ON l.invite_link=e.invite_link "
                        "JOIN tasks t ON t.id=l.task_id WHERE l.owner_user_id=?",
                        (uid,)
                    ).fetchone()["total"]
                    if int(earned or 0) > 0:
                        c.execute("UPDATE users SET points=points+? WHERE user_id=?", (int(earned), uid))
                        u = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
            promoter = c.execute("SELECT * FROM promoter_profiles WHERE user_id=?", (uid,)).fetchone()
        minimum_points = int(minimum["value"]) if minimum else 1000
        points = int(u["points"] or 0) if u else 0
        points_per_birr = max(1, int(setting_value("points_per_birr", "100") or 100))
        if promoter and setting_enabled("promoter_withdraw_require_target", True) and int(promoter["completed_count"]) < int(promoter["target_count"]):
            await q.edit_message_text(f"🔒 Promoter withdrawal is locked until you reach your target.\n\nVerified joins: {promoter['completed_count']}/{promoter['target_count']}\nPoints in wallet: {points}\n\nShare your unique referral link with real people, then check your progress again.", reply_markup=kb([[("🔗 My promoter progress","promoter_stats")],[("⬅️ Dashboard","home")]])); return
        if promoter and points < max(1, int(setting_value("promoter_withdraw_min_points", "1000") or 0)):
            await q.edit_message_text("You have no points available to withdraw yet.", reply_markup=kb([[("📊 My promoter progress","promoter_stats")],[("⬅️ Dashboard","home")]])); return
        if points < minimum_points and not promoter:
            await q.edit_message_text(f"💸 Withdrawal unavailable yet.\nYour points: {points}\nMinimum: {minimum_points} points.\nAdmins can change this limit.", reply_markup=kb([[("⬅️ Dashboard","home")]])); return
        if promoter:
            if not promoter["account_number"] or not promoter["account_name"]:
                await q.edit_message_text("Your payout details are incomplete. Please contact an admin.", reply_markup=kb([[("📊 My promoter progress","promoter_stats")]])); return
            payout_details = f"{promoter['account_number']} | {promoter['account_name']}"
            with db() as c:
                cur = c.execute("INSERT INTO withdrawals(user_id,points,payout_method,payout_details,created_at) VALUES(?,?,?,?,?)", (uid,points,promoter["method"],payout_details,now()))
                withdrawal_id = cur.lastrowid
                c.execute("UPDATE users SET points=0 WHERE user_id=?", (uid,))
            await q.edit_message_text(f"✅ Withdrawal request #{withdrawal_id} submitted!\nPoints requested: {points}\nMethod: {promoter['method']}\nAccount: {promoter['account_number']}\nName: {promoter['account_name']}\n\nAdmins will review and contact you after processing.", reply_markup=kb([[("📊 My promoter progress","promoter_stats")],[("⬅️ Dashboard","home")]]))
            await notify_admins(context, f"💸 PROMOTER WITHDRAWAL #{withdrawal_id}\nUser: {uid}\nPoints: {points}\nMethod: {promoter['method']}\nAccount: {promoter['account_number']}\nAccount holder: {promoter['account_name']}\nTarget: {promoter['completed_count']}/{promoter['target_count']}")
            return
        with db() as c:
            pending = c.execute("SELECT 1 FROM withdrawals WHERE user_id=? AND status='pending' LIMIT 1", (uid,)).fetchone()
            if pending:
                await q.edit_message_text("⏳ You already have a pending withdrawal request. Please wait for an admin to review it.", reply_markup=kb([[("👛 My Wallet","wallet")],[("⬅️ Dashboard","home")]]))
                return
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?, 'withdraw_choose_method','{}') ON CONFLICT(user_id) DO UPDATE SET action='withdraw_choose_method',data='{}'", (uid,))
        await q.edit_message_text(
            f"💸 WITHDRAW POINTS\n\nAvailable points: {points}\nEstimated value: {points / points_per_birr:.2f} ETB\nConversion: {points_per_birr} points = 1 ETB\nMinimum: {minimum_points} points\n\nChoose where you want to receive your payout:",
            reply_markup=kb([[("📱 Telebirr","withdraw_method_telebirr"),("🏦 CBE","withdraw_method_cbe")],[("❌ Cancel","home")]])
        )
    elif action == "ads":
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("📣 Promotion / Ads Center\n\nChoose what you want to do:", reply_markup=kb([
            [("📢 ቻናል አለኝ፣ ማስተዋወቅ እፈልጋለሁ (Promoter)","promoter_market_start")],
            [("🛍 ምርቴን ማስታወቅ እፈልጋለሁ (Advertiser)","advertiser_start")],
            [("📊 My promoter progress","promoter_stats")],
            [("📋 My ad requests","my_ads")],
            [("⬅️ Dashboard","home")]
        ]))
    elif action == "advertiser_start":
        if not setting_enabled("advertiser_active", True):
            await q.edit_message_text(str(setting_value("advertiser_unavailable_message", "Advertiser service is temporarily unavailable.")), reply_markup=kb([[( "📋 My ad requests","my_ads")],[( "⬅️ Promotion Center","ads")]])); return
        rules=str(setting_value("advertiser_rules","Please follow the advertising conditions."))
        channels=str(setting_value("advertiser_channels","Not configured yet"))
        await q.edit_message_text("🛍 ADVERTISER CENTER\n\n📡 Available channels\n"+channels+"\n\n📌 CONDITIONS\n"+rules+"\n\nTap Continue to choose your product type, duration, price and payment information.", reply_markup=kb([[( "➡️ Continue","advertiser_continue")],[( "📋 My ad requests","my_ads")],[( "⬅️ Promotion Center","ads")]]))
    elif action == "advertiser_continue":
        if not setting_enabled("advertiser_active", True):
            await q.edit_message_text(str(setting_value("advertiser_unavailable_message","Advertiser service is temporarily unavailable.")),reply_markup=kb([[( "⬅️ Promotion Center","ads")]])); return
        set_pending(uid,"advertiser_form",{"step":"product_type"})
        await q.edit_message_text("🛍 What type of product or service do you want to advertise?",reply_markup=kb([[( "📦 Physical product","advertiser_type_product"),( "📱 App / software","advertiser_type_app")],[( "🛠 Service","advertiser_type_service"),( "📣 Channel / content","advertiser_type_channel")],[( "✍️ Other","advertiser_type_other")],[( "❌ Cancel","ads")]]))
    elif action.startswith("advertiser_type_"):
        kinds={"product":"Physical product","app":"App / software","service":"Service","channel":"Channel / content","other":"Other"}
        kind=action.removeprefix("advertiser_type_")
        if kind not in kinds: await q.answer("Unknown product type.",show_alert=True); return
        set_pending(uid,"advertiser_form",{"step":"duration","product_type":kinds[kind]})
        labels=[]
        for period,label in [("day","Per day"),("week","Per week"),("month","Per month")]:
            price=str(setting_value("advertiser_price_"+period,"") or "").strip()
            labels.append((label+(" · "+price+" ETB" if price else " · price not set"),"advertiser_duration_"+period))
        await q.edit_message_text("Type: "+kinds[kind]+"\n\nChoose advertising duration. The exact price and payment details will appear next.",reply_markup=kb([[labels[0]],[labels[1]],[labels[2]],[( "⬅️ Back","advertiser_continue")],[( "❌ Cancel","ads")]]))
    elif action.startswith("advertiser_duration_"):
        period=action.removeprefix("advertiser_duration_")
        if period not in {"day","week","month"}: await q.answer("Unknown period.",show_alert=True); return
        with db() as c: pending=c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?",(uid,)).fetchone()
        if not pending or pending["action"]!="advertiser_form":
            await q.edit_message_text("Your advertiser form expired. Please start again.",reply_markup=kb([[( "🛍 Advertiser Center","advertiser_start")]])); return
        state=decode_pending(pending["data"])
        raw=str(setting_value("advertiser_price_"+period,"") or "").strip()
        if not raw:
            await q.edit_message_text("⚠️ Price for this period has not been configured. Choose another period or contact an admin.",reply_markup=kb([[( "💵 Per day","advertiser_duration_day")],[( "📅 Per week","advertiser_duration_week")],[( "🗓 Per month","advertiser_duration_month")],[( "⬅️ Back","advertiser_continue")]])); return
        try:
            price=float(raw.replace(",",""))
            if price<0 or not math.isfinite(price): raise ValueError()
        except ValueError:
            await q.edit_message_text("⚠️ The configured price is invalid. Please contact an admin.",reply_markup=kb([[( "⬅️ Back","advertiser_continue")]])); return
        state.update({"step":"content","duration":period,"price":price})
        set_pending(uid,"advertiser_content",state)
        payment=str(setting_value("advertiser_payment_info","Payment information has not been configured yet."))
        await q.edit_message_text("💰 ADVERTISING QUOTE\n\nProduct type: "+state.get("product_type","Other")+"\nDuration: "+{"day":"Per day","week":"Per week","month":"Per month"}[period]+"\nPrice: "+str(price)+" ETB\n\n🏦 PAYMENT INFORMATION\n"+payment+"\n\n📤 Now send your ad content as text, photo, video, PDF, APK, or another document. Admins will review it and reply here. Do not pay until you have checked the details.",reply_markup=kb([[( "❌ Cancel","ads")]]))
    elif action == "advertiser_admin":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        active=setting_enabled("advertiser_active",True)
        with db() as c: count=c.execute("SELECT COUNT(*) n FROM ad_requests WHERE status IN ('pending','quoted','receipt_submitted')").fetchone()["n"]
        await q.edit_message_text("🛠 ADVERTISER MANAGEMENT\n\nService: "+("🟢 ON" if active else "🔴 OFF")+"\nOpen requests: "+str(count)+"\n\nConfigure the public form, channels, pricing and payment instructions.",reply_markup=kb([[( "📥 Advertiser requests","advertiser_admin_requests")],[( "🔄 Turn service "+("OFF" if active else "ON"),"advertiser_admin_toggle")],[( "✏️ Edit conditions / rules","advertiser_edit_rules")],[( "📡 Edit available channels","advertiser_edit_channels")],[( "💵 Set per-day price","advertiser_edit_price_day")],[( "📅 Set per-week price","advertiser_edit_price_week")],[( "🗓 Set per-month price","advertiser_edit_price_month")],[( "🏦 Edit payment information","advertiser_edit_payment")],[( "🚫 Edit service-off message","advertiser_edit_unavailable")],[( "⬅️ Admin Dashboard","admin")]]))
    elif action == "advertiser_admin_toggle":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        new="0" if setting_enabled("advertiser_active",True) else "1"
        with db() as c: c.execute("INSERT INTO settings(key,value) VALUES('advertiser_active',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(new,))
        await q.edit_message_text("🔴 Advertiser service is OFF." if new=="0" else "🟢 Advertiser service is ON.",reply_markup=kb([[( "🛠 Advertiser management","advertiser_admin")]]))
    elif action.startswith("advertiser_edit_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        field=action.removeprefix("advertiser_edit_")
        allowed={"rules":"advertiser_rules","channels":"advertiser_channels","payment":"advertiser_payment_info","unavailable":"advertiser_unavailable_message","price_day":"advertiser_price_day","price_week":"advertiser_price_week","price_month":"advertiser_price_month"}
        key=allowed.get(field)
        if not key: await q.edit_message_text("Unknown setting."); return
        set_pending(uid,"advertiser_admin_setting",{"field":field,"key":key})
        await q.edit_message_text("Send the new "+field.replace("_"," ")+" value. Send /cancel to stop.\n\nCurrent value:\n"+str(setting_value(key,"") or "(not set)")[:1600],reply_markup=kb([[( "❌ Cancel","advertiser_admin")]]))
    elif action == "advertiser_admin_requests":
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        with db() as c: rows=c.execute("SELECT id,user_id,kind,status FROM ad_requests ORDER BY id DESC LIMIT 20").fetchall()
        buttons=[[(f"#{r['id']} · user {r['user_id']} · {r['status']}",f"advertiser_request_{r['id']}")] for r in rows]
        buttons.extend([[( "🔄 Refresh","advertiser_admin_requests")],[( "⬅️ Advertiser management","advertiser_admin")],[( "⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text("📥 ADVERTISER REQUESTS\nChoose a request to inspect and reply.",reply_markup=kb(buttons))
    elif action.startswith("advertiser_request_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        request_id=action.removeprefix("advertiser_request_")
        with db() as c: row=c.execute("SELECT * FROM ad_requests WHERE id=?",(request_id,)).fetchone()
        if not row: await q.edit_message_text("Request not found."); return
        try: details=json.loads(row["details"] or "{}")
        except Exception: details={"content_text":row["details"]}
        body=f"📣 ADVERTISER REQUEST #{request_id}\nUser: {row['user_id']}\nType: {details.get('product_type',row['kind'])}\nDuration: {details.get('duration',row['duration'])}\nPrice: {row['quoted_price'] if row['quoted_price'] is not None else details.get('price','not set')} ETB\nStatus: {row['status']}\nContent: {details.get('content_text','(media attached)')}\nCreated: {row['created_at']}"
        await q.edit_message_text(body[:3900],reply_markup=kb([[( "💬 Reply to advertiser","advertiser_admin_reply_"+str(request_id))],[( "✅ Mark approved","advertiser_status_approved_"+str(request_id)),( "❌ Reject","advertiser_status_rejected_"+str(request_id))],[( "📥 All advertiser requests","advertiser_admin_requests")],[( "🛠 Advertiser settings","advertiser_admin")],[( "⬅️ Admin Dashboard","admin")]]))
    elif action.startswith("advertiser_admin_reply_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        request_id=action.removeprefix("advertiser_admin_reply_")
        with db() as c: row=c.execute("SELECT user_id FROM ad_requests WHERE id=?",(request_id,)).fetchone()
        if not row: await q.edit_message_text("Request not found."); return
        set_pending(uid,"admin_ad_message",{"request_id":int(request_id),"target_user_id":int(row["user_id"])})
        await q.edit_message_text("Send your reply as text, photo, video, PDF, APK or another document.",reply_markup=kb([[( "❌ Cancel","advertiser_request_"+request_id)]]))
    elif action.startswith("advertiser_status_"):
        if not is_admin(uid): await q.edit_message_text("⛔ Admin access only."); return
        parts=action.split("_"); status=parts[2]; request_id=parts[3]
        if status not in {"approved","rejected"}: await q.edit_message_text("Invalid status."); return
        with db() as c:
            row=c.execute("SELECT user_id FROM ad_requests WHERE id=?",(request_id,)).fetchone()
            if row: c.execute("UPDATE ad_requests SET status=? WHERE id=?",(status,request_id))
        if row:
            try: await context.bot.send_message(row["user_id"],f"📣 Your advertiser request #{request_id} was {status}. You can continue messaging the admin from My ad requests.")
            except Exception: log.warning("Could not notify advertiser request user %s",request_id)
        await q.edit_message_text(f"Request #{request_id} marked {status}.",reply_markup=kb([[( "📥 Advertiser requests","advertiser_admin_requests")],[( "⬅️ Advertiser management","advertiser_admin")]]))
    elif action.startswith("advertiser_chat_"):
        request_id=action.removeprefix("advertiser_chat_")
        with db() as c: row=c.execute("SELECT id FROM ad_requests WHERE id=? AND user_id=?",(request_id,uid)).fetchone()
        if not row: await q.edit_message_text("Request not found for your account."); return
        set_pending(uid,"advertiser_user_message",{"request_id":int(request_id)})
        await q.edit_message_text("💬 Send your message about this advertiser request. Text, photo, video, PDF, APK and other documents are supported.",reply_markup=kb([[( "❌ Cancel","my_ads")]]))
    elif action == "promoter_market_start":
        if not setting_enabled("promoter_ads_active"):
            await q.edit_message_text("📣 Promoter submissions are temporarily unavailable. Please check back later.", reply_markup=kb([[("⬅️ Promotion Center","ads")]])); return
        try: submission_limit = max(0, int(setting_value("promoter_ads_submission_limit", "0") or 0))
        except (TypeError, ValueError): submission_limit = 0
        if submission_limit:
            with db() as c:
                current_submissions = c.execute("SELECT COUNT(*) n FROM promoter_ad_submissions WHERE status IN ('pending','approved')").fetchone()["n"]
            if int(current_submissions) >= submission_limit:
                await q.edit_message_text("📣 The promoter application limit has been reached for now. Please check back later.", reply_markup=kb([[("⬅️ Promotion Center","ads")]])); return
        set_pending(uid, "promoter_market_form", {"step":"platforms","platforms":[],"prices":{}})
        await q.edit_message_text(
            str(setting_value("promoter_question_platforms", "Which social media platforms do you have? Select at least one.")) +
            "\n\nChoose one or more platforms, then tap Continue.",
            reply_markup=kb([
                [("Telegram","promoter_market_platform_telegram"),("TikTok","promoter_market_platform_tiktok")],
                [("YouTube","promoter_market_platform_youtube"),("Instagram","promoter_market_platform_instagram")],
                [("Facebook","promoter_market_platform_facebook"),("Other","promoter_market_platform_other")],
                [("✅ Continue","promoter_market_platform_continue")],
                [("❌ Cancel","ads")]
            ])
        )
    elif action.startswith("promoter_market_platform_"):
        if not setting_enabled("promoter_ads_active"):
            await q.edit_message_text("📣 Promoter submissions are temporarily unavailable.", reply_markup=kb([[("⬅️ Promotion Center","ads")]])); return
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
        if not pending or pending["action"] != "promoter_market_form":
            await q.edit_message_text("Your promoter form expired. Please start again.", reply_markup=kb([[("📢 Start promoter form","promoter_market_start")]])); return
        state = decode_pending(pending["data"])
        platform = action.removeprefix("promoter_market_platform_")
        if platform == "continue":
            selected = state.get("platforms", [])
            if not selected:
                await q.answer("Choose at least one platform before continuing.", show_alert=True); return
            state["step"] = "content"
            set_pending(uid, "promoter_market_form", state)
            await q.edit_message_text(str(setting_value("promoter_question_content", "What type of content do you publish on your channel?")), reply_markup=kb([[("❌ Cancel","ads")]]))
            return
        platform_names = {"telegram":"Telegram","tiktok":"TikTok","youtube":"YouTube","instagram":"Instagram","facebook":"Facebook","other":"Other"}
        if platform not in platform_names:
            await q.answer("Unknown platform."); return
        selected = list(state.get("platforms", []))
        if platform in selected: selected.remove(platform)
        else: selected.append(platform)
        state["platforms"] = selected
        set_pending(uid, "promoter_market_form", state)
        rows = [
            [(("✅ " if p in selected else "") + name, "promoter_market_platform_" + p) for p,name in [("telegram","Telegram"),("tiktok","TikTok")]],
            [(("✅ " if p in selected else "") + name, "promoter_market_platform_" + p) for p,name in [("youtube","YouTube"),("instagram","Instagram")]],
            [(("✅ " if p in selected else "") + name, "promoter_market_platform_" + p) for p,name in [("facebook","Facebook"),("other","Other")]],
            [("✅ Continue","promoter_market_platform_continue")],
            [("❌ Cancel","ads")]
        ]
        await q.edit_message_text(
            str(setting_value("promoter_question_platforms", "Which social media platforms do you have? Select at least one.")) +
            "\n\nSelected: " + (", ".join(platform_names[p] for p in selected) if selected else "None yet") +
            "\nChoose one or more, then tap Continue.",
            reply_markup=kb(rows)
        )
    elif action.startswith("promoter_market_price_"):
        if not setting_enabled("promoter_ads_active"):
            await q.edit_message_text("📣 Promoter submissions are temporarily unavailable.", reply_markup=kb([[("⬅️ Promotion Center","ads")]])); return
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
        if not pending or pending["action"] != "promoter_market_form":
            await q.edit_message_text("Your promoter form expired. Please start again.", reply_markup=kb([[("📢 Start promoter form","promoter_market_start")]])); return
        state = decode_pending(pending["data"])
        period = action.removeprefix("promoter_market_price_")
        if period == "done":
            # Validate every required field before creating a review request.
            platforms = state.get("platforms") or []
            content_type = str(state.get("content_type") or "").strip()
            channel_link = str(state.get("channel_link") or "").strip()
            prices = state.get("prices") or {}
            try:
                followers = int(state.get("followers", -1))
                valid_prices = {
                    key: float(value) for key, value in prices.items()
                    if key in {"day", "week", "month"} and math.isfinite(float(value)) and float(value) > 0
                }
            except (TypeError, ValueError):
                followers, valid_prices = -1, {}
            if not platforms or not content_type or not channel_link or followers < 0 or not valid_prices:
                await q.answer("Your application is incomplete. Please finish each step and set at least one valid price.", show_alert=True)
                await q.edit_message_text(
                    "⚠️ Your promoter application is incomplete. Please review the form and submit again.",
                    reply_markup=kb([[("📢 Restart promoter form","promoter_market_start")], [("⬅️ Promotion Center","ads")]])
                )
                return
            try:
                with db() as c:
                    # Avoid creating duplicate pending applications when a user taps twice.
                    existing = c.execute(
                        "SELECT id FROM promoter_ad_submissions WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
                        (uid,)
                    ).fetchone()
                    if existing:
                        submission_id = int(existing["id"])
                        c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
                    else:
                        cur = c.execute(
                            "INSERT INTO promoter_ad_submissions(user_id,platforms,content_type,followers,channel_link,price_day,price_week,price_month,status,created_at,updated_at) "
                            "VALUES(?,?,?,?,?,?,?,?, 'pending',?,?)",
                            (uid, ", ".join(str(p) for p in platforms), content_type, followers, channel_link,
                             valid_prices.get("day"), valid_prices.get("week"), valid_prices.get("month"), now(), now())
                        )
                        submission_id = cur.lastrowid
                        c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
            except Exception:
                log.exception("Failed to save promoter application for user %s", uid)
                await q.edit_message_text(
                    "⚠️ We couldn't submit your application because of a temporary error. Your form may still be available; please tap Submit for admin review again in a moment.",
                    reply_markup=kb([[("🔄 Try submit again","promoter_market_price_done")], [("⬅️ Promotion Center","ads")]])
                )
                return
            await q.edit_message_text(
                f"✅ Your promoter application #{submission_id} has been submitted for admin review.\n\nAdmins will review your platforms, channel details, follower count, and prices. You can check the Promotion Center later.",
                reply_markup=kb([[("📣 Promotion Center","ads")], [("🏠 Dashboard","home")]])
            )
            try:
                await notify_admins(
                    context,
                    f"📣 NEW PROMOTER SUBMISSION #{submission_id}\nUser: {uid}\nPlatforms: {', '.join(str(p) for p in platforms)}\nContent: {content_type}\nFollowers: {followers}\nLink: {channel_link}\nPrices: day={valid_prices.get('day','—')} ETB, week={valid_prices.get('week','—')} ETB, month={valid_prices.get('month','—')} ETB\nReview in Admin Dashboard → Promoter submissions."
                )
            except Exception:
                log.exception("Promoter application %s saved but admin notification failed", submission_id)
            return
        if period not in {"day","week","month"}:
            await q.answer("Unknown price period.", show_alert=True); return
        if period in state.get("prices", {}):
            await q.answer("You already set this period's price. Choose another period or submit.", show_alert=True); return
        state["price_period"] = period
        state["step"] = "price_amount"
        set_pending(uid, "promoter_market_form", state)
        await q.edit_message_text(f"Enter your advertising price in ETB per {period}. Use a number greater than 0.", reply_markup=kb([[("❌ Cancel","ads")]]))
    elif action == "promoter_market_add_price":
        with db() as c:
            pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (uid,)).fetchone()
        if not pending or pending["action"] != "promoter_market_form":
            await q.edit_message_text("Your promoter form expired.", reply_markup=kb([[("📢 Start promoter form","promoter_market_start")]])); return
        state = decode_pending(pending["data"])
        state["step"] = "choose_price"
        set_pending(uid, "promoter_market_form", state)
        prices = state.get("prices", {})
        rows = []
        for p,label in [("day","Per day"),("week","Per week"),("month","Per month")]:
            if p not in prices: rows.append([(label, "promoter_market_price_"+p)])
        rows += [[("✅ Submit for admin review","promoter_market_price_done")],[("❌ Cancel","ads")]]
        await q.edit_message_text("Set prices for any period you want. At least one price is required. You may set one, two, or all three.", reply_markup=kb(rows))
    elif action == "promoter_start":
        with db() as c:
            existing_profile = c.execute("SELECT user_id FROM promoter_profiles WHERE user_id=?", (uid,)).fetchone()
        if existing_profile:
            await q.edit_message_text("You already have a promoter profile. Open your progress dashboard to view your saved payout details and referral link.", reply_markup=kb([[("📊 My promoter progress","promoter_stats")],[("⬅️ Promotion Center","ads")]]))
            return
        channel = setting_value("promoter_channel", "").strip()
        try:
            target = max(1, int(setting_value("promoter_target", "100")))
            points = max(1, int(setting_value("promoter_points_per_join", "1")))
        except (TypeError, ValueError):
            target, points = 100, 1
        if not channel:
            await q.edit_message_text("⏳ The promoter campaign is not configured yet. Please check back later.", reply_markup=kb([[("⬅️ Promotion Center","ads")]]))
            return
        rules = setting_value("promoter_rules", "Please follow the campaign rules.")
        await q.edit_message_text(
            f"{rules}\n\n📌 Current campaign target: {target} verified joins\n🎁 Reward: {points} points per verified join\n\nDo you agree to these rules and want to continue?",
            reply_markup=kb([[("✅ Agree & Confirm","promoter_agree")],[("❌ I don't agree","ads")]])
        )
    elif action == "promoter_agree":
        channel = setting_value("promoter_channel", "").strip()
        if not channel:
            await q.edit_message_text("The promoter campaign is temporarily unavailable. Please try again later.", reply_markup=kb([[("⬅️ Promotion Center","ads")]]))
            return
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,'promoter_choose_method','{}') ON CONFLICT(user_id) DO UPDATE SET action='promoter_choose_method',data='{}'", (uid,))
        await q.edit_message_text("✅ Rules accepted.\n\nChoose where you want to receive your promoter payout:", reply_markup=kb([[("📱 Telebirr","promoter_method_telebirr")],[("🏦 CBE","promoter_method_cbe")],[("❌ Cancel","ads")]]))
    elif action in ("promoter_method_telebirr","promoter_method_cbe"):
        method = "Telebirr" if action.endswith("telebirr") else "CBE"
        set_pending(uid, "promoter_account_number", {"method": method})
        await q.edit_message_text(f"💳 Payout method: {method}\n\nSend your {method} account/phone number. This is saved privately for admin payout processing.", reply_markup=kb([[("❌ Cancel","ads")]]))
    elif action == "promoter_stats":
        with db() as c:
            profile = c.execute("SELECT * FROM promoter_profiles WHERE user_id=?", (uid,)).fetchone()
            joined = c.execute("SELECT COUNT(*) n FROM promoter_join_events WHERE promoter_user_id=?", (uid,)).fetchone()["n"]
            wallet_row = c.execute("SELECT points FROM users WHERE user_id=?", (uid,)).fetchone()
            wallet_points = wallet_row["points"] if wallet_row else 0
        if not profile:
            await q.edit_message_text("📊 You haven't registered for the promoter program yet. Tap Start promoter to read the rules and register.", reply_markup=kb([[("📢 Start promoter","promoter_start")],[("⬅️ Promotion Center","ads")]]))
            return
        target = int(profile["target_count"])
        completed = int(profile["completed_count"])
        points_earned = joined * int(profile["points_per_join"])
        msg = (f"📊 YOUR PROMOTER DASHBOARD\n\nStatus: {profile['status'].replace('_',' ').title()}\n"
               f"Verified joins: {completed}/{target}\nPoints earned from tracked joins: {points_earned}\n"
               f"Wallet balance: {wallet_points} points\nRemaining to target: {max(0,target-completed)}\nPayout method: {profile['method']}\n"
               f"Account number: {profile['account_number']}\nAccount holder: {profile['account_name']}\n")
        if profile["invite_link"]:
            msg += f"\n🔗 Your unique channel referral link:\n{profile['invite_link']}\n\nShare this link with real people. Only verified unique joins count."
        if completed >= target:
            msg += "\n\n🎉 Target reached! you can request withdrawal of your available points."
        await q.edit_message_text(msg, reply_markup=kb([[("🔄 Refresh progress","promoter_stats")],[("📊 Statistics","promoter_stats")],[("💬 Chat with admin","promoter_chat")],[("💸 Withdraw","promoter_withdraw_start")],[("⬅️ Promotion Center","ads")]]))
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
        buttons = [[("💬 Message admin · request #" + str(r["id"]), "advertiser_chat_" + str(r["id"]))] for r in rows]
        buttons.extend([[("🛍 New advertisement","advertiser_start")],[("⬅️ Promotions","ads")],[("⬅️ Dashboard","home")]])
        await q.edit_message_text(msg, reply_markup=kb(buttons))
    elif action == "market":
        rows = [
            [(setting_value("buy_usdt_menu_button", "💵 Buy USDT | USDT ይግዙ"),"buy_usdt")],
            [("📱 Buy social media accounts","buy_accounts")],
            [("🧾 My account purchases","account_my_purchases")],
            [("💸 Sell USDT | USDT ይሽጡ","sell_usdt")],
            [("📤 Sell a social media account","sell_social")],
        ]
        for product in digital_products():
            label = render_digital_template(
                digital_template("digital_market_product_button_template", "🌟 {name} · {duration} months · {price} ETB"),
                {"name": product["name"], "duration": product["duration_months"], "price": f"{product['price']:g}", "stock": product["stock"]}
            )
            rows.append([(label[:60], f"digital_product_{product['id']}")])
        rows.append([("⬅️ Dashboard","home")])
        await q.edit_message_text(digital_template("digital_market_title", "🛍 Marketplace — choose what you want to do:"), reply_markup=kb(rows))
    elif action == "buy_accounts":
        await q.edit_message_text("📱 BUY SOCIAL MEDIA ACCOUNTS\n\nChoose the platform you are interested in:", reply_markup=kb([
            [("🎵 TikTok","buy_accounts_tiktok")],
            [("✈️ Telegram","buy_accounts_telegram")],
            [("▶️ YouTube","buy_accounts_youtube")],
            [("📸 Instagram","buy_accounts_instagram")],
            [("📘 Facebook","buy_accounts_facebook")],
            [("⬅️ Marketplace","market")]
        ]))
    elif action.startswith("buy_accounts_") and action != "buy_accounts":
        platform_key = action.removeprefix("buy_accounts_")
        platforms = {"tiktok":"TikTok","telegram":"Telegram","youtube":"YouTube","instagram":"Instagram","facebook":"Facebook"}
        platform = platforms.get(platform_key)
        if not platform:
            await q.edit_message_text("Unknown platform.", reply_markup=kb([[("⬅️ Marketplace","market")]])); return
        with db() as c:
            listings = c.execute(
                "SELECT id,short_description,details,price FROM market_listings "
                "WHERE action='sell_social' AND status='approved' AND listing_active=1 "
                "AND purchase_status='available' AND lower(asset_type)=lower(?) ORDER BY id DESC LIMIT 20",
                (platform,)
            ).fetchall()
        rows = []
        for item in listings:
            description = (item["short_description"] or item["details"] or "Account details available on request").replace("\n"," ").strip()
            if len(description) > 42: description = description[:39] + "..."
            price_label = f"{item['price']:g} ETB" if item["price"] is not None else "Price pending"
            rows.append([(f"#{item['id']} · {description} · {price_label}"[:62], f"account_view_{item['id']}")])
        rows.extend([[("⬅️ Choose platform","buy_accounts")],[("⬅️ Marketplace","market")]])
        await q.edit_message_text(f"📱 {platform} accounts available\n\nChoose an account to view its short description and price.", reply_markup=kb(rows))
    elif action == "account_my_purchases":
        with db() as c:
            orders = c.execute("SELECT id,asset_type,price,purchase_status,payment_method FROM market_listings WHERE buyer_user_id=? AND action='sell_social' ORDER BY id DESC LIMIT 20",(uid,)).fetchall()
        rows = [[(f"#{o['id']} · {o['asset_type']} · {o['purchase_status'].replace('_',' ').title()}",f"account_purchase_{o['id']}")] for o in orders]
        rows.extend([[("📱 Browse accounts","buy_accounts")],[("⬅️ Marketplace","market")]])
        await q.edit_message_text("🧾 MY ACCOUNT PURCHASES\n\nOpen an order to see its current status and messages from the admin.",reply_markup=kb(rows))
    elif action.startswith("account_purchase_"):
        try: listing_id=int(action.removeprefix("account_purchase_"))
        except ValueError:
            await q.edit_message_text("Invalid purchase."); return
        with db() as c:
            item=c.execute("SELECT id,asset_type,price,purchase_status,payment_method,buyer_user_id FROM market_listings WHERE id=? AND buyer_user_id=? AND action='sell_social'",(listing_id,uid)).fetchone()
            messages=c.execute("SELECT sender_role,message_type,message_text,file_id,created_at FROM marketplace_messages WHERE listing_id=? AND user_id=? ORDER BY id DESC LIMIT 15",(listing_id,uid)).fetchall()
        if not item:
            await q.edit_message_text("Purchase not found.",reply_markup=kb([[("🧾 My purchases","account_my_purchases")]])); return
        body=f"🧾 ACCOUNT ORDER #{listing_id}\nPlatform: {item['asset_type']}\nPrice: {item['price'] if item['price'] is not None else '—'} ETB\nPayment: {item['payment_method'] or '—'}\nStatus: {item['purchase_status'].replace('_',' ').title()}\n\n💬 RECENT MESSAGES\n"
        if messages:
            for m in reversed(messages):
                label="Admin" if m["sender_role"]=="admin" else "You"
                body+=f"\n{label} · {m['created_at']}\n{m['message_text'] or ('['+m['message_type']+' attachment]' if m['file_id'] else '')}"
        else: body+="\nNo messages yet."
        await q.edit_message_text(body[:3900], reply_markup=kb([[("💬 Message admin", "account_chat_" + str(listing_id))], [("🔄 Refresh", f"account_purchase_{listing_id}")], [("⬅️ My purchases", "account_my_purchases")]]))
    elif action.startswith("account_chat_"):
        try: listing_id=int(action.removeprefix("account_chat_"))
        except ValueError:
            await q.edit_message_text("Invalid purchase."); return
        with db() as c:
            item=c.execute("SELECT id FROM market_listings WHERE id=? AND buyer_user_id=? AND action='sell_social'",(listing_id,uid)).fetchone()
        if not item:
            await q.edit_message_text("Purchase not found."); return
        set_pending(uid,"account_buyer_message",{"listing_id":listing_id})
        await q.edit_message_text("💬 Send your message to the admin as text, a photo, or a document. Your message will be saved in this order's conversation.",reply_markup=kb([[("❌ Cancel",f"account_purchase_{listing_id}")]]))
    elif action.startswith("account_view_"):
        try: listing_id = int(action.removeprefix("account_view_"))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND action='sell_social' AND status='approved' AND listing_active=1 AND purchase_status='available'", (listing_id,)).fetchone()
        if not item:
            await q.edit_message_text("This account listing is no longer available.", reply_markup=kb([[("📱 Browse accounts","buy_accounts")],[("⬅️ Marketplace","market")]])); return
        description = (item["short_description"] or item["details"] or "Ask the admin for details.").strip()
        price = f"{item['price']:g} ETB" if item["price"] is not None else "Price not set yet"
        await q.edit_message_text(
            f"📱 {item['asset_type']} account · #{item['id']}\n\n{description}\n\n💰 Price: {price}\n\n🔒 Never share account passwords or one-time verification codes in this chat. Confirm ownership and transfer terms with an admin.",
            reply_markup=kb([[("🛒 Buy now",f"account_buy_{listing_id}")],[("🧾 My purchases","account_my_purchases")],[("⬅️ Back to listings",f"buy_accounts_{str(item['asset_type']).lower()}")],[("⬅️ Marketplace","market")]])
        )
    elif action.startswith("account_buy_"):
        try: listing_id = int(action.removeprefix("account_buy_"))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND action='sell_social' AND status='approved' AND listing_active=1 AND purchase_status='available'", (listing_id,)).fetchone()
            gateways = c.execute("SELECT id,name FROM digital_payment_gateways WHERE enabled=1 ORDER BY name").fetchall()
        if not item:
            await q.edit_message_text("Sorry, this account is no longer available.", reply_markup=kb([[("📱 Browse accounts","buy_accounts")],[("⬅️ Marketplace","market")]])); return
        if item["price"] is None or float(item["price"]) <= 0:
            await q.edit_message_text("The admin has not set a valid price for this account yet. Please check back later.", reply_markup=kb([[("⬅️ Back to listing",f"account_view_{listing_id}")]])); return
        if not gateways:
            await q.edit_message_text("Payment is temporarily unavailable. Please contact an admin.", reply_markup=kb([[("⬅️ Back to listing",f"account_view_{listing_id}")]])); return
        rows = [[(f"{g['name']}",f"account_pay_{listing_id}_{g['id']}")] for g in gateways]
        rows.append([("❌ Cancel",f"account_view_{listing_id}")])
        await q.edit_message_text(f"🛒 Buy {item['asset_type']} account #{listing_id}\nPrice: {float(item['price']):g} ETB\n\nChoose your payment method:", reply_markup=kb(rows))
    elif action.startswith("account_pay_"):
        parts = action.split("_",3)
        if len(parts) != 4 or not parts[2].isdigit():
            await q.edit_message_text("Invalid payment option."); return
        listing_id, gateway_id = int(parts[2]), parts[3]
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND action='sell_social' AND status='approved' AND listing_active=1 AND purchase_status='available'", (listing_id,)).fetchone()
            gateway = c.execute("SELECT * FROM digital_payment_gateways WHERE id=? AND enabled=1", (gateway_id,)).fetchone()
            if item and gateway and item["price"] is not None and float(item["price"]) > 0:
                cur = c.execute("UPDATE market_listings SET purchase_status='awaiting_payment',buyer_user_id=?,payment_method=? WHERE id=? AND purchase_status='available'", (uid,gateway["name"],listing_id))
                reserved = cur.rowcount == 1
            else: reserved = False
        if not reserved:
            await q.edit_message_text("This listing or payment method is no longer available. Please choose another listing.", reply_markup=kb([[("📱 Browse accounts","buy_accounts")],[("⬅️ Marketplace","market")]])); return
        set_pending(uid,"account_buy_receipt",{"listing_id":listing_id,"gateway_id":gateway_id,"gateway_name":gateway["name"]})
        await q.edit_message_text(
            f"💳 PAYMENT DETAILS · ACCOUNT #{listing_id}\n\nAmount: {float(item['price']):g} ETB\nMethod: {gateway['name']}\nAccount/Number: {gateway['account_number']}\nAccount holder: {gateway['account_name']}\n\n{gateway['instructions']}\n\nAfter paying, upload a clear payment screenshot as a photo or document. An admin will verify it manually before processing the order.\n{gateway['warning']}",
            reply_markup=kb([[("❌ Cancel purchase",f"account_cancel_{listing_id}")]])
        )
    elif action.startswith("account_cancel_"):
        try: listing_id = int(action.removeprefix("account_cancel_"))
        except ValueError: listing_id = 0
        with db() as c:
            c.execute("UPDATE market_listings SET purchase_status='available',buyer_user_id=NULL,payment_method='' WHERE id=? AND buyer_user_id=? AND purchase_status='awaiting_payment'", (listing_id,uid))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("Purchase cancelled. You can browse the listings again.", reply_markup=kb([[("📱 Browse accounts","buy_accounts")],[("⬅️ Marketplace","market")]]))
    elif action in ("buy_social","sell_social"):
        if action == "buy_social":
            await q.edit_message_text("📣 SOCIAL MEDIA PROMOTION\n\nChoose a promotion service:", reply_markup=kb([
                [("📣 Promote a channel / page","ad_product")],
                [("👥 Get channel members","ad_members")],
                [("👁️ Get views / reach","ad_views")],
                [("⬅️ Marketplace","market")]
            ]))
        else:
            await q.edit_message_text("📤 SELL A SOCIAL MEDIA ACCOUNT\n\nChoose the platform, then send a short description, any public profile link, and your expected price. Do not send passwords or verification codes.", reply_markup=kb([
                [("🎵 TikTok","sell_account_tiktok")],
                [("✈️ Telegram","sell_account_telegram")],
                [("▶️ YouTube","sell_account_youtube")],
                [("📸 Instagram","sell_account_instagram")],
                [("📘 Facebook","sell_account_facebook")],
                [("📋 My listings","my_market")],
                [("⬅️ Marketplace","market")]
            ]))
    elif action.startswith("sell_account_"):
        platform_key = action.removeprefix("sell_account_")
        platforms = {"tiktok":"TikTok","telegram":"Telegram","youtube":"YouTube","instagram":"Instagram","facebook":"Facebook"}
        platform = platforms.get(platform_key)
        if not platform:
            await q.edit_message_text("Unknown platform.", reply_markup=kb([[("⬅️ Marketplace","market")]])); return
        set_pending(uid,"sell_social",{"platform":platform})
        await q.edit_message_text(f"📤 List your {platform} account\n\nSend a short description, public profile link (if available), and your expected price in ETB. Never send passwords, one-time codes, or recovery details.", reply_markup=kb([[("❌ Cancel","market")]]))
    elif action == "my_market":
        with db() as c:
            rows = c.execute("SELECT id,action,asset_type,status,price FROM market_listings WHERE user_id=? ORDER BY id DESC LIMIT 10", (uid,)).fetchall()
        msg = "📋 YOUR LISTINGS\n\n" + ("\n".join(f"#{r['id']} · {r['asset_type']} · {r['status']} · {r['price'] if r['price'] is not None else 'price pending'} ETB" for r in rows) if rows else "You haven't submitted any listings yet.")
        await q.edit_message_text(msg, reply_markup=kb([[("📤 Sell an account","sell_social")],[("⬅️ Marketplace","market")]]))
    elif action == "admin_tasks":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            tasks = c.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT 25").fetchall()
            claim_counts = {r["task_id"]: r["n"] for r in c.execute("SELECT task_id,COUNT(*) n FROM task_claims GROUP BY task_id").fetchall()}
        rows = []
        for task in tasks:
            assigned = int(claim_counts.get(task["id"], 0))
            cap = "∞" if not int(task["participant_limit"] or 0) else str(task["participant_limit"])
            state = "🟢" if task["active"] and task["completed_count"] < task["target"] else "⏸"
            tid = int(task["id"])
            # Keep the task title as the full control panel, and expose the most-used
            # actions directly on the Manage Daily Tasks screen as well.
            rows.append([(f"{state} #{tid} {task['title']} · {assigned}/{cap}", f"admintask_view_{tid}")])
            rows.append([
                ("✏️ Edit details", f"admintask_edit_title_{tid}"),
                ("📝 User message", f"admintask_message_edit_{tid}"),
                ("🗑 Delete", f"admintask_delete_confirm_{tid}")
            ])
        rows += [
            [("➕ Create new task","admin_new_task")],
            [("🏆 Overall leaderboard","admin_task_leaderboard")],
            [("⬅️ Admin Dashboard","admin")]
        ]
        await q.edit_message_text(
            "📋 MANAGE DAILY TASKS\n\n"
            "Choose a task to open its control panel. From there you can update its details, "
            "change the message users see, pause/resume it, review participants, or delete it.\n\n"
            "🟢 Active · ⏸ Paused or target reached\n"
            "The personal invite link is generated separately for each participant and cannot be edited.",
            reply_markup=kb(rows)
        )
    elif action == "admin_task_leaderboard":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            rows = c.execute(
                "SELECT u.user_id,u.username,u.first_name,COUNT(e.id) joins_count,"
                "COALESCE(SUM(t.points),0) earned "
                "FROM invite_events e JOIN invite_links l ON l.invite_link=e.invite_link "
                "JOIN tasks t ON t.id=l.task_id JOIN users u ON u.user_id=l.owner_user_id "
                "GROUP BY u.user_id ORDER BY joins_count DESC,earned DESC LIMIT 15"
            ).fetchall()
        if rows:
            lines = ["🏆 TASK LEADERBOARD", "Verified task joins · Top 15", ""]
            for i, row in enumerate(rows, 1):
                label = ("@" + row["username"]) if row["username"] else (row["first_name"] or str(row["user_id"]))
                lines.append(f"{i}. {label[:30]} — {row['joins_count']} joins · {row['earned']} points")
            body = "\n".join(lines)
        else:
            body = "🏆 TASK LEADERBOARD\n\nNo verified task joins have been recorded yet."
        await q.edit_message_text(body, reply_markup=kb([[("📋 Manage tasks","admin_tasks")],[("⬅️ Admin Dashboard","admin")]]))
    elif action.startswith("admintask_view_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            tid = int(action.rsplit("_",1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
            assigned = c.execute("SELECT COUNT(*) n FROM task_claims WHERE task_id=?", (tid,)).fetchone()["n"]
            joins = c.execute("SELECT COUNT(*) n FROM invite_events e JOIN invite_links l ON l.invite_link=e.invite_link WHERE l.task_id=?", (tid,)).fetchone()["n"]
            participants = c.execute(
                "SELECT u.user_id,u.username,u.first_name,tc.status,COUNT(e.id) joins_count "
                "FROM task_claims tc JOIN users u ON u.user_id=tc.user_id "
                "LEFT JOIN invite_links l ON l.task_id=tc.task_id AND l.owner_user_id=tc.user_id "
                "LEFT JOIN invite_events e ON e.invite_link=l.invite_link "
                "WHERE tc.task_id=? GROUP BY u.user_id ORDER BY joins_count DESC LIMIT 10", (tid,)
            ).fetchall()
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        cap = "Unlimited" if not int(task["participant_limit"] or 0) else str(task["participant_limit"])
        state = "Active" if task["active"] else "Paused/closed"
        body = (f"📋 TASK #{tid} · {task['title']}\n\nStatus: {state}\nChannel: {task['channel']}\n"
                f"Reward per verified join: {task['points']} points\nCampaign: {task['completed_count']}/{task['target']} verified joins\n"
                f"Assigned users: {assigned}/{cap}\nTotal verified joins: {joins}\n\n👥 TOP PARTICIPANTS")
        if participants:
            for i, p in enumerate(participants, 1):
                who = ("@" + p["username"]) if p["username"] else (p["first_name"] or str(p["user_id"]))
                body += f"\n{i}. {who[:25]} — {p['joins_count']} joins · {p['status']}"
        else:
            body += "\nNo users have claimed this task yet."
        rows = [
            [("✏️ Edit title","admintask_edit_title_" + str(tid)),("📣 Edit channel","admintask_edit_channel_" + str(tid))],
            [("🎯 Edit join target","admintask_edit_target_" + str(tid)),("💰 Edit reward","admintask_edit_points_" + str(tid))],
            [("👥 Edit participant limit","admintask_edit_limit_" + str(tid))],
            [("📝 Edit user-facing message","admintask_message_edit_" + str(tid))],
            [("♻️ Reset message to default","admintask_message_reset_" + str(tid))],
            [("📊 View participant stats","admintask_leaderboard_" + str(tid))],
            [("⏸ Pause / Resume task","admintask_toggle_" + str(tid))],
            [("🗑 Delete task","admintask_delete_confirm_" + str(tid))],
            [("⬅️ All tasks","admin_tasks")]
        ]
        await q.edit_message_text(body, reply_markup=kb(rows))
    elif action.startswith("admintask_message_edit_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            tid = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT id,title FROM tasks WHERE id=?", (tid,)).fetchone()
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        template = setting_value(f"task_message_template_{tid}", default_task_message_template())
        media_raw = setting_value(f"task_message_media_{tid}", "")
        try:
            media_info = json.loads(media_raw) if media_raw else None
        except (TypeError, ValueError):
            media_info = None
        media_status = f"Attached media: {media_info.get('kind', 'file')} (send a new file to replace it)." if isinstance(media_info, dict) else "Attached media: none."
        prompt = (
            f"✍️ EDIT CUSTOMER MESSAGE · TASK #{tid} — {task['title']}\n\n"
            "Send a new text template to edit the message, OR attach an image, video, APK, or PDF to attach/replace the task media. "
            "When sending media, its optional caption is saved with the file; the existing text template is kept.\n\n"
            "IMPORTANT: Text templates must keep {{PERSONAL_INVITE_LINK}} exactly once. The bot replaces it with each participant's own personal invite link.\n\n"
            "Available placeholders: {title} · {points} · {channel} · {completed_count} · {target} · {participants} · {status}\n"
            "Text template limit: 3000 characters. " + media_status + "\n\n"
            "CURRENT MESSAGE TEMPLATE:\n" + template
        )
        set_pending(uid, "admin_task_message_template", {"task_id":tid})
        await q.edit_message_text(prompt, reply_markup=kb([[("Cancel","admintask_message_cancel_" + str(tid))]]))
    elif action.startswith("admintask_message_done_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            tid = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("✅ Task message editing finished. Saved text and media changes are kept.",
                                  reply_markup=kb([[("📋 Review task",f"admintask_view_{tid}")],
                                                   [("📋 Manage tasks","admin_tasks")]]))
    elif action.startswith("admintask_message_cancel_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            tid = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (uid,))
        await q.edit_message_text("Message editing cancelled.", reply_markup=kb([[("📋 Review task",f"admintask_view_{tid}")],[("📋 Manage tasks","admin_tasks")]]))
    elif action.startswith("admintask_message_reset_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            tid = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT id FROM tasks WHERE id=?", (tid,)).fetchone()
            if task:
                c.execute("DELETE FROM settings WHERE key IN (?,?)",
                          (f"task_message_template_{tid}", f"task_message_media_{tid}"))
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        await q.edit_message_text("♻️ Customer message reset to the default template. The personal invite link remains automatically generated.", reply_markup=kb([[("📋 Review task",f"admintask_view_{tid}")],[("📋 Manage tasks","admin_tasks")]]))
    elif action.startswith("admintask_edit_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            prefix, tid_text = action.rsplit("_",1)
            tid = int(tid_text)
            field = prefix[len("admintask_edit_"):]
        except (ValueError, IndexError):
            await q.edit_message_text("Invalid task edit request."); return
        field_labels = {"title":"task title","channel":"target channel username or ID","target":"verified-join target","points":"points per verified join","limit":"maximum number of users"}
        if field not in field_labels:
            await q.edit_message_text("Unknown task setting."); return
        set_pending(uid, "admin_task_edit", {"task_id":tid,"field":field})
        extra = " Send the channel's @username or numeric ID. The bot must be an administrator and able to create invite links." if field == "channel" else " For user limit, send 0 for unlimited." if field == "limit" else ""
        await q.edit_message_text(f"Send the new {field_labels[field]}.{extra}", reply_markup=kb([[("Cancel","admintask_view_" + str(tid))]]))
    elif action.startswith("admintask_toggle_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: tid = int(action.rsplit("_",1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT active,completed_count,target FROM tasks WHERE id=?", (tid,)).fetchone()
            if task:
                new_active = 0 if task["active"] else (1 if task["completed_count"] < task["target"] else 0)
                c.execute("UPDATE tasks SET active=? WHERE id=?", (new_active,tid))
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        await q.edit_message_text("✅ Task updated.", reply_markup=kb([[("📋 Review task","admintask_view_" + str(tid))],[("📋 Manage tasks","admin_tasks")]]))
    elif action.startswith("admintask_delete_confirm_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: tid = int(action.rsplit("_",1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT title FROM tasks WHERE id=?", (tid,)).fetchone()
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        await q.edit_message_text(f"Remove task #{tid} · {task['title']}? Its task links and join records will be removed. Previously awarded points will remain in user wallets.", reply_markup=kb([[("🗑 Yes, remove","admintask_delete_" + str(tid)),("Cancel","admintask_view_" + str(tid))]]))
    elif action.startswith("admintask_delete_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: tid = int(action.rsplit("_",1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            links = [r["invite_link"] for r in c.execute("SELECT invite_link FROM invite_links WHERE task_id=?", (tid,)).fetchall()]
            for link in links:
                c.execute("DELETE FROM invite_events WHERE invite_link=?", (link,))
            c.execute("DELETE FROM invite_links WHERE task_id=?", (tid,))
            c.execute("DELETE FROM task_claims WHERE task_id=?", (tid,))
            c.execute("DELETE FROM settings WHERE key=?", (f"task_message_template_{tid}",))
            cur = c.execute("DELETE FROM tasks WHERE id=?", (tid,))
        await q.edit_message_text("🗑 Task removed. Previously awarded points remain unchanged.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")],[("⬅️ Admin Dashboard","admin")]]))
    elif action.startswith("admintask_leaderboard_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: tid = int(action.rsplit("_",1)[1])
        except ValueError:
            await q.edit_message_text("Invalid task ID."); return
        with db() as c:
            task = c.execute("SELECT title,points FROM tasks WHERE id=?", (tid,)).fetchone()
            rows = c.execute(
                "SELECT u.user_id,u.username,u.first_name,COUNT(e.id) joins_count "
                "FROM task_claims tc JOIN users u ON u.user_id=tc.user_id "
                "LEFT JOIN invite_links l ON l.task_id=tc.task_id AND l.owner_user_id=tc.user_id "
                "LEFT JOIN invite_events e ON e.invite_link=l.invite_link "
                "WHERE tc.task_id=? GROUP BY u.user_id ORDER BY joins_count DESC LIMIT 20", (tid,)
            ).fetchall()
        if not task:
            await q.edit_message_text("Task not found.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")]])); return
        body = f"🏆 LEADERBOARD · {task['title']}\nVerified joins by participant:\n\n"
        if rows:
            for i, row in enumerate(rows, 1):
                who = ("@" + row["username"]) if row["username"] else (row["first_name"] or str(row["user_id"]))
                body += f"{i}. {who[:25]} — {row['joins_count']} joins · {row['joins_count'] * int(task['points'])} pts\n"
        else:
            body += "No participants yet."
        await q.edit_message_text(body, reply_markup=kb([[("⬅️ Review task","admintask_view_" + str(tid))],[("📋 Manage tasks","admin_tasks")]]))
    elif action == "admin":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        await q.edit_message_text("🛡 Admin Dashboard\nManage tasks, review payouts and listings, configure prices, and inspect platform statistics.", reply_markup=kb([
            [("➕ Create join task","admin_new_task"),("📋 Manage tasks","admin_tasks")],
            [("📊 Statistics","admin_stats"),("🏆 Task leaderboard","admin_task_leaderboard")],
            [("👥 Total Bot Users","admin_total_bot_users")],
            [("🛍 Social account listings","admin_marketplace")],
            [("🏦 Account payment methods","admin_account_payments")],
            [("📥 Review requests","admin_queue"),("🪙 Crypto orders","admin_crypto_orders")],
            [("💸 Manage Sell USDT","admin_sell_usdt")],
            [("⚙️ Set prices / limits","admin_settings")],
            [("🌟 Gemini Pro & Products","admin_digital_products")],
            [("🛍 Advertiser Management","advertiser_admin")],
            [("📣 Promoter Program Settings","admin_promoters")],
            [("📥 Review Promoter Submissions","promoter_ads_review")],
            [("🔗 Manage Invite & Earn","admin_invite_earn")],
            [("⬅️ Dashboard","home")]
        ]))
    elif action == "admin_invite_earn":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        active = str(setting_value("invite_earn_active", "1")).strip().lower() in {"1", "true", "yes", "on"}
        with db() as c:
            total_users = c.execute("SELECT COUNT(*) n FROM users WHERE referred_by IS NOT NULL").fetchone()["n"]
            total_rewards = c.execute("SELECT COALESCE(SUM(points_awarded),0) n FROM referral_rewards").fetchone()["n"]
            referrers = c.execute("SELECT COUNT(DISTINCT referred_by) n FROM users WHERE referred_by IS NOT NULL").fetchone()["n"]
            total_wallet_points = c.execute("SELECT COALESCE(SUM(points),0) n FROM users").fetchone()["n"]
        points_per_birr = max(1, int(setting_value("points_per_birr", "100") or 100))
        reward = int(setting_value("referral_points", "0") or 0)
        minimum = int(setting_value("min_withdraw_points", "1000") or 1000)
        preview = str(setting_value("invite_earn_message", "") or "Default Invite & Earn information")[:350]
        await q.edit_message_text(
            f"🔗 INVITE & EARN ADMIN\n\nStatus: {'🟢 AVAILABLE' if active else '🔴 UNAVAILABLE'}\n"
            f"New referred users: {total_users}\nUsers who referred someone: {referrers}\n"
            f"Recorded referral points awarded: {total_rewards}\nTotal user wallet points: {total_wallet_points}\n\n"
            f"Reward per referral: {reward} points\nWithdrawal minimum: {minimum} points\n"
            f"Conversion: {points_per_birr} points = 1 ETB\n\nInformation preview:\n{preview}",
            reply_markup=kb([
                [("⏸ Make unavailable" if active else "🟢 Make available","admin_invite_earn_toggle")],
                [("✏️ Edit displayed information","admin_setkey_invite_earn_message")],
                [("⭐ Set points per referral","admin_setkey_referral_points"),("💱 Set points per 1 ETB","admin_setkey_points_per_birr")],
                [("💸 Set withdrawal minimum","admin_setkey_min_withdraw_points")],
                [("✏️ Edit unavailable notice (text)","admin_invite_unavailable_text")],
                [("🖼 Set unavailable notice image","admin_invite_unavailable_photo")],
                [("🗑 Remove notice image","admin_invite_unavailable_clear_photo")],
                [("👥 Review referral participation","admin_invite_earn_users")],
                [("💸 Review withdrawal requests","admin_invite_withdrawals")],
                [("🔄 Refresh statistics","admin_invite_earn"),("⬅️ Admin Dashboard","admin")]
            ])
        )
    elif action == "admin_invite_unavailable_text":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute(
                "INSERT INTO pending_inputs(user_id,action,data) VALUES(?, 'admin_invite_unavailable_text', '') "
                "ON CONFLICT(user_id) DO UPDATE SET action='admin_invite_unavailable_text',data=''",
                (uid,)
            )
        await q.edit_message_text(
            "✏️ Send the complete message users should see when Invite & Earn is unavailable. "
            "This switches the notice to text mode and removes the configured image. Send /cancel to stop.",
            reply_markup=kb([[("❌ Cancel","admin_invite_earn")]])
        )
    elif action == "admin_invite_unavailable_photo":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute(
                "INSERT INTO pending_inputs(user_id,action,data) VALUES(?, 'admin_invite_unavailable_photo', '') "
                "ON CONFLICT(user_id) DO UPDATE SET action='admin_invite_unavailable_photo',data=''",
                (uid,)
            )
        await q.edit_message_text(
            "🖼 Send the image users should see when Invite & Earn is unavailable. "
            "You may add a caption to use as the notice text. Send /cancel to stop.",
            reply_markup=kb([[("❌ Cancel","admin_invite_earn")]])
        )
    elif action == "admin_invite_unavailable_clear_photo":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_unavailable_photo','') "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        await q.edit_message_text(
            "✅ Unavailable notice image removed. The configured text message will be shown instead.",
            reply_markup=kb([[("🔗 Invite & Earn Management","admin_invite_earn")]])
        )
    elif action == "admin_invite_earn_toggle":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        current = str(setting_value("invite_earn_active", "1")).strip().lower() in {"1", "true", "yes", "on"}
        new_value = "0" if current else "1"
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_active',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (new_value,))
        await q.edit_message_text(
            "🔴 Invite & Earn is now unavailable to users." if new_value == "0" else "🟢 Invite & Earn is now available to users.",
            reply_markup=kb([[("🔗 Manage Invite & Earn","admin_invite_earn")],[("⬅️ Admin Dashboard","admin")]])
        )
    elif action == "admin_invite_earn_users":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            rows = c.execute(
                "SELECT u.user_id,u.username,u.first_name,u.points,"
                "(SELECT COUNT(*) FROM users referred WHERE referred.referred_by=u.user_id) referral_count,"
                "COALESCE(SUM(r.points_awarded),0) earned "
                "FROM users u LEFT JOIN referral_rewards r ON r.inviter_user_id=u.user_id "
                "WHERE EXISTS (SELECT 1 FROM users referred WHERE referred.referred_by=u.user_id) "
                "GROUP BY u.user_id,u.username,u.first_name,u.points ORDER BY referral_count DESC,u.user_id LIMIT 20"
            ).fetchall()
        body = ["👥 INVITE & EARN PARTICIPATION", "Choose a referrer to inspect their invited users and recorded rewards.", ""]
        rows_kb = []
        for i, row in enumerate(rows, 1):
            name = ("@" + row["username"]) if row["username"] else (row["first_name"] or str(row["user_id"]))
            body.append(f"{i}. {name[:24]} · ID {row['user_id']} · Referrals: {int(row['referral_count'] or 0)} · Earned: {int(row['earned'] or 0)} pts · Wallet: {int(row['points'] or 0)} pts")
            rows_kb.append([(f"🔎 Review {name[:18]} · {int(row['referral_count'] or 0)}", f"admin_invite_earn_user_{row['user_id']}")])
        if not rows:
            body.append("No referral participation recorded yet.")
        rows_kb.extend([[("🔄 Refresh", "admin_invite_earn_users")],
                        [("⬅️ Invite & Earn Management", "admin_invite_earn")]])
        await q.edit_message_text("\n".join(body)[:3900], reply_markup=kb(rows_kb))
    elif action.startswith("admin_invite_earn_user_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            referrer_id = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid referrer ID."); return
        with db() as c:
            referrer = c.execute("SELECT user_id,username,first_name,points FROM users WHERE user_id=?", (referrer_id,)).fetchone()
            referrals = c.execute(
                "SELECT u.user_id,u.username,u.first_name,u.joined_at,r.points_awarded,r.created_at "
                "FROM users u LEFT JOIN referral_rewards r ON r.invited_user_id=u.user_id "
                "WHERE u.referred_by=? ORDER BY u.joined_at DESC LIMIT 30",
                (referrer_id,)
            ).fetchall()
        if not referrer:
            await q.edit_message_text("Referrer not found.", reply_markup=kb([[("⬅️ Participation","admin_invite_earn_users")]])); return
        referrer_name = ("@" + referrer["username"]) if referrer["username"] else (referrer["first_name"] or str(referrer_id))
        body = [f"🔎 REFERRAL REVIEW · {referrer_name}", f"User ID: {referrer_id}", f"Current wallet: {int(referrer['points'] or 0)} points", "", "Invited users:"]
        for invited in referrals:
            invited_name = ("@" + invited["username"]) if invited["username"] else (invited["first_name"] or str(invited["user_id"]))
            reward_value = int(invited["points_awarded"] or 0)
            body.append(f"• {invited_name[:24]} · ID {invited['user_id']} · Reward: {reward_value} pts · Joined: {str(invited['joined_at'] or '')[:10]}")
        if not referrals:
            body.append("No invited users found.")
        await q.edit_message_text("\n".join(body)[:3900], reply_markup=kb([
            [("⬅️ All referrers", "admin_invite_earn_users")],
            [("💸 Review withdrawal requests", "admin_invite_withdrawals")],
            [("⬅️ Invite & Earn Management", "admin_invite_earn")]
        ]))
    elif action == "admin_invite_withdrawals":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            pending_withdrawals = c.execute(
                "SELECT id,user_id,points,payout_method,status,created_at FROM withdrawals WHERE status='pending' ORDER BY id DESC LIMIT 20"
            ).fetchall()
        rows_kb = []
        body = ["💸 INVITE & EARN · WITHDRAWAL REVIEW", "Choose a request to inspect payout details, then approve or reject it.", ""]
        for wd in pending_withdrawals:
            body.append(f"Request #{wd['id']} · User {wd['user_id']} · {wd['points']} pts · {wd['payout_method']} · {str(wd['created_at'] or '')[:10]}")
            rows_kb.append([(f"🔎 Request #{wd['id']} · {wd['points']} pts", f"admin_invite_withdraw_view_{wd['id']}")])
        if not pending_withdrawals:
            body.append("There are no pending withdrawal requests.")
        rows_kb.extend([[("🔄 Refresh", "admin_invite_withdrawals")],
                        [("📥 All review requests", "admin_queue")],
                        [("⬅️ Invite & Earn Management", "admin_invite_earn")]])
        await q.edit_message_text("\n".join(body)[:3900], reply_markup=kb(rows_kb))
    elif action.startswith("admin_invite_withdraw_view_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try:
            withdrawal_id = int(action.rsplit("_", 1)[1])
        except ValueError:
            await q.edit_message_text("Invalid withdrawal ID."); return
        with db() as c:
            wd = c.execute(
                "SELECT w.*,u.username,u.first_name,u.points wallet_points FROM withdrawals w "
                "LEFT JOIN users u ON u.user_id=w.user_id WHERE w.id=?",
                (withdrawal_id,)
            ).fetchone()
        if not wd:
            await q.edit_message_text("Withdrawal request not found.", reply_markup=kb([[("⬅️ Withdrawals", "admin_invite_withdrawals")]])); return
        body = (f"💸 WITHDRAWAL REQUEST #{withdrawal_id}\n\nUser: {wd['first_name'] or 'Unknown'} "
                f"(@{wd['username'] or 'no_username'})\nUser ID: {wd['user_id']}\n"
                f"Points requested: {wd['points']}\nCurrent wallet: {wd['wallet_points'] or 0} points\n"
                f"Payout method: {wd['payout_method']}\nPayout details: {wd['payout_details']}\n"
                f"Status: {wd['status']}\nSubmitted: {wd['created_at']}")
        rows_kb = []
        if wd["status"] == "pending":
            rows_kb.append([("✅ Approve", f"approve_wd_{withdrawal_id}"), ("❌ Reject", f"reject_wd_{withdrawal_id}")])
        if wd["status"] in ("pending", "approved"):
            rows_kb.append([("📨 Send payout message / proof", f"withdraw_delivery_{withdrawal_id}")])
        rows_kb.extend([[("⬅️ Pending withdrawals", "admin_invite_withdrawals")],
                        [("⬅️ Invite & Earn Management", "admin_invite_earn")]])
        await q.edit_message_text(body[:3900], reply_markup=kb(rows_kb))
    elif action == "admin_new_task":
        if not is_admin(uid): return
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,'admin_task_title','') ON CONFLICT(user_id) DO UPDATE SET action='admin_task_title',data=''", (uid,))
        await q.edit_message_text("Create a task using this format (separate each value with |):\nTask title | @channel or channel ID | verified-join target | points per join | max users\nExample: Join Channel | @ExampleChannel | 50 | 20 | 100\nSet max users to 0 for unlimited. The bot must be an administrator in the channel with invite-link permissions.", reply_markup=kb([[("Cancel","admin")]]))
    elif action == "admin_total_bot_users":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            total_users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
        await q.edit_message_text(
            f"👥 TOTAL BOT USERS\n\nRegistered users: {total_users}",
            reply_markup=kb([[("🔄 Refresh","admin_total_bot_users")], [("⬅️ Admin Dashboard","admin")]])
        )
    elif action == "admin_stats":
        if not is_admin(uid): return
        with db() as c:
            users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            tasks = c.execute("SELECT COUNT(*) n FROM tasks").fetchone()["n"]
            assigned = c.execute("SELECT COUNT(*) n FROM task_claims").fetchone()["n"]
            active_tasks = c.execute("SELECT COUNT(*) n FROM tasks WHERE active=1 AND completed_count<target").fetchone()["n"]
            ads = c.execute("SELECT COUNT(*) n FROM ad_requests WHERE status='pending'").fetchone()["n"]
            wd = c.execute("SELECT COUNT(*) n FROM withdrawals WHERE status='pending'").fetchone()["n"]
            listings = c.execute("SELECT COUNT(*) n FROM market_listings WHERE status='pending'").fetchone()["n"]
            joins = c.execute("SELECT COUNT(*) n FROM invite_events").fetchone()["n"]
        await q.edit_message_text(f"📊 PLATFORM STATISTICS\nUsers: {users}\nTasks total: {tasks}\nTasks open: {active_tasks}\nUsers assigned to tasks: {assigned}\nVerified task joins: {joins}\nPending ad requests: {ads}\nPending withdrawals: {wd}\nPending marketplace listings: {listings}", reply_markup=kb([[("🏆 Task leaderboard","admin_task_leaderboard")],[("📋 Manage tasks","admin_tasks")],[("⬅️ Admin Dashboard","admin")]]))
    elif action == "admin_settings":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        # Returning to this menu cancels an unfinished single-setting edit.
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='admin_setting_value'", (uid,))
        await q.edit_message_text(
            "⚙️ Prices & Limits\n\nChoose one service to manage. You’ll see its settings separately, then the bot will ask for one value at a time.",
            reply_markup=kb([
                [("📣 Ads & Promotion","admin_setgroup_ads")],
                [("📞 Contact & Support","admin_setgroup_contact")],
                [("🎁 Rewards & Withdrawals","admin_setgroup_rewards")],
                [("💵 Buy USDT","admin_setgroup_buyusdt")],
                [("⬅️ Admin Dashboard","admin")]
            ])
        )
    elif action.startswith("admin_setgroup_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        group = action.removeprefix("admin_setgroup_")
        groups = {
            "contact": ("📞 Contact & Support", [
                ("Admin Telegram username", "contact_admin_username"),
            ]),
            "ads": ("📣 Ads & Promotion", [
                ("Product promotion price (ETB)", "price_ad_product"),
                ("Member promotion price (ETB)", "price_ad_members"),
                ("Views promotion price (ETB)", "price_ad_views"),
            ]),
            "rewards": ("🎁 Rewards & Withdrawals", [
                ("Minimum withdrawal points", "min_withdraw_points"),
                ("Referral reward points", "referral_points"),
                ("Points per 1 ETB (Birr)", "points_per_birr"),
                ("Invite & Earn displayed information", "invite_earn_message"),
            ]),
            "buyusdt": ("💵 Buy USDT", [
                ("Buy rate (ETB per USDT)", "buy_usdt_rate"),
                ("Minimum order (USDT)", "buy_usdt_min"),
                ("Maximum order (USDT)", "buy_usdt_max"),
                ("Available stock (USDT)", "buy_usdt_stock"),
                ("BEP20 minimum (USDT)", "buy_usdt_bep20_min"),
                ("Bybit minimum (USDT)", "buy_usdt_bybit_min"),
                ("Processing time message", "buy_usdt_processing_time"),
            ]),
        }
        if group not in groups:
            await q.edit_message_text("That settings group is unavailable.", reply_markup=kb([[("⬅️ Prices & Limits","admin_settings")]]))
            return
        title, items = groups[group]
        rows = []
        for label, key in items:
            current = setting_value(key, "Not set")
            rows.append([(f"{label}: {str(current)[:18]}", f"admin_setkey_{key}")])
        rows.extend([[("⬅️ Service groups","admin_settings")], [("⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text(f"{title}\n\nChoose the one setting you want to change:", reply_markup=kb(rows))
    elif action.startswith("admin_setkey_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only.")
            return
        key = action.removeprefix("admin_setkey_")
        allowed_keys = {
            "contact_admin_username",
            "price_ad_product", "price_ad_members", "price_ad_views",
            "min_withdraw_points", "referral_points", "points_per_birr", "invite_earn_message", "buy_usdt_rate",
            "buy_usdt_min", "buy_usdt_max", "buy_usdt_stock",
            "buy_usdt_bep20_min", "buy_usdt_bybit_min", "buy_usdt_processing_time",
        }
        if key not in allowed_keys:
            await q.edit_message_text("That setting is not available here.", reply_markup=kb([[("⬅️ Prices & Limits","admin_settings")]]))
            return
        with db() as c:
            c.execute(
                "INSERT INTO pending_inputs(user_id,action,data) VALUES(?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,data=excluded.data",
                (uid, "admin_setting_value", key)
            )
        labels = {
            "contact_admin_username": "Admin Telegram username",
            "price_ad_product": "Product promotion price in ETB",
            "price_ad_members": "Member promotion price in ETB",
            "price_ad_views": "Views promotion price in ETB",
            "min_withdraw_points": "Minimum withdrawal points",
            "referral_points": "Referral reward points",
            "points_per_birr": "Points required for 1 ETB (Birr)",
            "invite_earn_message": "Invite & Earn displayed information",
            "buy_usdt_rate": "Buy rate in ETB per USDT",
            "buy_usdt_min": "Minimum Buy USDT order",
            "buy_usdt_max": "Maximum Buy USDT order",
            "buy_usdt_stock": "Available Buy USDT stock",
            "buy_usdt_bep20_min": "BEP20 minimum order",
            "buy_usdt_bybit_min": "Bybit minimum order",
            "buy_usdt_processing_time": "Processing time message",
        }
        current = setting_value(key, "Not set")
        await q.edit_message_text(
            f"✏️ Change: {labels[key]}\n\nCurrent value: {current}\n\nSend the new value in one message. "
            + ("Send the admin's Telegram username (for example @yourname), or a https://t.me/username link. Send 'off' to hide the profile link. "
               if key == "contact_admin_username" else
               "Enter the number of points equal to 1 ETB (for example 100). " if key == "points_per_birr" else
               "Send the full customer-facing Invite & Earn message (up to 2500 characters). Supported placeholders: {link}, {reward_points}, {referrals}, {earned_points}, {wallet_points}, {points_per_birr}, {wallet_birr}, {minimum_points}, {pending_withdrawals}. " if key == "invite_earn_message" else
               "For prices, rates, stock, and USDT amounts, enter a number only. For processing time, you can send text such as 1–2 hours."),
            reply_markup=kb([[("❌ Cancel","admin_settings")]])
        )
    elif action == "promoter_ads_toggle":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        new_value = "0" if setting_enabled("promoter_ads_active") else "1"
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('promoter_ads_active',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (new_value,))
        await q.edit_message_text(
            "Promoter submissions are now " + ("AVAILABLE to users." if new_value == "1" else "UNAVAILABLE to users."),
            reply_markup=kb([[("⬅️ Promoter Center","admin_promoters")]])
        )
    elif action == "promoter_ads_set_limit":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        current = setting_value("promoter_ads_submission_limit", "0")
        set_pending(uid, "promoter_ads_limit_edit", {})
        await q.edit_message_text(f"🔢 SET PROMOTER SUBMISSION LIMIT\n\nCurrent limit: {current}\nSend a whole number. Use 0 for unlimited submissions.", reply_markup=kb([[("❌ Cancel","admin_promoters")]]))
    elif action == "promoter_ads_form_settings":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        rows = [
            [("✏️ Platform question","promoter_ads_edit_question_platforms")],
            [("✏️ Content question","promoter_ads_edit_question_content")],
            [("✏️ Follower-count question","promoter_ads_edit_question_followers")],
            [("✏️ Channel-link question","promoter_ads_edit_question_link")],
            [("✏️ Pricing question","promoter_ads_edit_question_prices")],
            [("⬅️ Promoter Center","admin_promoters")]
        ]
        await q.edit_message_text("📝 EDIT PROMOTER FORM\n\nChoose a question to update. These questions are shown to users while they submit their channel/profile for advertising. Required fields and validation remain enabled.", reply_markup=kb(rows))
    elif action.startswith("promoter_ads_edit_question_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        field = action.removeprefix("promoter_ads_edit_question_")
        if field not in {"platforms","content","followers","link","prices"}:
            await q.edit_message_text("Unknown form question."); return
        set_pending(uid, "promoter_ads_question_edit", {"field":field})
        current = setting_value("promoter_question_"+field, "")
        await q.edit_message_text(f"✏️ EDIT QUESTION · {field.replace('_',' ').title()}\n\nCurrent question:\n{current}\n\nSend the new question text (1–500 characters).", reply_markup=kb([[("❌ Cancel","promoter_ads_form_settings")]]))
    elif action == "promoter_ads_review":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            submissions = c.execute("SELECT id,user_id,platforms,followers,status FROM promoter_ad_submissions ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, id DESC LIMIT 30").fetchall()
        rows = [[(f"{'🕓' if s['status']=='pending' else '✅'} #{s['id']} · {s['platforms'][:25]} · {s['followers']} followers · {s['status']}", f"promoter_ads_view_{s['id']}")] for s in submissions]
        rows += [[("🔄 Refresh","promoter_ads_review")],[("⬅️ Promoter Center","admin_promoters")]]
        await q.edit_message_text("📥 PROMOTER SUBMISSIONS\nChoose a submission to review. Pending submissions appear first.", reply_markup=kb(rows))
    elif action.startswith("promoter_ads_view_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: submission_id = int(action.removeprefix("promoter_ads_view_"))
        except ValueError:
            await q.edit_message_text("Invalid submission ID."); return
        with db() as c:
            sub = c.execute("SELECT * FROM promoter_ad_submissions WHERE id=?", (submission_id,)).fetchone()
            u = c.execute("SELECT username,first_name FROM users WHERE user_id=?", (sub["user_id"],)).fetchone() if sub else None
        if not sub:
            await q.edit_message_text("Submission not found.", reply_markup=kb([[("⬅️ Submissions","promoter_ads_review")]])); return
        price_lines = []
        for period,label in [("price_day","Per day"),("price_week","Per week"),("price_month","Per month")]:
            if sub[period] is not None: price_lines.append(f"{label}: {sub[period]:g} ETB")
        username = ("@"+u["username"]) if u and u["username"] else (u["first_name"] if u else str(sub["user_id"]))
        body = (f"📣 PROMOTER SUBMISSION #{submission_id}\n\nUser: {username} (ID {sub['user_id']})\n"
                f"Platforms: {sub['platforms']}\nContent type: {sub['content_type']}\nFollowers/subscribers: {sub['followers']}\n"
                f"Channel/profile link: {sub['channel_link']}\nPrices:\n" + ("\n".join(price_lines) if price_lines else "No prices") +
                f"\n\nStatus: {sub['status']}\nSubmitted: {sub['created_at']}\nAdmin note: {sub['admin_note'] or 'None'}")
        rows = []
        if sub["status"] == "pending": rows.append([("✅ Save / Approve profile","promoter_ads_save_"+str(submission_id))])
        rows += [[("💬 Message user (text/photo)","promoter_ads_message_"+str(submission_id))],
                 [("🗑 Delete submission","promoter_ads_delete_confirm_"+str(submission_id))],
                 [("⬅️ All elif action == "admin_promoter_withdraw_limit":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        current=setting_value("promoter_withdraw_min_points","1000")
        set_pending(uid,"admin_promoter_withdraw_limit_edit",{})
        await q.edit_message_text(f"💰 SET PROMOTER WITHDRAWAL POINT LIMIT\n\nCurrent minimum: {current} points. Send a whole number. Send 0 to skip the point minimum (users still need at least 1 point).",reply_markup=kb([[( "❌ Cancel","admin_promoters")]]))
    elif action == "admin_promoter_toggle_target_rule":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        new_value="0" if setting_enabled("promoter_withdraw_require_target",True) else "1"
        with db() as c: c.execute("INSERT INTO settings(key,value) VALUES('promoter_withdraw_require_target',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(new_value,))
        await q.edit_message_text("✅ Referral target rule is now "+("ON. Users must reach their join target before withdrawing." if new_value=="1" else "OFF. Users do not need to reach the join target, but must meet the point limit."),reply_markup=kb([[( "⬅️ Promoter Program","admin_promoters")]]))
    elif action == "admin_promoter_toggle_chat":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        new_value="0" if setting_enabled("promoter_chat_enabled",True) else "1"
        with db() as c: c.execute("INSERT INTO settings(key,value) VALUES('promoter_chat_enabled',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(new_value,))
        await q.edit_message_text("Promoter chat is now "+("enabled." if new_value=="1" else "disabled for users."),reply_markup=kb([[( "⬅️ Promoter Program","admin_promoters")]]))
    elif action == "admin_promoter_statistics":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            total=c.execute("SELECT COUNT(*) n FROM promoter_profiles").fetchone()["n"]
            active=c.execute("SELECT COUNT(*) n FROM promoter_profiles WHERE status='active'").fetchone()["n"]
            joins=c.execute("SELECT COUNT(*) n FROM promoter_join_events").fetchone()["n"]
            pending=c.execute("SELECT COUNT(*) n FROM withdrawals WHERE status='pending' AND payout_method IN ('Telebirr','CBE','CBE Birr')").fetchone()["n"]
            paid=c.execute("SELECT COALESCE(SUM(points),0) n FROM withdrawals WHERE status='approved' AND payout_method IN ('Telebirr','CBE','CBE Birr')").fetchone()["n"]
        await q.edit_message_text(f"📈 PROMOTER STATISTICS\n\nRegistered promoter profiles: {total}\nActive profiles: {active}\nVerified referral joins: {joins}\nPending promoter withdrawals: {pending}\nPoints in approved withdrawals: {paid}\n\nWithdrawal minimum: {setting_value('promoter_withdraw_min_points','1000')} points\nTarget rule: {'ON' if setting_enabled('promoter_withdraw_require_target',True) else 'OFF'}",reply_markup=kb([[( "👥 View promoter users","admin_promoter_users")],[( "💬 Promoter chat inbox","admin_promoter_chat_inbox")],[( "⬅️ Promoter Program","admin_promoters")]]))
    elif action == "admin_promoter_chat_inbox":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            profiles=c.execute("SELECT user_id,method,completed_count,target_count FROM promoter_profiles ORDER BY updated_at DESC LIMIT 30").fetchall()
        rows=[[(f"💬 User {p['user_id']} · {p['completed_count']}/{p['target_count']} joins · {p['method']}",f"admin_promoter_chat_reply_{p['user_id']}")] for p in profiles]
        rows += [[( "🔄 Refresh","admin_promoter_chat_inbox")],[( "⬅️ Promoter Program","admin_promoters")]]
        await q.edit_message_text("💬 PROMOTER CHAT INBOX\nChoose a promoter to start or continue a conversation. You can send multiple text, photo, or document messages.",reply_markup=kb(rows))
    elif action.startswith("admin_promoter_chat_reply_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: target_uid=int(action.removeprefix("admin_promoter_chat_reply_"))
        except ValueError:
            await q.edit_message_text("Invalid promoter ID."); return
        with db() as c: exists=c.execute("SELECT user_id FROM promoter_profiles WHERE user_id=?",(target_uid,)).fetchone()
        if not exists:
            await q.edit_message_text("Promoter profile not found."); return
        set_pending(uid,"admin_promoter_chat_reply",{"user_id":target_uid})
        await q.edit_message_text(f"💬 Reply to promoter {target_uid}. Send text, a photo with caption, or a document. You can send multiple messages; use /done to finish.",reply_markup=kb([[( "✅ Finish reply","admin_promoter_chat_done")],[( "⬅️ Chat inbox","admin_promoter_chat_inbox")]]))
    elif action == "admin_promoter_chat_done":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='admin_promoter_chat_reply'",(uid,))
        await q.edit_message_text("Chat reply mode closed.",reply_markup=kb([[( "💬 Promoter chat inbox","admin_promoter_chat_inbox")],[( "⬅️ Promoter Program","admin_promoters")]]))
    submissions","promoter_ads_review")]]
        await q.edit_message_text(body, reply_markup=kb(rows), disable_web_page_preview=True)
    elif action.startswith("promoter_ads_save_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: submission_id = int(action.removeprefix("promoter_ads_save_"))
        except ValueError:
            await q.edit_message_text("Invalid submission ID."); return
        with db() as c:
            cur = c.execute("UPDATE promoter_ad_submissions SET status='approved',updated_at=? WHERE id=?", (now(),submission_id))
            sub = c.execute("SELECT user_id FROM promoter_ad_submissions WHERE id=?", (submission_id,)).fetchone()
        if not cur.rowcount:
            await q.edit_message_text("Submission not found."); return
        if sub:
            try: await context.bot.send_message(sub["user_id"], f"✅ Your promoter profile submission #{submission_id} has been saved/approved by an admin. We'll contact you if more details are needed.")
            except Exception: pass
        await q.edit_message_text("✅ Submission saved/approved.", reply_markup=kb([[("📋 View submission",f"promoter_ads_view_{submission_id}")],[("⬅️ All submissions","promoter_ads_review")]]))
    elif action.startswith("promoter_ads_delete_confirm_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        submission_id = action.removeprefix("promoter_ads_delete_confirm_")
        await q.edit_message_text(f"Delete promoter submission #{submission_id}? This permanently removes the saved review.", reply_markup=kb([[("🗑 Yes, delete","promoter_ads_delete_"+submission_id),("Cancel","promoter_ads_view_"+submission_id)]]))
    elif action.startswith("promoter_ads_delete_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: submission_id = int(action.removeprefix("promoter_ads_delete_"))
        except ValueError:
            await q.edit_message_text("Invalid submission ID."); return
        with db() as c:
            cur = c.execute("DELETE FROM promoter_ad_submissions WHERE id=?", (submission_id,))
        await q.edit_message_text("🗑 Submission deleted." if cur.rowcount else "Submission not found.", reply_markup=kb([[("⬅️ All submissions","promoter_ads_review")],[("⬅️ Promoter Center","admin_promoters")]]))
    elif action.startswith("promoter_ads_message_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: submission_id = int(action.removeprefix("promoter_ads_message_"))
        except ValueError:
            await q.edit_message_text("Invalid submission ID."); return
        with db() as c:
            sub = c.execute("SELECT user_id FROM promoter_ad_submissions WHERE id=?", (submission_id,)).fetchone()
        if not sub:
            await q.edit_message_text("Submission not found."); return
        set_pending(uid, "promoter_ads_admin_message", {"submission_id":submission_id,"target_user_id":int(sub["user_id"])})
        await q.edit_message_text("💬 Send a text message, a photo with an optional caption, or a document with an optional caption. It will be delivered privately to the promoter.", reply_markup=kb([[("❌ Cancel",f"promoter_ads_view_{submission_id}")]]))
    elif action == "admin_promoters":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action IN ('admin_promoter_setting','admin_promoter_message','admin_promoter_edit_payout')", (uid,))
        rules = setting_value("promoter_rules", "")
        channel = setting_value("promoter_channel", "Not set")
        target = setting_value("promoter_target", "100")
        points = setting_value("promoter_points_per_join", "1")
        ads_active = setting_enabled("promoter_ads_active")
        try: submission_limit = max(0, int(setting_value("promoter_ads_submission_limit", "0") or 0))
        except (TypeError, ValueError): submission_limit = 0
        rows = [
            [("📥 Review promoter submissions","promoter_ads_review")],
            [("📝 Edit promoter form","promoter_ads_form_settings")],
            [("🔢 Set submission limit","promoter_ads_set_limit")],
            [("🔴 Turn submissions off" if ads_active else "🟢 Turn submissions on","promoter_ads_toggle")],
            [("✏️ Edit legacy referral rules","admin_promoter_set_rules")],
            [("📡 Set referral channel","admin_promoter_set_channel")],
            [("🎯 Set referral target","admin_promoter_set_target")],
            [("⭐ Set referral points","admin_promoter_set_points")],
            [("💰 Set withdrawal point limit","admin_promoter_withdraw_limit")],
            [("🔒 Require referral target: " + ("ON" if setting_enabled("promoter_withdraw_require_target", True) else "OFF"),"admin_promoter_toggle_target_rule")],
            [("💬 Promoter chat inbox","admin_promoter_chat_inbox")],
            [("🔴 Disable promoter chat" if setting_enabled("promoter_chat_enabled", True) else "🟢 Enable promoter chat","admin_promoter_toggle_chat")],
            [("📈 Promoter statistics","admin_promoter_statistics")],
            [("👥 View promoter users","admin_promoter_users")],
            [("⬅️ Admin Dashboard","admin")]
        ]
        await q.edit_message_text(
            f"📣 PROMOTER CENTER SETTINGS\n\nPromoter ad submissions: {'🟢 AVAILABLE' if ads_active else '🔴 UNAVAILABLE'}\n"
            f"Submission limit: {submission_limit if submission_limit else 'Unlimited'} approved/pending profiles\n"
            f"Legacy referral channel: {channel or 'Not set'}\nLegacy target: {target} verified joins\nLegacy points per join: {points}\n\n"
            "Use Review promoter submissions to approve/save or delete submitted channel profiles. "
            "Use Edit promoter form to change the questions users see.",
            reply_markup=kb(rows)
        )
    elif action.startswith("admin_promoter_set_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        field = action.removeprefix("admin_promoter_set_")
        labels = {"rules":"campaign rules/instructions","channel":"target channel username or ID","target":"target number of verified joins per promoter","points":"points rewarded per verified join"}
        if field not in labels:
            await q.edit_message_text("Setting not found.", reply_markup=kb([[("⬅️ Promoter Program","admin_promoters")]])); return
        set_pending(uid, "admin_promoter_setting", {"field":field})
        current = setting_value("promoter_"+field, "Not set")
        await q.edit_message_text(
            f"✏️ Update promoter {labels[field]}\n\nCurrent value: {str(current)[:700]}\n\nSend the new value in one message. For the channel, the bot must be an administrator with invite-link permission. For target/points, enter a positive whole number.",
            reply_markup=kb([[("❌ Cancel","admin_promoters")]])
        )
    elif action == "admin_promoter_users":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action IN ('admin_promoter_setting','admin_promoter_message','admin_promoter_edit_payout')", (uid,))
            profiles = c.execute("SELECT user_id,status,completed_count,target_count,method FROM promoter_profiles ORDER BY updated_at DESC LIMIT 20").fetchall()
        rows = [[(f"{p['user_id']} · {p['completed_count']}/{p['target_count']} · {p['status']}", f"admin_promoter_user_{p['user_id']}")] for p in profiles]
        rows.extend([[("🔄 Refresh","admin_promoter_users")],[("⬅️ Promoter Program","admin_promoters")],[("⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text("👥 Promoter users\nChoose a user to view their saved payout information, progress, and send them a message:", reply_markup=kb(rows))
    elif action.startswith("admin_promoter_user_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action IN ('admin_promoter_setting','admin_promoter_message','admin_promoter_edit_payout')", (uid,))
        try: promoter_uid = int(action.removeprefix("admin_promoter_user_"))
        except ValueError:
            await q.edit_message_text("Invalid promoter user ID."); return
        with db() as c:
            profile = c.execute("SELECT * FROM promoter_profiles WHERE user_id=?", (promoter_uid,)).fetchone()
            user_row = c.execute("SELECT username,first_name,points FROM users WHERE user_id=?", (promoter_uid,)).fetchone()
            joined = c.execute("SELECT COUNT(*) n FROM promoter_join_events WHERE promoter_user_id=?", (promoter_uid,)).fetchone()["n"]
        if not profile:
            await q.edit_message_text("Promoter profile not found.", reply_markup=kb([[("⬅️ Promoter users","admin_promoter_users")]])); return
        msg = (f"👤 PROMOTER PROFILE\nUser ID: {promoter_uid}\nName: {(user_row['first_name'] or '—') if user_row else '—'}\n"
               f"Username: @{user_row['username'] if user_row and user_row['username'] else '—'}\nWallet points: {user_row['points'] if user_row else 0}\n"
               f"Status: {profile['status']}\nProgress: {profile['completed_count']}/{profile['target_count']}\n"
               f"Tracked joins: {joined}\nPoints per join: {profile['points_per_join']}\nPayout: {profile['method']}\n"
               f"Account number: {profile['account_number']}\nAccount holder: {profile['account_name']}\nInvite link: {profile['invite_link'] or 'Not created'}")
        await q.edit_message_text(msg, reply_markup=kb([
            [("💬 Chat with user (text/photo)",f"admin_promoter_chat_reply_{promoter_uid}")],
            [("✏️ Edit payout method",f"admin_promoter_edit_method_{promoter_uid}")],
            [("✏️ Edit account number",f"admin_promoter_edit_number_{promoter_uid}")],
            [("✏️ Edit account holder",f"admin_promoter_edit_name_{promoter_uid}")],
            [("🔗 Refresh invite link",f"admin_promoter_refresh_{promoter_uid}")],
            [("⬅️ Promoter users","admin_promoter_users")]
        ]))
    elif action.startswith("admin_promoter_edit_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        parts = action.split("_")
        if len(parts) < 5:
            await q.edit_message_text("Invalid edit action."); return
        field = parts[3]
        promoter_uid = parts[4]
        field_map = {"method":"method","number":"account_number","name":"account_name"}
        if field not in field_map or not promoter_uid.isdigit():
            await q.edit_message_text("Invalid payout field."); return
        set_pending(uid, "admin_promoter_edit_payout", {"user_id":int(promoter_uid),"field":field_map[field]})
        prompt = "Send Telebirr or CBE." if field == "method" else ("Send the new account/phone number." if field == "number" else "Send the new account-holder name.")
        await q.edit_message_text(f"✏️ Edit promoter {field}\n\n{prompt}", reply_markup=kb([[("❌ Cancel",f"admin_promoter_user_{promoter_uid}")]]))
    elif action.startswith("admin_promoter_refresh_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: promoter_uid = int(action.removeprefix("admin_promoter_refresh_"))
        except ValueError:
            await q.edit_message_text("Invalid user ID."); return
        channel = setting_value("promoter_channel", "").strip()
        if not channel:
            await q.edit_message_text("Set the promoter referral channel first.", reply_markup=kb([[("⬅️ Promoter Program","admin_promoters")]])); return
        try:
            link_obj = await context.bot.create_chat_invite_link(chat_id=channel, name=f"kefia-promoter-{promoter_uid}")
            with db() as c:
                c.execute("UPDATE promoter_profiles SET invite_link=?,status=CASE WHEN completed_count>=target_count THEN 'completed' ELSE 'active' END,updated_at=? WHERE user_id=?", (link_obj.invite_link,now(),promoter_uid))
            await context.bot.send_message(promoter_uid, f"🔗 Your promoter referral link is ready:\n{link_obj.invite_link}\n\nShare it with real people and track your progress with 📊 My promoter progress.")
            await q.edit_message_text("✅ Invite link refreshed and sent to the promoter.", reply_markup=kb([[("👤 Promoter profile",f"admin_promoter_user_{promoter_uid}")],[("⬅️ Promoter users","admin_promoter_users")]]))
        except Exception as exc:
            log.warning("Could not refresh promoter invite link: %s", exc)
            await q.edit_message_text("Could not create the link. Check that the bot is an admin in the configured channel and can invite users.", reply_markup=kb([[("👤 Promoter profile",f"admin_promoter_user_{promoter_uid}")]]))
    elif action.startswith("admin_promoter_message_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: promoter_uid = int(action.removeprefix("admin_promoter_message_"))
        except ValueError:
            await q.edit_message_text("Invalid user ID."); return
        set_pending(uid, "admin_promoter_message", {"user_id":promoter_uid})
        await q.edit_message_text(f"💬 Send the message you want to deliver to promoter {promoter_uid}.", reply_markup=kb([[("❌ Cancel",f"admin_promoter_user_{promoter_uid}")]]))
    elif action == "admin_account_payments":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            gateways = c.execute("SELECT id,name,account_number,account_name,enabled FROM digital_payment_gateways ORDER BY name").fetchall()
        rows = [[(f"{'🟢' if g['enabled'] else '🔴'} {g['name']} · {g['account_number'] or 'number not set'}",f"digital_admin_gateway_{g['id']}")] for g in gateways]
        rows.extend([[("➕ Add / manage payment methods","admin_digital_products")],[("⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text(
            "🏦 SOCIAL ACCOUNT PAYMENT METHODS\n\nThese methods are shared with the digital-products payment gateway manager. Open a method to edit its name, account number, account holder, instructions, warning, or enabled status. Only enabled methods are offered to buyers.\n\nIf no methods appear, open Add / manage payment methods to configure CBE and Telebirr first.",
            reply_markup=kb(rows)
        )
    elif action == "admin_marketplace":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action IN ('admin_market_edit','admin_market_message')", (uid,))
            items = c.execute("SELECT id,asset_type,status,listing_active,price,purchase_status FROM market_listings WHERE action='sell_social' ORDER BY id DESC LIMIT 25").fetchall()
        rows = []
        for item in items:
            state = "LIVE" if item["listing_active"] else item["status"].upper()
            price_label = f"{item['price']:g} ETB" if item["price"] is not None else "price unset"
            rows.append([(f"#{item['id']} · {item['asset_type']} · {state} · {price_label}"[:62],f"admin_market_item_{item['id']}")])
        rows.extend([[("🔄 Refresh","admin_marketplace")],[("⬅️ Admin Dashboard","admin")]])
        rows.insert(0,[("🏦 Configure payment methods","admin_account_payments")])
        await q.edit_message_text("🛍 SOCIAL ACCOUNT LISTINGS\n\nManage account listings and orders. Open an order to review receipts, approve/reject purchases, or reply to the buyer. Text, photos, and documents are saved to the buyer's order conversation.", reply_markup=kb(rows))
    elif action.startswith("admin_market_item_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action IN ('admin_market_edit','admin_market_message')", (uid,))
        try: listing_id = int(action.removeprefix("admin_market_item_"))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND action='sell_social'", (listing_id,)).fetchone()
            buyer = c.execute("SELECT username,first_name FROM users WHERE user_id=?", (item["buyer_user_id"],)).fetchone() if item and item["buyer_user_id"] else None
        if not item:
            await q.edit_message_text("Listing not found.", reply_markup=kb([[("⬅️ Social account listings","admin_marketplace")]])); return
        desc = item["short_description"] or item["details"] or "Not set"
        msg = (f"🛍 LISTING #{listing_id}\nPlatform: {item['asset_type']}\nSeller ID: {item['user_id']}\n"
               f"Status: {item['status']}\nPublished: {'Yes' if item['listing_active'] else 'No'}\n"
               f"Price: {item['price'] if item['price'] is not None else 'Not set'} ETB\n"
               f"Purchase status: {item['purchase_status']}\nBuyer ID: {item['buyer_user_id'] or 'None'}\n"
               f"Payment method: {item['payment_method'] or 'None'}\nShort description:\n{desc[:900]}\n\nSeller submission:\n{(item['details'] or '')[:700]}")
        rows = [
            [("✏️ Edit short description",f"admin_market_desc_{listing_id}"),("💰 Set price",f"admin_market_price_{listing_id}")],
            [("📣 Publish listing",f"admin_market_publish_{listing_id}"),("🙈 Hide listing",f"admin_market_hide_{listing_id}")],
            [("💬 Message seller",f"admin_market_message_{listing_id}_seller")],
        ]
        if item["buyer_user_id"]:
            rows.append([("💬 Message buyer",f"admin_market_message_{listing_id}_buyer")])
        if item["receipt_file_id"]:
            rows.append([("🧾 Resend receipt to admin",f"admin_market_receipt_{listing_id}")])
        if item["purchase_status"] == "receipt_submitted":
            rows.append([("✅ Approve purchase",f"admin_market_purchase_approve_{listing_id}"),("❌ Reject purchase",f"admin_market_purchase_reject_{listing_id}")])
        rows.extend([[("⬅️ All listings","admin_marketplace")],[("⬅️ Admin Dashboard","admin")]])
        await q.edit_message_text(msg, reply_markup=kb(rows))
    elif action.startswith("admin_market_price_") or action.startswith("admin_market_desc_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        prefix = "admin_market_price_" if action.startswith("admin_market_price_") else "admin_market_desc_"
        try: listing_id = int(action.removeprefix(prefix))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        field = "price" if prefix.endswith("price_") else "description"
        set_pending(uid,"admin_market_edit",{"listing_id":listing_id,"field":field})
        prompt = "Send the sale price in ETB (a positive number)." if field == "price" else "Send the short customer-facing description (max 350 characters). This is what buyers will see before tapping Buy now."
        await q.edit_message_text(f"✏️ Edit listing #{listing_id}\n\n{prompt}", reply_markup=kb([[("❌ Cancel",f"admin_market_item_{listing_id}")]]))
    elif action.startswith("admin_market_publish_") or action.startswith("admin_market_hide_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        publish = action.startswith("admin_market_publish_")
        prefix = "admin_market_publish_" if publish else "admin_market_hide_"
        try: listing_id = int(action.removeprefix(prefix))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        with db() as c:
            item = c.execute("SELECT status,price,short_description FROM market_listings WHERE id=? AND action='sell_social'", (listing_id,)).fetchone()
            if publish and item and item["status"] == "approved" and item["price"] is not None and float(item["price"]) > 0 and item["short_description"].strip():
                c.execute("UPDATE market_listings SET listing_active=1 WHERE id=?", (listing_id,))
                published = True
            elif not publish and item:
                c.execute("UPDATE market_listings SET listing_active=0 WHERE id=?", (listing_id,))
                published = False
            else:
                published = False
        if publish and not published:
            await q.edit_message_text("Cannot publish yet. First approve the listing, set a positive price, and add a short description.", reply_markup=kb([[("⬅️ Edit listing",f"admin_market_item_{listing_id}")]])); return
        await q.edit_message_text("✅ Listing published." if published else "✅ Listing hidden.", reply_markup=kb([[("⬅️ Listing details",f"admin_market_item_{listing_id}")],[("⬅️ All listings","admin_marketplace")]]))
    elif action.startswith("admin_market_message_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        parts = action.split("_")
        try: listing_id = int(parts[3]); recipient_role = parts[4]
        except (ValueError,IndexError):
            await q.edit_message_text("Invalid message action."); return
        if recipient_role not in ("seller","buyer"):
            await q.edit_message_text("Invalid recipient."); return
        with db() as c:
            item = c.execute("SELECT user_id,buyer_user_id FROM market_listings WHERE id=?", (listing_id,)).fetchone()
        target_user = (item["user_id"] if recipient_role == "seller" else item["buyer_user_id"]) if item else None
        if not target_user:
            await q.edit_message_text("This listing has no such recipient.", reply_markup=kb([[("⬅️ Listing details",f"admin_market_item_{listing_id}")]])); return
        set_pending(uid,"admin_market_message",{"listing_id":listing_id,"target_user_id":target_user,"recipient_role":recipient_role})
        await q.edit_message_text(f"💬 Send a text message or upload a photo/document to the {recipient_role} of listing #{listing_id}.", reply_markup=kb([[("❌ Cancel",f"admin_market_item_{listing_id}")]]))
    elif action.startswith("admin_market_purchase_approve_") or action.startswith("admin_market_purchase_reject_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        approved = action.startswith("admin_market_purchase_approve_")
        prefix = "admin_market_purchase_approve_" if approved else "admin_market_purchase_reject_"
        try: listing_id = int(action.removeprefix(prefix))
        except ValueError:
            await q.edit_message_text("Invalid purchase."); return
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND action='sell_social' AND purchase_status='receipt_submitted'", (listing_id,)).fetchone()
            if item:
                if approved:
                    c.execute("UPDATE market_listings SET purchase_status='sold',listing_active=0 WHERE id=?", (listing_id,))
                else:
                    c.execute("UPDATE market_listings SET purchase_status='available',buyer_user_id=NULL,payment_method='' WHERE id=?", (listing_id,))
        if not item:
            await q.edit_message_text("This purchase is no longer awaiting review.", reply_markup=kb([[("⬅️ Social account listings","admin_marketplace")]])); return
        if approved:
            buyer_message = f"✅ Payment verified for social account #{listing_id}. An admin will contact you about the next steps for the account transfer. Please do not share passwords or one-time codes in chat."
            seller_message = f"🎉 Your social account listing #{listing_id} has a buyer and the payment was marked verified by an admin. An admin will contact you about the transfer."
        else:
            buyer_message = f"⚠️ Payment for social account #{listing_id} could not be approved. Please contact an admin before trying again."
            seller_message = f"ℹ️ The purchase attempt for your social account listing #{listing_id} was not approved. The listing is available again."
        for target_uid, notice in ((item["buyer_user_id"],buyer_message),(item["user_id"],seller_message)):
            if target_uid:
                try: await context.bot.send_message(target_uid,notice)
                except Exception: log.warning("Could not notify user %s about marketplace purchase %s",target_uid,listing_id)
        await q.edit_message_text("✅ Purchase marked verified. Listing marked sold." if approved else "Purchase rejected; listing is available again.", reply_markup=kb([[("👤 Listing details",f"admin_market_item_{listing_id}")],[("⬅️ All listings","admin_marketplace")]]))
    elif action.startswith("admin_market_receipt_"):
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        try: listing_id = int(action.removeprefix("admin_market_receipt_"))
        except ValueError:
            await q.edit_message_text("Invalid listing."); return
        with db() as c:
            item = c.execute("SELECT receipt_file_id,receipt_type,buyer_user_id,price,payment_method FROM market_listings WHERE id=?", (listing_id,)).fetchone()
        if not item or not item["receipt_file_id"]:
            await q.edit_message_text("No receipt saved for this listing."); return
        caption = f"SOCIAL ACCOUNT PURCHASE #{listing_id}\nBuyer: {item['buyer_user_id']}\nPrice: {item['price']} ETB\nPayment: {item['payment_method']}\nVerify payment independently."
        if item["receipt_type"] == "photo":
            await context.bot.send_photo(uid,item["receipt_file_id"],caption=caption)
        else:
            await context.bot.send_document(uid,item["receipt_file_id"],caption=caption)
        await q.edit_message_text("🧾 Receipt sent to your admin chat.", reply_markup=kb([[("⬅️ Listing details",f"admin_market_item_{listing_id}")]]))
    elif action == "admin_queue":
        if not is_admin(uid): return
        with db() as c:
            ads = c.execute("SELECT id,user_id,kind,status FROM ad_requests WHERE status IN ('pending','receipt_submitted') ORDER BY id LIMIT 8").fetchall()
            wds = c.execute("SELECT id,user_id,points,status FROM withdrawals WHERE status='pending' ORDER BY id LIMIT 8").fetchall()
            mks = c.execute("SELECT id,user_id,action,asset_type,status FROM market_listings WHERE status='pending' ORDER BY id LIMIT 8").fetchall()
        rows = []
        for x in ads: rows.append([(f"Approve ad #{x['id']} · user {x['user_id']}",f"approve_ad_{x['id']}"),( "Reject",f"reject_ad_{x['id']}")])
        for x in wds:
            rows.append([(f"Approve withdrawal #{x['id']} · {x['points']} pts",f"approve_wd_{x['id']}"),("Reject",f"reject_wd_{x['id']}")])
            rows.append([("📨 Send payout message / proof",f"withdraw_delivery_{x['id']}")])
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
    # Advertiser submissions and replies accept photos, videos, animations, and documents.
    with db() as c:
        current_pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if current_pending and current_pending["action"] in {"promoter_user_chat","admin_promoter_chat_reply"}:
        chat_action=current_pending["action"]
        state=decode_pending(current_pending["data"])
        caption=(message.caption or "").strip()
        media_type=None; file_id=None
        if message.photo: media_type="photo"; file_id=message.photo[-1].file_id
        elif message.document: media_type="document"; file_id=message.document.file_id
        elif message.video: media_type="video"; file_id=message.video.file_id
        else:
            await message.reply_text("Please send an image/photo, video, or document, optionally with a caption."); return
        if chat_action=="promoter_user_chat":
            for aid in sorted(ADMIN_IDS):
                try:
                    note=f"💬 PROMOTER MEDIA MESSAGE\nFrom: {user.first_name or 'Promoter'} (@{user.username or 'no_username'})\nUser ID: {user.id}\nCaption: {caption[:450] or '(no caption)'}"
                    markup=kb([[( "↩️ Reply to promoter",f"admin_promoter_chat_reply_{user.id}")]])
                    if media_type=="photo": await context.bot.send_photo(aid,file_id,caption=note[:1024],reply_markup=markup)
                    elif media_type=="document": await context.bot.send_document(aid,file_id,caption=note[:1024],reply_markup=markup)
                    else: await context.bot.send_video(aid,file_id,caption=note[:1024],reply_markup=markup)
                except Exception: log.warning("Could not forward promoter media from %s to admin %s",user.id,aid)
            await message.reply_text("✅ Media sent to the admin team. You can send another message or /done to finish.")
            return
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        target=int(state.get("user_id",0) or 0)
        if not target:
            await message.reply_text("Promoter recipient not found."); return
        try:
            note=("📩 Photo from KefiaETBot admin" if media_type=="photo" else "📩 File from KefiaETBot admin")
            if caption: note+="\n\n"+caption[:850]
            if media_type=="photo": await context.bot.send_photo(target,file_id,caption=note[:1024])
            elif media_type=="document": await context.bot.send_document(target,file_id,caption=note[:1024])
            else: await context.bot.send_video(target,file_id,caption=note[:1024])
            await message.reply_text("✅ Media reply sent. Send another message or /done to finish.")
        except Exception:
            await message.reply_text("Could not deliver the media. The promoter may have blocked the bot.")
        return

    if current_pending and current_pending["action"] in {"advertiser_content","advertiser_user_message","admin_ad_message"}:
        pending_action=current_pending["action"]; state=decode_pending(current_pending["data"])
        request_id=int(state.get("request_id",0) or 0); target=int(state.get("target_user_id",0) or 0)
        caption=(message.caption or "").strip(); media_type=None; file_id=None
        if message.photo: media_type="photo"; file_id=message.photo[-1].file_id
        elif message.video: media_type="video"; file_id=message.video.file_id
        elif message.document: media_type="document"; file_id=message.document.file_id
        elif message.animation: media_type="animation"; file_id=message.animation.file_id
        else:
            await message.reply_text("Please send a photo, video, PDF, APK or another document."); return
        if pending_action=="advertiser_content":
            if not setting_enabled("advertiser_active",True):
                with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
                await message.reply_text(str(setting_value("advertiser_unavailable_message","Advertiser service is temporarily unavailable."))); return
            details={"product_type":state.get("product_type","Other"),"duration":state.get("duration",""),"price":state.get("price"),"content_type":media_type,"content_text":caption,"file_id":file_id}
            with db() as c:
                cur=c.execute("INSERT INTO ad_requests(user_id,kind,details,duration,quoted_price,status,created_at) VALUES(?,?,?,?,?,'pending',?)",(user.id,details["product_type"],json.dumps(details,ensure_ascii=False),details["duration"],float(details.get("price") or 0),now()))
                request_id=cur.lastrowid
                c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
            await message.reply_text(f"✅ Advertiser request #{request_id} submitted. An admin will review the content and reply here.",reply_markup=kb([[( "💬 Message admin",f"advertiser_chat_{request_id}")],[( "📋 My ad requests","my_ads")]]))
            admin_caption=f"📣 ADVERTISER REQUEST #{request_id}\nUser: {user.id} (@{user.username or 'no_username'})\nType: {details['product_type']}\nDuration: {details['duration']}\nPrice: {details['price']} ETB\nCaption: {caption[:500] or '(none)'}"
            for aid in ADMIN_IDS:
                try:
                    markup=kb([[( "🔎 Review advertiser request",f"advertiser_request_{request_id}")]])
                    if media_type=="photo": await context.bot.send_photo(aid,file_id,caption=admin_caption[:1024],reply_markup=markup)
                    elif media_type=="video": await context.bot.send_video(aid,file_id,caption=admin_caption[:1024],reply_markup=markup)
                    elif media_type=="document": await context.bot.send_document(aid,file_id,caption=admin_caption[:1024],reply_markup=markup)
                    else: await context.bot.send_animation(aid,file_id,caption=admin_caption[:1024],reply_markup=markup)
                except Exception: log.warning("Could not forward advertiser request %s",request_id)
            return
        if pending_action=="advertiser_user_message":
            with db() as c: owned=c.execute("SELECT id FROM ad_requests WHERE id=? AND user_id=?",(request_id,user.id)).fetchone()
            if not owned: await message.reply_text("That advertiser request was not found."); return
            for aid in ADMIN_IDS:
                try:
                    note=f"💬 ADVERTISER MESSAGE · request #{request_id}\nFrom user {user.id}\n{caption[:500]}"
                    if media_type=="photo": await context.bot.send_photo(aid,file_id,caption=note[:1024])
                    elif media_type=="video": await context.bot.send_video(aid,file_id,caption=note[:1024])
                    elif media_type=="document": await context.bot.send_document(aid,file_id,caption=note[:1024])
                    else: await context.bot.send_animation(aid,file_id,caption=note[:1024])
                except Exception: log.warning("Could not forward advertiser message")
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
            await message.reply_text("✅ Message sent to the admin.")
            return
        if pending_action=="admin_ad_message":
            if not is_admin(user.id): await message.reply_text("⛔ Admin access only."); return
            if not target: await message.reply_text("Advertiser recipient not found."); return
            prefix=f"📩 Admin reply about advertiser request #{request_id}\n"
            try:
                if media_type=="photo": await context.bot.send_photo(target,file_id,caption=(prefix+caption)[:1024])
                elif media_type=="video": await context.bot.send_video(target,file_id,caption=(prefix+caption)[:1024])
                elif media_type=="document": await context.bot.send_document(target,file_id,caption=(prefix+caption)[:1024])
                else: await context.bot.send_animation(target,file_id,caption=(prefix+caption)[:1024])
                with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
                await message.reply_text("✅ Reply delivered to advertiser.",reply_markup=kb([[( "🔎 View request",f"advertiser_request_{request_id}")],[( "📥 Advertiser requests","advertiser_admin_requests")]]))
            except Exception:
                log.exception("Could not deliver advertiser media reply")
                await message.reply_text("Could not deliver the media to the advertiser.")
            return

    # Admin can send a text/photo/document response to a promoter submission.
    with db() as c:
        current_pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if is_admin(user.id) and current_pending and current_pending["action"] == "promoter_ads_admin_message":
        state = decode_pending(current_pending["data"])
        target_user = int(state.get("target_user_id",0))
        submission_id = int(state.get("submission_id",0))
        caption_text = (message.caption or "").strip()
        if not target_user:
            await message.reply_text("The promoter recipient was not found."); return
        try:
            note = caption_text
            if message.photo:
                await context.bot.send_photo(target_user, message.photo[-1].file_id,
                    caption=("📩 Message from KefiaETBot admin about your promoter submission.\n\n"+caption_text)[:1024])
            elif message.document:
                await context.bot.send_document(target_user, message.document.file_id,
                    caption=("📩 Message from KefiaETBot admin about your promoter submission.\n\n"+caption_text)[:1024])
            else:
                await message.reply_text("Please send a photo or document, optionally with a caption."); return
            with db() as c:
                c.execute("UPDATE promoter_ad_submissions SET admin_note=?,updated_at=? WHERE id=?", (note or "Admin sent media message",now(),submission_id))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Media message delivered to promoter.", reply_markup=kb([[("📋 View submission",f"promoter_ads_view_{submission_id}")],[("⬅️ Submissions","promoter_ads_review")]]))
        except Exception:
            log.exception("Could not send promoter submission media")
            await message.reply_text("Could not deliver the media. The user may have blocked the bot.")
        return

    # Admin-managed Invite & Earn unavailable notice image.
    if is_admin(user.id) and current_pending and current_pending["action"] == "admin_invite_unavailable_photo":
        if not message.photo:
            await message.reply_text("Please send the notice as a photo, optionally with a caption. Text-only notices can be set from the Invite & Earn Management menu."); return
        caption = (message.caption or "").strip()
        if len(caption) > 1000:
            await message.reply_text("Keep the image caption under 1,000 characters."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_unavailable_photo',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (message.photo[-1].file_id,))
            if caption:
                c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_unavailable_message',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (caption,))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Unavailable notice image saved." + (" Its caption will be shown with the image." if caption else " The current notice text will be used as its caption."), reply_markup=kb([[("🔗 Invite & Earn Management","admin_invite_earn")]]))
        return

    # Admin can send a photo/document to a marketplace buyer or seller from the listing controls.
    with db() as c:
        current_pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if is_admin(user.id) and current_pending and current_pending["action"] == "admin_market_message":
        state = decode_pending(current_pending["data"])
        target_user = int(state.get("target_user_id",0))
        listing_id = int(state.get("listing_id",0))
        if not target_user:
            await message.reply_text("The message recipient was not found."); return
        try:
            if message.photo:
                file_id=message.photo[-1].file_id
                await context.bot.send_photo(target_user,file_id,caption=f"📩 Message from KefiaETBot admin about social account listing #{listing_id}\n{message.caption or ''}")
                msg_type="photo"
            elif message.document:
                file_id=message.document.file_id
                await context.bot.send_document(target_user,file_id,caption=f"📩 Message from KefiaETBot admin about social account listing #{listing_id}\n{message.caption or ''}")
                msg_type="document"
            elif value:
                file_id=""
                await context.bot.send_message(target_user,f"📩 Message from KefiaETBot admin about social account listing #{listing_id}:\n\n{value}")
                msg_type="text"
            else:
                await message.reply_text("Please send text, a photo, or a document."); return
            with db() as c:
                c.execute("INSERT INTO marketplace_messages(listing_id,user_id,sender_role,message_type,message_text,file_id,created_at) VALUES(?,?,'admin',?,?,?,?)",(listing_id,target_user,msg_type,value or message.caption or "",file_id,now()))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Media message sent.", reply_markup=kb([[("⬅️ Listing details",f"admin_market_item_{listing_id}")]]))
        except Exception:
            await message.reply_text("Could not deliver the media. The user may have blocked the bot.")
        return

    if current_pending and current_pending["action"] == "account_buyer_message":
        state=decode_pending(current_pending["data"])
        try: listing_id=int(state.get("listing_id",0))
        except (TypeError,ValueError): listing_id=0
        with db() as c:
            item=c.execute("SELECT id FROM market_listings WHERE id=? AND buyer_user_id=? AND action='sell_social'",(listing_id,user.id)).fetchone()
        if not item:
            await message.reply_text("This purchase was not found."); return
        if message.photo: msg_type,file_id="photo",message.photo[-1].file_id
        elif message.document: msg_type,file_id="document",message.document.file_id
        elif value: msg_type,file_id="text",""
        else:
            await message.reply_text("Send text, a photo, or a document."); return
        with db() as c:
            c.execute("INSERT INTO marketplace_messages(listing_id,user_id,sender_role,message_type,message_text,file_id,created_at) VALUES(?,?,'buyer',?,?,?,?)",(listing_id,user.id,msg_type,value or message.caption or "",file_id,now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
        note=f"💬 BUYER MESSAGE · Social account #{listing_id}\nUser ID: {user.id}\n"+(value or message.caption or "[attachment]")
        for aid in ADMIN_IDS:
            try:
                if msg_type=="photo": await context.bot.send_photo(aid,file_id,caption=note[:1024])
                elif msg_type=="document": await context.bot.send_document(aid,file_id,caption=note[:1024])
                else: await context.bot.send_message(aid,note)
                await context.bot.send_message(aid,"Reply from the Social account listings dashboard.",reply_markup=kb([[("🛍 Open account order",f"admin_market_item_{listing_id}")]]))
            except Exception: log.warning("Could not forward social account buyer message to admin %s",aid)
        await message.reply_text("✅ Message sent. You can return to My account purchases to read the admin's reply.",reply_markup=kb([[("🧾 My account purchases","account_my_purchases")],[("🏠 Dashboard","home")]]))
        return

    # Buyer payment proof for a social account listing.
    if current_pending and current_pending["action"] == "account_buy_receipt":
        state = decode_pending(current_pending["data"])
        try: listing_id = int(state.get("listing_id",0))
        except (TypeError,ValueError): listing_id = 0
        if message.photo:
            receipt_file_id, receipt_type = message.photo[-1].file_id, "photo"
        elif message.document:
            receipt_file_id, receipt_type = message.document.file_id, "document"
        else:
            await message.reply_text("📸 Please upload a payment screenshot as a photo or document."); return
        with db() as c:
            item = c.execute("SELECT * FROM market_listings WHERE id=? AND buyer_user_id=? AND purchase_status='awaiting_payment'", (listing_id,user.id)).fetchone()
            if not item:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("This purchase is no longer awaiting payment proof. Please browse available listings again."); return
            c.execute("UPDATE market_listings SET receipt_file_id=?,receipt_type=?,purchase_status='receipt_submitted' WHERE id=? AND buyer_user_id=?",
                      (receipt_file_id,receipt_type,listing_id,user.id))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        with db() as c:
            c.execute("INSERT INTO marketplace_messages(listing_id,user_id,sender_role,message_type,message_text,file_id,created_at) VALUES(?,?,'buyer','receipt',?,?,?)",(listing_id,user.id,message.caption or "Payment proof submitted",receipt_file_id,now()))
        await message.reply_text(f"✅ Payment screenshot submitted for account #{listing_id}. Please wait for admin approval. Return to Marketplace → My account purchases to view updates and messages.")
        caption = (f"SOCIAL ACCOUNT PURCHASE #{listing_id}\nBuyer: {user.id}\nSeller: {item['user_id']}\n"
                   f"Platform: {item['asset_type']}\nPrice: {item['price']} ETB\nPayment: {item['payment_method']}\n"
                   "Status: receipt submitted — verify the actual transfer before approving.")
        for aid in ADMIN_IDS:
            try:
                markup = kb([[("🔎 Review purchase",f"admin_market_item_{listing_id}")]])
                if receipt_type == "photo":
                    await context.bot.send_photo(aid,receipt_file_id,caption=caption,reply_markup=markup)
                else:
                    await context.bot.send_document(aid,receipt_file_id,caption=caption,reply_markup=markup)
            except Exception:
                log.warning("Could not forward social account receipt %s to admin %s",listing_id,aid)
        await notify_admins(context,f"🧾 Payment screenshot received for social account purchase #{listing_id}. Buyer: {user.id}. Verify the transfer independently.")
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
            await message.reply_text(digital_template("digital_receipt_upload_prompt", "📸 Please upload a clear receipt screenshot as a photo or document."))
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

    if pending and pending["action"] == "withdraw_delivery":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(pending["data"])
        withdrawal_id = int(state.get("withdrawal_id", 0))
        target_uid = int(state.get("target_user_id", 0))
        with db() as c:
            wd = c.execute("SELECT status FROM withdrawals WHERE id=? AND user_id=?", (withdrawal_id, target_uid)).fetchone()
        if not wd or wd["status"] not in ("pending", "approved"):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("This withdrawal is no longer awaiting delivery."); return
        caption = f"💸 Payout update for withdrawal #{withdrawal_id}"
        try:
            if message.photo:
                await context.bot.send_photo(target_uid, message.photo[-1].file_id, caption=caption + (f"\n{message.caption}" if message.caption else ""))
            elif message.document:
                await context.bot.send_document(target_uid, message.document.file_id, caption=caption + (f"\n{message.caption}" if message.caption else ""))
            else:
                await message.reply_text("Please send a photo or document, optionally with a caption."); return
        except Exception:
            log.exception("Could not deliver payout proof for withdrawal %s", withdrawal_id)
            await message.reply_text("Could not deliver the file to the user. The request remains open."); return
        with db() as c:
            c.execute("UPDATE withdrawals SET status='approved' WHERE id=? AND status='pending'", (withdrawal_id,))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(f"✅ Payout proof delivered to user for withdrawal #{withdrawal_id}.")
        return

    if pending and pending["action"] in ("sell_user_chat", "sell_admin_chat"):
        state = decode_pending(pending["data"])
        action = pending["action"]
        order_id = int(state.get("order_id", 0))
        if action == "sell_user_chat":
            with db() as c:
                order = c.execute("SELECT id FROM crypto_orders WHERE id=? AND user_id=? AND side='sell'", (order_id, user.id)).fetchone()
            if not order:
                await message.reply_text("Sell USDT order not found."); return
            note = f"💬 SELL USDT USER MESSAGE · order #{order_id}\nUser ID: {user.id}\n{message.caption or ''}"
            delivered = False
            for aid in ADMIN_IDS:
                try:
                    if message.photo:
                        await context.bot.send_photo(aid, message.photo[-1].file_id, caption=note[:1024])
                    elif message.document:
                        await context.bot.send_document(aid, message.document.file_id, caption=note[:1024])
                    else:
                        continue
                    await context.bot.send_message(aid, "Reply to user:", reply_markup=kb([[("💬 Reply to user", f"sell_admin_reply_{order_id}")], [("📋 Open order", f"sell_admin_order_{order_id}")]]))
                    delivered = True
                except Exception:
                    log.exception("Could not forward Sell USDT media to admin")
            if not delivered:
                await message.reply_text("Could not send the file to admin."); return
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ File sent to admin. Replies will arrive here."); return
        if action == "sell_admin_chat":
            target = int(state.get("target_user_id", 0))
            try:
                if message.photo:
                    await context.bot.send_photo(target, message.photo[-1].file_id, caption=f"📩 Admin · Sell USDT order #{order_id}\n{message.caption or ''}"[:1024])
                elif message.document:
                    await context.bot.send_document(target, message.document.file_id, caption=f"📩 Admin · Sell USDT order #{order_id}\n{message.caption or ''}"[:1024])
                else:
                    await message.reply_text("Send a photo or document."); return
            except Exception:
                await message.reply_text("Could not deliver the file to the user."); return
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Reply delivered to user."); return

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
                                   gateway=buy_usdt_gateway(order["payment_method"] or ""))
            vals["rate"] = f"{order['rate_etb']:g}"
            await message.reply_text(render_digital_template(setting_value("buy_usdt_confirmation_template", ""), vals))
            caption = render_digital_template(setting_value("buy_usdt_admin_order_template", ""), vals)
        else:
            confirmation=setting_value("sell_confirmation_message","✅ Screenshot received for Sell USDT order #{order_id}. Admin will verify the transfer and message you here.")
            await message.reply_text(render_digital_template(confirmation,{"order_id":order_id,"amount":f"{order['amount_usdt']:g}","rate":f"{order['rate_etb']:g}","total":f"{order['total_etb']:g}"}),reply_markup=kb([[("💬 Message admin about this order",f"sell_user_chat_{order_id}")]]))
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
                markup = kb([[("🔎 Review order", f"sell_admin_order_{order_id}" if order["side"] == "sell" else f"crypto_order_view_{order_id}")]])
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
    allowed = {"min_withdraw_points","usdt_etb_rate","price_ad_product","price_ad_members","price_ad_views","referral_points",
               "force_join_channel","force_join_url"}
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
    clearable_detail = (key.startswith(("buy_payment_", "sell_network_")) and key.endswith(("_details", "_destination"))) or key == "force_join_url"
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


async def handle_task_message_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save an admin-supplied image, video, APK, or PDF for a task's user-facing message."""
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not is_admin(user.id):
        return
    with db() as c:
        pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if not pending or pending["action"] != "admin_task_message_template":
        return
    state = decode_pending(pending["data"])
    try:
        tid = int(state.get("task_id", 0))
    except (TypeError, ValueError):
        tid = 0
    with db() as c:
        task = c.execute("SELECT id,title FROM tasks WHERE id=?", (tid,)).fetchone() if tid > 0 else None
    if not task:
        await message.reply_text("This task edit expired. Open Manage Tasks and try again.")
        return
    file_id = ""
    kind = ""
    if message.photo:
        file_id, kind = message.photo[-1].file_id, "photo"
    elif message.video:
        file_id, kind = message.video.file_id, "video"
    elif message.document:
        document = message.document
        filename = (document.file_name or "").lower()
        mime = (document.mime_type or "").lower()
        if filename.endswith((".apk", ".pdf")) or mime in {"application/vnd.android.package-archive", "application/pdf"}:
            kind, file_id = "document", document.file_id
        else:
            await message.reply_text("Please attach an image, video, APK, or PDF file.")
            return
    else:
        await message.reply_text("Please attach an image, video, APK, or PDF file.")
        return
    media_info = {
        "kind": kind,
        "file_id": file_id,
        "caption": (message.caption or "").strip()[:1024],
        "file_name": message.document.file_name if message.document else None,
    }
    with db() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                  (f"task_message_media_{tid}", json.dumps(media_info, ensure_ascii=False)))
    await message.reply_text(
        f"✅ Media attached to task #{tid} — {task['title']}. Users will see it when they open the task. "
        "Send another supported file to replace it, send text to update the message template, or press Done to finish.",
        reply_markup=kb([[("✅ Done", f"admintask_message_done_{tid}")],
                         [("Cancel", f"admintask_message_cancel_{tid}")]])
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message or not message.text: return
    value = message.text.strip()

    # Handle Contact Admin username first so unrelated admin command handlers
    # cannot consume or interrupt this pending one-setting edit.
    try:
        with db() as c:
            contact_pending = c.execute(
                "SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)
            ).fetchone()
        if contact_pending and contact_pending["action"] == "admin_setting_value" and str(contact_pending["data"] or "") == "contact_admin_username":
            if not is_admin(user.id):
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("⛔ Admin access only.")
                return
            contact_value = value.strip()
            if contact_value.lower() in {"off", "none", "disabled"}:
                contact_value = ""
            else:
                contact_value = contact_value.removeprefix("@")
                if contact_value.lower().startswith(("https://t.me/", "http://t.me/")):
                    contact_value = contact_value.split("t.me/", 1)[1].split("/", 1)[0].split("?", 1)[0]
                if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", contact_value):
                    await message.reply_text("Invalid Telegram username. Send @username or https://t.me/username, or send 'off' to hide Contact Admin.")
                    return
            with db() as c:
                c.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("contact_admin_username", contact_value)
                )
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                "✅ Contact Admin profile saved." if contact_value else "✅ Contact Admin profile link hidden.",
                reply_markup=kb([[("📞 Contact & Support","admin_setgroup_contact")], [("🛡 Admin Dashboard","admin")]])
            )
            return
    except Exception:
        log.exception("Failed to save Contact Admin Telegram username for admin %s", user.id)
        await message.reply_text("⚠️ I couldn't save the Contact Admin username because of a database error. Please check Render Logs and try again.")
        return

    # Admin can reply directly to the forwarded receipt in the private admin chat.
    if is_admin(user.id) and message.reply_to_message:
        caption = message.reply_to_message.caption or ""
        match = re.search(r"DIGITAL ORDER #([0-9]+)", caption)
        if match and value:
            await send_digital_admin_reply(update, context, int(match.group(1)), value, reject=False)
            return
        market_match = re.search(r"SOCIAL ACCOUNT PURCHASE #([0-9]+)", caption)
        if market_match and value:
            listing_id = int(market_match.group(1))
            with db() as c:
                item = c.execute("SELECT buyer_user_id FROM market_listings WHERE id=?", (listing_id,)).fetchone()
            if item and item["buyer_user_id"]:
                try:
                    await context.bot.send_message(item["buyer_user_id"], f"📩 Message from KefiaETBot admin about your social-account purchase #{listing_id}:\n\n{value}")
                    await message.reply_text("✅ Message sent to the buyer.")
                except Exception:
                    await message.reply_text("Could not deliver the message to the buyer.")
                return

    # Database-backed admin commands for products, gateways and editable user messages.
    if is_admin(user.id) and await handle_digital_admin_command(update, context, value):
        return

    with db() as c:
        p = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if not p:
        await message.reply_text("Use the dashboard buttons to get started.", reply_markup=home_keyboard(is_admin(user.id))); return
    action, data = p["action"], p["data"]
    if action == "advertiser_admin_setting":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state=decode_pending(data); field=state.get("field"); key=state.get("key")
        allowed={"rules":"advertiser_rules","channels":"advertiser_channels","payment":"advertiser_payment_info","unavailable":"advertiser_unavailable_message","price_day":"advertiser_price_day","price_week":"advertiser_price_week","price_month":"advertiser_price_month"}
        if field not in allowed or key!=allowed[field]:
            await message.reply_text("This advertiser setting session expired."); return
        saved=value
        if field.startswith("price_"):
            try:
                amount=float(value.replace(",",""))
                if amount<0 or not math.isfinite(amount): raise ValueError()
                saved="" if amount==0 else f"{amount:g}"
            except ValueError:
                await message.reply_text("Enter a valid non-negative ETB amount. Use 0 to hide this duration."); return
        if len(saved)>3500:
            await message.reply_text("Keep the setting under 3,500 characters."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,saved))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
        await message.reply_text("✅ Advertiser setting saved.",reply_markup=kb([[( "🛠 Advertiser management","advertiser_admin")],[( "⬅️ Admin Dashboard","admin")]]))
        return
    if action == "advertiser_content":
        state=decode_pending(data)
        if not setting_enabled("advertiser_active",True):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
            await message.reply_text(str(setting_value("advertiser_unavailable_message","Advertiser service is temporarily unavailable."))); return
        if not value or len(value)>3500:
            await message.reply_text("Send ad text between 1 and 3,500 characters, or upload a photo/video/document."); return
        details={"product_type":state.get("product_type","Other"),"duration":state.get("duration",""),"price":state.get("price"),"content_type":"text","content_text":value}
        with db() as c:
            cur=c.execute("INSERT INTO ad_requests(user_id,kind,details,duration,quoted_price,status,created_at) VALUES(?,?,?,?,?,'pending',?)",(user.id,details["product_type"],json.dumps(details,ensure_ascii=False),details["duration"],float(details.get("price") or 0),now()))
            request_id=cur.lastrowid
            c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
        await message.reply_text(f"✅ Advertiser request #{request_id} submitted. An admin will review it and reply here.",reply_markup=kb([[( "💬 Message admin",f"advertiser_chat_{request_id}")],[( "📋 My ad requests","my_ads")]]))
        await notify_admins(context,f"📣 ADVERTISER REQUEST #{request_id}\nUser: {user.id} (@{user.username or 'no_username'})\nType: {details['product_type']}\nDuration: {details['duration']}\nPrice: {details['price']} ETB\nContent: {value[:1800]}\nReview in Admin Dashboard → Advertiser Management.")
        return
    if action == "advertiser_user_message":
        state=decode_pending(data); request_id=int(state.get("request_id",0) or 0)
        with db() as c: owned=c.execute("SELECT id FROM ad_requests WHERE id=? AND user_id=?",(request_id,user.id)).fetchone()
        if not owned:
            await message.reply_text("That advertiser request was not found."); return
        await notify_admins(context,f"💬 ADVERTISER MESSAGE · request #{request_id}\nFrom user {user.id}\n{value[:2500]}")
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
        await message.reply_text("✅ Message sent to the admin. You can continue from My ad requests.")
        return
    if action == "admin_ad_message":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        state=decode_pending(data); target=int(state.get("target_user_id",0) or 0); request_id=int(state.get("request_id",0) or 0)
        if not target:
            await message.reply_text("Advertiser recipient not found."); return
        try:
            await context.bot.send_message(target,f"📩 Admin reply about advertiser request #{request_id}:\n\n{value}")
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
            await message.reply_text("✅ Reply sent to advertiser.",reply_markup=kb([[( "🔎 View request",f"advertiser_request_{request_id}")],[( "📥 Advertiser requests","advertiser_admin_requests")]]))
        except Exception:
            log.exception("Could not send advertiser reply")
            await message.reply_text("Could not deliver the message. The advertiser may have blocked the bot.")
        return
    if action == "admin_invite_unavailable_text":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        if value == "/cancel":
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("Notice editing cancelled.", reply_markup=kb([[("🔗 Invite & Earn Management","admin_invite_earn")]])); return
        if not value or len(value) > 3500:
            await message.reply_text("Send a notice between 1 and 3,500 characters, or /cancel."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_unavailable_message',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (value,))
            c.execute("INSERT INTO settings(key,value) VALUES('invite_earn_unavailable_photo','') ON CONFLICT(key) DO UPDATE SET value=''" )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Unavailable notice text saved. The image notice has been cleared.", reply_markup=kb([[("🔗 Invite & Earn Management","admin_invite_earn")]]))
        return
    if action == "admin_market_edit":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        listing_id = int(state.get("listing_id",0))
        field = state.get("field")
        if not listing_id or field not in {"price","description"}:
            await message.reply_text("This listing edit session expired."); return
        with db() as c:
            exists = c.execute("SELECT id FROM market_listings WHERE id=? AND action='sell_social'", (listing_id,)).fetchone()
        if not exists:
            await message.reply_text("Listing not found."); return
        if field == "price":
            try:
                price = float(value)
                if price <= 0 or price > 100000000: raise ValueError()
            except ValueError:
                await message.reply_text("Enter a positive price in ETB, for example 1500."); return
            with db() as c:
                c.execute("UPDATE market_listings SET price=? WHERE id=?", (price,listing_id))
        else:
            if len(value) > 350:
                await message.reply_text("Please keep the short description within 350 characters."); return
            with db() as c:
                c.execute("UPDATE market_listings SET short_description=? WHERE id=?", (value,listing_id))
        with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Listing updated. Publish it when the listing is approved, priced, and has a short description.", reply_markup=kb([[("🛍 Open listing",f"admin_market_item_{listing_id}")],[("⬅️ All listings","admin_marketplace")]]))
        return
    if action == "admin_market_message":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        target_user = int(state.get("target_user_id",0))
        listing_id = int(state.get("listing_id",0))
        if not target_user or not value:
            await message.reply_text("Message cannot be empty."); return
        try:
            await context.bot.send_message(target_user, f"📩 Message from KefiaETBot admin about social account listing #{listing_id}:\n\n{value}")
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Message sent.", reply_markup=kb([[("⬅️ Listing details",f"admin_market_item_{listing_id}")]]))
        except Exception:
            await message.reply_text("Could not deliver the message. The user may have blocked the bot.")
        return
    if action == "account_buy_receipt":
        await message.reply_text("📸 Please upload your payment screenshot as a photo or document so an admin can review it.")
        return
    if action == "admin_promoter_withdraw_limit_edit":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        try:
            minimum=int(value)
            if minimum<0 or minimum>100000000: raise ValueError()
        except ValueError:
            await message.reply_text("Send a whole number from 0 to 100,000,000. Use 0 to skip the point minimum."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('promoter_withdraw_min_points',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(minimum),))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
        await message.reply_text(f"Promoter withdrawal minimum saved: {minimum} points.",reply_markup=kb([[( "📣 Promoter Program","admin_promoters")]]))
        return
    if action == "promoter_withdraw_account_number":
        state=decode_pending(data); method=state.get("method","Telebirr")
        if method=="Telebirr":
            compact=value.replace(" ","").replace("-","")
            if not (re.fullmatch(r"09\d{8}",compact) or re.fullmatch(r"\+2519\d{8}",compact) or re.fullmatch(r"2519\d{8}",compact)):
                await message.reply_text("Invalid Telebirr number. Use 09XXXXXXXX or +2519XXXXXXXX."); return
            account_number=compact
        else:
            if not re.fullmatch(r"\d{13}",value):
                await message.reply_text("Invalid CBE account number. It must contain exactly 13 digits, numbers only."); return
            account_number=value
        set_pending(user.id,"promoter_withdraw_account_name",{"method":method,"account_number":account_number})
        await message.reply_text("👤 Enter the account holder's full name exactly as registered with the bank/Telebirr.")
        return
    if action == "promoter_withdraw_account_name":
        if len(value)<2 or len(value)>100:
            await message.reply_text("Please enter the account holder name (2–100 characters)."); return
        state=decode_pending(data); method=state.get("method","Telebirr"); account_number=state.get("account_number","")
        with db() as c:
            c.execute("UPDATE promoter_profiles SET method=?,account_number=?,account_name=?,updated_at=? WHERE user_id=?",(method,account_number,value,now(),user.id))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?",(user.id,))
            profile=c.execute("SELECT completed_count,target_count FROM promoter_profiles WHERE user_id=?",(user.id,)).fetchone()
            row=c.execute("SELECT points FROM users WHERE user_id=?",(user.id,)).fetchone()
        if not profile:
            await message.reply_text("Promoter profile not found. Please register first."); return
        points=int(row["points"] or 0) if row else 0
        target_required=setting_enabled("promoter_withdraw_require_target",True)
        minimum=max(0,int(setting_value("promoter_withdraw_min_points","1000") or 0))
        eligible=(not target_required or int(profile["completed_count"])>=int(profile["target_count"])) and points>=max(1,minimum)
        reply="✅ Your payout information is saved.\n\nMethod: "+method+"\nAccount: "+account_number+"\nAccount holder: "+value+"\n\n"
        if eligible:
            reply+="You currently meet the withdrawal rules. Tap Request withdrawal when you're ready."
            rows=[[( "💸 Request withdrawal","withdraw")],[( "📊 Promoter statistics","promoter_stats")],[( "⬅️ Promoter dashboard","promoter_stats")]]
        else:
            reply+="You can withdraw when you meet the configured point limit"+(" and referral target." if target_required else ".")
            rows=[[( "📊 View progress","promoter_stats")],[( "💸 Check withdrawal eligibility","promoter_withdraw_start")],[( "⬅️ Promoter dashboard","promoter_stats")]]
        await message.reply_text(reply,reply_markup=kb(rows))
        await notify_admins(context,f"PROMOTER PAYOUT DETAILS UPDATED\nUser: {user.id}\nMethod: {method}\nAccount: {account_number}\nHolder: {value}")
        return
    if action == "promoter_user_chat":
        if value.lower()=="/done":
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='promoter_user_chat'",(user.id,))
            await message.reply_text("Chat mode closed."); return
        for aid in sorted(ADMIN_IDS):
            try:
                await context.bot.send_message(aid,f"PROMOTER MESSAGE\nFrom: {user.first_name or 'Promoter'} (@{user.username or 'no_username'})\nUser ID: {user.id}\n\n{value[:3000]}",reply_markup=kb([[( "↩️ Reply to promoter",f"admin_promoter_chat_reply_{user.id}")]]))
            except Exception: log.warning("Could not forward promoter chat from %s to admin %s",user.id,aid)
        await message.reply_text("✅ Message sent to the admin team. Send another message or /done to finish.")
        return
    if action == "admin_promoter_chat_reply":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        if value.lower()=="/done":
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=? AND action='admin_promoter_chat_reply'",(user.id,))
            await message.reply_text("Reply mode closed."); return
        state=decode_pending(data); target=int(state.get("user_id",0) or 0)
        if not target:
            await message.reply_text("Promoter recipient not found."); return
        try:
            await context.bot.send_message(target,f"📩 Message from KefiaETBot admin:\n\n{value[:3500]}")
            await message.reply_text("✅ Reply sent. Send another message or /done to finish.")
        except Exception:
            await message.reply_text("Could not deliver the reply. The user may have blocked the bot.")
        return
    if action == "promoter_choose_method":
        await message.reply_text("Please tap Telebirr or CBE using the buttons shown above.")
        return
    if action == "promoter_account_number":
        if len(value) < 5 or len(value) > 40:
            await message.reply_text("Please enter a valid account/phone number (5–40 characters).")
            return
        state = decode_pending(data)
        set_pending(user.id, "promoter_account_name", {"method":state.get("method","Telebirr"),"account_number":value})
        await message.reply_text("👤 Now send the account holder's full name exactly as registered.")
        return
    if action == "promoter_account_name":
        if len(value) < 2 or len(value) > 100:
            await message.reply_text("Please enter the account holder's name (2–100 characters).")
            return
        state = decode_pending(data)
        method = state.get("method","Telebirr")
        account_number = state.get("account_number","")
        try:
            target = max(1,int(setting_value("promoter_target","100")))
            points_per_join = max(1,int(setting_value("promoter_points_per_join","1")))
        except (TypeError,ValueError):
            target, points_per_join = 100, 1
        channel = setting_value("promoter_channel","").strip()
        invite_link = ""
        status = "setup_pending"
        try:
            if channel:
                link_obj = await context.bot.create_chat_invite_link(chat_id=channel, name=f"kefia-promoter-{user.id}")
                invite_link = link_obj.invite_link
                status = "active"
        except Exception as exc:
            log.warning("Could not create promoter link for user %s: %s", user.id, exc)
        with db() as c:
            c.execute(
                "INSERT INTO promoter_profiles(user_id,method,account_number,account_name,status,target_count,points_per_join,completed_count,invite_link,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,0,?,?,?) ON CONFLICT(user_id) DO UPDATE SET method=excluded.method,account_number=excluded.account_number,account_name=excluded.account_name,status=excluded.status,target_count=excluded.target_count,points_per_join=excluded.points_per_join,invite_link=excluded.invite_link,updated_at=excluded.updated_at",
                (user.id,method,account_number,value,status,target,points_per_join,invite_link,now(),now())
            )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await notify_admins(context, f"📣 NEW PROMOTER REGISTRATION\nUser: {user.id} (@{user.username or 'no_username'})\nName: {user.first_name or '—'}\nPayout method: {method}\nAccount number: {account_number}\nAccount holder: {value}\nStatus: {status}\nTarget: {target} verified joins\nPoints per join: {points_per_join}\nInvite link: {invite_link or 'NOT CREATED — check channel permissions'}")
        if invite_link:
            await message.reply_text(
                f"🎉 Your promoter profile is saved!\n\n📡 Campaign target: {target} verified joins\n⭐ Reward: {points_per_join} points per verified join\n\n🔗 Your unique referral link:\n{invite_link}\n\nShare it with real people. Your dashboard tracks verified joins and points. Withdrawal unlocks when you reach the target and have points available.",
                reply_markup=kb([[("📊 My promoter progress","promoter_stats")],[("⬅️ Promotion Center","ads")]])
            )
        else:
            await message.reply_text("✅ Your payout details were saved and admins were notified, but the referral link could not be created yet. Please check My promoter progress later or contact support.", reply_markup=kb([[("📊 My promoter progress","promoter_stats")],[("⬅️ Promotion Center","ads")]]))
        return
    if action == "promoter_market_form":
        state = decode_pending(data)
        step = state.get("step")
        if not setting_enabled("promoter_ads_active"):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("Promoter submissions are temporarily unavailable.", reply_markup=kb([[("⬅️ Promotion Center","ads")]])); return
        if step == "content":
            if not value or len(value) > 500:
                await message.reply_text("Please enter a short content description (1–500 characters)."); return
            state["content_type"] = value
            state["step"] = "followers"
            set_pending(user.id, "promoter_market_form", state)
            await message.reply_text(str(setting_value("promoter_question_followers", "How many followers or subscribers do you have? Enter a whole number.")), reply_markup=kb([[("❌ Cancel","ads")]])); return
        if step == "followers":
            try:
                followers = int(value.replace(",","").replace(" ",""))
                if followers < 0: raise ValueError()
            except ValueError:
                await message.reply_text("Enter the follower/subscriber count as a whole number (0 or more)."); return
            state["followers"] = followers
            state["step"] = "link"
            set_pending(user.id, "promoter_market_form", state)
            await message.reply_text(str(setting_value("promoter_question_link", "Send your public channel or profile link.")), reply_markup=kb([[("❌ Cancel","ads")]])); return
        if step == "link":
            link = value.strip()
            if len(link) > 500 or not (link.startswith(("https://","http://","@"))):
                await message.reply_text("Send a public profile link beginning with https://, http://, or @username."); return
            state["channel_link"] = link
            state["step"] = "choose_price"
            set_pending(user.id, "promoter_market_form", state)
            await message.reply_text(str(setting_value("promoter_question_prices", "Set your advertising price in ETB for at least one period.")),
                reply_markup=kb([[("💵 Per day","promoter_market_price_day")],[("📅 Per week","promoter_market_price_week")],[("🗓 Per month","promoter_market_price_month")],[("❌ Cancel","ads")]])); return
        if step == "price_amount":
            try:
                amount = float(value.replace(",",""))
                if amount <= 0 or not math.isfinite(amount): raise ValueError()
            except ValueError:
                await message.reply_text("Enter a valid price greater than 0 ETB."); return
            period = state.get("price_period")
            if period not in {"day","week","month"}:
                await message.reply_text("Your price step expired. Please choose a period again."); return
            state.setdefault("prices", {})[period] = amount
            state.pop("price_period", None)
            state["step"] = "choose_price"
            set_pending(user.id, "promoter_market_form", state)
            prices = state["prices"]
            rows = []
            for p,label in [("day","💵 Per day"),("week","📅 Per week"),("month","🗓 Per month")]:
                if p not in prices: rows.append([(label,"promoter_market_price_"+p)])
            rows += [[("➕ Set another period","promoter_market_add_price")],[("✅ Submit for admin review","promoter_market_price_done")],[("❌ Cancel","ads")]]
            summary = "\n".join(f"{p.title()}: {v:g} ETB" for p,v in prices.items())
            await message.reply_text("✅ Price saved.\n\nYour prices:\n"+summary+"\n\nYou must set at least one period; set more if you want, or submit now.", reply_markup=kb(rows)); return
        await message.reply_text("Your promoter form step was not recognized. Please restart the form.", reply_markup=kb([[("📢 Start promoter form","promoter_market_start")]])); return
    if action == "promoter_ads_limit_edit":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        try:
            limit = int(value)
            if limit < 0: raise ValueError()
        except ValueError:
            await message.reply_text("Enter a whole number: 0 for unlimited, or a positive number for the maximum submissions."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES('promoter_ads_submission_limit',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(limit),))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(f"✅ Promoter submission limit set to {'unlimited' if limit == 0 else limit}.", reply_markup=kb([[("📣 Promoter Center","admin_promoters")]])); return
    if action == "promoter_ads_question_edit":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        field = state.get("field")
        if field not in {"platforms","content","followers","link","prices"} or not value or len(value) > 500:
            await message.reply_text("Send a question between 1 and 500 characters."); return
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", ("promoter_question_"+field,value))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Promoter form question updated.", reply_markup=kb([[("📝 Edit more questions","promoter_ads_form_settings")],[("⬅️ Promoter Center","admin_promoters")]])); return
    if action == "promoter_ads_admin_message":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        target_uid = int(state.get("target_user_id",0))
        submission_id = int(state.get("submission_id",0))
        if not target_uid or not value:
            await message.reply_text("Message cannot be empty."); return
        try:
            await context.bot.send_message(target_uid, "📩 Message from KefiaETBot admin about your promoter submission:\n\n"+value)
        except Exception:
            await message.reply_text("Could not deliver the message. The user may have blocked the bot."); return
        with db() as c:
            c.execute("UPDATE promoter_ad_submissions SET admin_note=?,updated_at=? WHERE id=?", (value,now(),submission_id))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Message sent to promoter.", reply_markup=kb([[("📋 View submission",f"promoter_ads_view_{submission_id}")],[("⬅️ Submissions","promoter_ads_review")]])); return
    if action == "admin_promoter_setting":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        field = state.get("field")
        if field not in {"rules","channel","target","points"}:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That promoter setting expired."); return
        saved = value
        if field in {"target","points"}:
            try:
                number = int(value)
                if number < 1: raise ValueError()
            except ValueError:
                await message.reply_text("Enter a positive whole number (1 or more). Please try again."); return
            saved = str(number)
        elif field == "rules":
            if len(value) > 3500:
                await message.reply_text("Keep the rules under 3,500 characters."); return
        elif field == "channel":
            try:
                chat = await context.bot.get_chat(value)
                member = await context.bot.get_chat_member(chat.id, context.bot.id)
                if chat.type != "channel" or member.status not in ("administrator","creator"):
                    await message.reply_text("The bot must be an administrator in a Telegram channel. Check the channel and try again."); return
                saved = str(chat.id)
            except Exception:
                await message.reply_text("I couldn't access that channel. Send its @username or numeric ID, and make sure the bot is an admin with invite-link permission."); return
        key = "promoter_"+field
        with db() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key,saved))
            if field == "target":
                c.execute("UPDATE promoter_profiles SET target_count=?,status=CASE WHEN completed_count>=? THEN 'completed' WHEN invite_link<>'' THEN 'active' ELSE 'setup_pending' END,updated_at=?", (int(saved),int(saved),now()))
            elif field == "points":
                c.execute("UPDATE promoter_profiles SET points_per_join=?,updated_at=?", (int(saved),now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(f"✅ Promoter setting saved: {field.replace('_',' ')}.\n\nWhat next?", reply_markup=kb([[("📣 Promoter Program","admin_promoters")],[("⬅️ Admin Dashboard","admin")]]))
        return
    if action == "admin_promoter_message":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        target_uid = int(state.get("user_id",0))
        if not target_uid:
            await message.reply_text("Promoter user was not found."); return
        try:
            await context.bot.send_message(target_uid, f"📩 Message from KefiaETBot admin:\n\n{value}")
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Message sent.", reply_markup=kb([[("👤 Promoter profile",f"admin_promoter_user_{target_uid}")],[("⬅️ Promoter users","admin_promoter_users")]]))
        except Exception:
            await message.reply_text("Could not deliver the message. The user may have blocked the bot.")
        return
    if action == "admin_promoter_edit_payout":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        target_uid = int(state.get("user_id",0))
        field = state.get("field")
        if field not in {"method","account_number","account_name"} or not target_uid:
            await message.reply_text("Payout edit session expired."); return
        saved = value
        if field == "method":
            normalized = value.strip().lower()
            if normalized not in {"cbe","telebirr"}:
                await message.reply_text("Enter either Telebirr or CBE."); return
            saved = "CBE" if normalized == "cbe" else "Telebirr"
        if field == "account_number" and not (5 <= len(value) <= 40):
            await message.reply_text("Enter an account/phone number between 5 and 40 characters."); return
        if field == "account_name" and not (2 <= len(value) <= 100):
            await message.reply_text("Enter a name between 2 and 100 characters."); return
        with db() as c:
            c.execute(f"UPDATE promoter_profiles SET {field}=?,updated_at=? WHERE user_id=?", (saved,now(),target_uid))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        try: await context.bot.send_message(target_uid, f"ℹ️ Your promoter payout {field.replace('_',' ')} was updated by an admin.")
        except Exception: pass
        await message.reply_text("✅ Payout details updated.", reply_markup=kb([[("👤 Promoter profile",f"admin_promoter_user_{target_uid}")],[("⬅️ Promoter users","admin_promoter_users")]]))
        return
    if action == "digital_receipt":
        await message.reply_text(digital_template(
            "digital_receipt_upload_prompt",
            "📸 Please upload a clear payment receipt screenshot as a photo or document."
        ))
        return
    if action == "digital_choose_gateway":
        await message.reply_text(digital_template(
            "digital_choose_gateway_prompt",
            "💳 Please tap one of the payment method buttons shown above."
        ))
        return
    if action == "digital_admin_reply":
        state = decode_pending(data)
        await send_digital_admin_reply(update, context, int(state.get("order_id", 0)), value,
                                       reject=bool(state.get("reject", False)))
        return
    if action == "digital_admin_wizard":
        if not is_admin(user.id):
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only.")
            return
        state = decode_pending(data)
        mode = state.get("mode")
        raw = value.strip()
        if not raw:
            await message.reply_text("Please send a value, or tap Cancel to leave this setup.")
            return
        if mode == "add_product":
            steps = [
                ("id","Product ID"), ("name","Product name"), ("duration_months","Duration in months"),
                ("price","Price in ETB"), ("stock","Available stock"), ("description","Customer-facing description"),
                ("features","Features (one per line)"), ("important_note","Important note"), ("notice","Notice"), ("warranty","Warranty text")
            ]
            step = state.get("step","id")
            fields = [x[0] for x in steps]
            if step not in fields:
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Setup expired. Please start again from Digital Products.")
                return
            vals = state.get("values",{})
            if step == "id":
                if not re.fullmatch(r"[A-Za-z0-9_]{2,16}", raw):
                    await message.reply_text("Use 2–16 English letters, numbers, or underscores. Example: gemini_pro_1m")
                    return
                if digital_product(raw):
                    await message.reply_text("That ID already exists. Choose another unique ID, or edit the existing product.")
                    return
                vals[step] = raw
            elif step == "name":
                if len(raw) > 100:
                    await message.reply_text("Keep the product name under 100 characters.")
                    return
                vals[step] = raw
            elif step in {"duration_months","stock"}:
                try:
                    number = int(raw)
                    if number < (1 if step == "duration_months" else 0):
                        raise ValueError()
                except ValueError:
                    await message.reply_text("Enter a whole number. Duration must be at least 1 month; stock can be 0 or more.")
                    return
                vals[step] = number
            elif step == "price":
                try:
                    number = Decimal(raw)
                    if not number.is_finite() or number < 0:
                        raise ValueError()
                except (ValueError, InvalidOperation):
                    await message.reply_text("Enter a valid price of 0 or more.")
                    return
                vals[step] = float(number)
            else:
                vals[step] = raw
            idx = fields.index(step)
            if idx < len(steps)-1:
                next_step, next_label = steps[idx+1]
                state.update({"step":next_step,"values":vals})
                set_pending(user.id, "digital_admin_wizard", state)
                prompts = {
                    "name":"Send the product name customers will see.",
                    "duration_months":"How many months does this product cover? Enter a whole number.",
                    "price":"What is the price in ETB? Enter a number.",
                    "stock":"How many units are available? Enter 0 if temporarily out of stock.",
                    "description":"Write the short description customers will see.",
                    "features":"List product features, one per line. Send — if you do not want to add features.",
                    "important_note":"Send any important note customers must know, or — to skip.",
                    "notice":"Send any warning/notice customers should see, or — to skip.",
                    "warranty":"Describe the warranty, or send — if there is none."
                }
                await message.reply_text(f"➕ Add product (step {idx+2} of {len(steps)})\n\n{prompts[next_step]}")
                return
            vals = {k: ("" if str(v).strip() == "—" else v) for k,v in vals.items()}
            with db() as c:
                c.execute(
                    "INSERT INTO digital_products(id,name,duration_months,price,stock,description,features,important_note,notice,warranty,active,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,0,?)",
                    (vals["id"], vals["name"], vals["duration_months"], vals["price"], vals["stock"],
                     vals.get("description",""), vals.get("features",""), vals.get("important_note",""),
                     vals.get("notice",""), vals.get("warranty",""), now())
                )
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                f"✅ Product “{vals['name']}” was created as a draft and is hidden from customers.\n\n"
                "Review its price, stock, and details, then tap Publish when it is ready.",
                reply_markup=kb([[("✏️ Manage this product",f"digital_admin_product_{vals['id']}")],
                                 [("➕ Add another product","digital_admin_add_product")],
                                 [("⬅️ Digital Products","admin_digital_products")]])
            )
            return
        if mode == "add_gateway":
            steps = [("id","Payment method ID"),("name","Name shown to customers"),("account_number","Account number or payment ID"),
                     ("account_name","Account holder name"),("instructions","Payment instructions"),("warning","Warning or extra information")]
            step = state.get("step","id")
            fields = [x[0] for x in steps]
            if step not in fields:
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Setup expired. Please start again.")
                return
            vals = state.get("values",{})
            if step == "id":
                if not re.fullmatch(r"[A-Za-z0-9_]{2,16}", raw):
                    await message.reply_text("Use 2–16 English letters, numbers, or underscores. Example: telebirr")
                    return
                with db() as c:
                    exists = c.execute("SELECT 1 FROM digital_payment_gateways WHERE id=?", (raw,)).fetchone()
                if exists:
                    await message.reply_text("That ID already exists. Choose another ID, or edit the existing method.")
                    return
                vals[step] = raw
            elif step == "name":
                if len(raw) > 80:
                    await message.reply_text("Keep the display name under 80 characters."); return
                vals[step] = raw
            else:
                vals[step] = "" if raw == "—" else raw
            idx = fields.index(step)
            if idx < len(steps)-1:
                next_step, next_label = steps[idx+1]
                state.update({"step":next_step,"values":vals})
                set_pending(user.id, "digital_admin_wizard", state)
                prompts = {
                    "name":"Send the payment method name customers should see.",
                    "account_number":"Send the payment account number, phone number, or payment ID.",
                    "account_name":"Send the account holder name, or — if not needed.",
                    "instructions":"Send the payment instructions customers should follow, or — to skip.",
                    "warning":"Send any extra warning customers should see, or — to skip."
                }
                await message.reply_text(f"➕ Add payment method (step {idx+2} of {len(steps)})\n\n{prompts[next_step]}")
                return
            with db() as c:
                c.execute(
                    "INSERT INTO digital_payment_gateways(id,name,account_number,account_name,instructions,warning,enabled,updated_at) "
                    "VALUES(?,?,?,?,?,?,0,?)",
                    (vals["id"],vals["name"],vals.get("account_number",""),vals.get("account_name",""),
                     vals.get("instructions",""),vals.get("warning",""),now())
                )
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                f"✅ Payment method “{vals['name']}” was added as disabled. Review its account details and enable it when everything is correct.",
                reply_markup=kb([[("✏️ Manage payment methods","digital_admin_gateways")],
                                 [("⬅️ Digital Products","admin_digital_products")]])
            )
            return
        if mode == "edit_product":
            product_id, field = state.get("product_id"), state.get("field")
            allowed = {"name","duration_months","price","stock","description","features","important_note","notice","warranty"}
            if field not in allowed or not product_id or not digital_product(product_id):
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Product or field not found. Please reopen the product settings.")
                return
            if field == "name":
                if len(raw) > 100: await message.reply_text("Keep the name under 100 characters."); return
                saved = raw
            elif field in {"duration_months","stock"}:
                try:
                    saved = int(raw)
                    if saved < (1 if field == "duration_months" else 0): raise ValueError()
                except ValueError:
                    await message.reply_text("Enter a whole number. Duration must be at least 1; stock must be 0 or more."); return
            elif field == "price":
                try:
                    parsed = Decimal(raw)
                    if not parsed.is_finite() or parsed < 0: raise ValueError()
                    saved = float(parsed)
                except (ValueError, InvalidOperation):
                    await message.reply_text("Enter a valid price of 0 or more."); return
            else:
                saved = "" if raw == "—" else raw
            with db() as c:
                c.execute(f"UPDATE digital_products SET {field}=?,updated_at=? WHERE id=?", (saved,now(),product_id))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                f"✅ Product updated. Customers will now see the new {field.replace('_',' ')}.",
                reply_markup=kb([[("⬅️ Manage product",f"digital_admin_product_{product_id}")],
                                 [("⬅️ All products","admin_digital_products")]])
            )
            return
        if mode == "edit_gateway":
            gateway_id, field = state.get("gateway_id"), state.get("field")
            if field not in {"name","account_number","account_name","instructions","warning"}:
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Payment field not found. Please reopen payment methods."); return
            with db() as c:
                gateway = c.execute("SELECT id FROM digital_payment_gateways WHERE id=?", (gateway_id,)).fetchone()
                if not gateway:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                    await message.reply_text("Payment method not found."); return
                c.execute(f"UPDATE digital_payment_gateways SET {field}=?,updated_at=? WHERE id=?", (raw,now(),gateway_id))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                f"✅ Payment method updated. Customers will now see the new {field.replace('_',' ')}.",
                reply_markup=kb([[("⬅️ Payment method settings",f"digital_admin_gateway_{gateway_id}")],
                                 [("⬅️ Payment methods","digital_admin_gateways")]])
            )
            return
        if mode == "edit_message":
            key = state.get("key")
            allowed = {
                "digital_product_details_template", "digital_payment_template", "digital_waiting_message",
                "digital_no_stock_message", "digital_no_gateway_message", "digital_invalid_payment_message", "digital_cancel_message",
                "digital_buy_button", "digital_cancel_button",
                "digital_market_button", "digital_choose_gateway_prompt", "digital_market_title",
                "digital_market_product_button_template", "digital_receipt_upload_prompt"
            }
            if key not in allowed:
                with db() as c:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Message setting not found. Please reopen Customer-facing text."); return
            if len(raw) > 3500:
                await message.reply_text("Please keep this message under 3,500 characters."); return
            with db() as c:
                c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key,raw))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                "✅ Customer-facing text saved. New and future customers will see this update.",
                reply_markup=kb([[("✏️ Edit another message","digital_admin_messages")],
                                 [("⬅️ Digital Products","admin_digital_products")]])
            )
            return
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("This setup step expired. Please start again from Digital Products.")
        return
    if action == "admin_setting_value":
        if not is_admin(user.id):
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only.")
            return
        key = str(data or "")
        allowed_keys = {
            "contact_admin_username",
            "price_ad_product", "price_ad_members", "price_ad_views",
            "min_withdraw_points", "referral_points", "points_per_birr", "invite_earn_message", "buy_usdt_rate",
            "buy_usdt_min", "buy_usdt_max", "buy_usdt_stock",
            "buy_usdt_bep20_min", "buy_usdt_bybit_min", "buy_usdt_processing_time",
        }
        if key not in allowed_keys:
            with db() as c:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That setting is no longer available. Please reopen Prices & Limits.")
            return
        if not value:
            await message.reply_text("Please send a value, or tap Cancel and choose another setting.")
            return
        if key == "invite_earn_message":
            message_value = value.strip()
            if not message_value or len(message_value) > 2500:
                await message.reply_text("Invite & Earn information must contain 1–2500 characters. Please try again.")
                return
            with db() as c:
                c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, message_value))
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("✅ Invite & Earn displayed information saved.", reply_markup=kb([[("🔗 Preview / Manage Invite & Earn","admin_invite_earn")],[("🛡 Admin Dashboard","admin")]]))
            return
        if key == "contact_admin_username":
            contact_value = value.strip()
            if contact_value.lower() in {"off", "none", "disabled"}:
                contact_value = ""
            else:
                contact_value = contact_value.removeprefix("@")
                if contact_value.lower().startswith("https://t.me/"):
                    contact_value = contact_value.split("t.me/", 1)[1].split("/", 1)[0].split("?", 1)[0]
                if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,31}", contact_value):
                    await message.reply_text("Invalid Telegram username. Send @username or https://t.me/username, or send 'off' to hide Contact Admin.")
                    return
            with db() as c:
                c.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, contact_value)
                )
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text(
                "✅ Contact Admin profile saved." if contact_value else "✅ Contact Admin profile link hidden.",
                reply_markup=kb([[("📞 Contact & Support","admin_setgroup_contact")], [("🛡 Admin Dashboard","admin")]])
            )
            return
        numeric_keys = {
            "price_ad_product", "price_ad_members", "price_ad_views",
            "referral_points", "points_per_birr", "buy_usdt_rate", "buy_usdt_min", "buy_usdt_max",
            "buy_usdt_stock", "buy_usdt_bep20_min", "buy_usdt_bybit_min",
        }
        if key in numeric_keys:
            try:
                number = Decimal(value)
                if not number.is_finite() or number < 0:
                    raise ValueError()
                if key in {"buy_usdt_rate", "buy_usdt_min", "points_per_birr"} and number == 0:
                    raise ValueError()
                if key == "buy_usdt_max" and number < Decimal(str(setting_value("buy_usdt_min", "1"))):
                    raise ValueError()
                if key == "referral_points" and number != number.to_integral_value():
                    raise ValueError()
            except (ValueError, InvalidOperation):
                await message.reply_text(
                    "That number is invalid. Enter a number greater than or equal to 0. "
                    "Buy rate, minimum order, and points-per-Birr conversion must be greater than 0; referral points must be a whole number. Try again."
                )
                return
        if key == "min_withdraw_points":
            try:
                if int(value) < 1:
                    raise ValueError()
            except ValueError:
                await message.reply_text("Minimum withdrawal points must be a positive whole number. Try again.")
                return
        if key == "buy_usdt_max":
            try:
                if Decimal(value) < Decimal(str(setting_value("buy_usdt_min", "1"))):
                    await message.reply_text("Maximum order cannot be lower than the current minimum order. Try again.")
                    return
            except InvalidOperation:
                await message.reply_text("Enter a valid maximum amount. Try again.")
                return
        if key == "buy_usdt_processing_time" and not value.strip():
            await message.reply_text("Please enter a short processing-time message.")
            return
        with db() as c:
            c.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value.strip())
            )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        group = (
            "ads" if key.startswith("price_ad_") else
            "rewards" if key in {"min_withdraw_points", "referral_points", "points_per_birr", "invite_earn_message"} else
            "buyusdt"
        )
        await message.reply_text(
            f"✅ Saved successfully.\n{key} = {value.strip()}\n\nWhat would you like to do next?",
            reply_markup=kb([
                [("✏️ Change another setting in this service", f"admin_setgroup_{group}")],
                [("⚙️ Other service groups","admin_settings")],
                [("🛡 Admin Dashboard","admin")]
            ])
        )
        return
    if await handle_crypto_text(update, context, action, data, value):
        return
    if action == "admin_task_message_template":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        try:
            tid = int(state.get("task_id", 0))
        except (TypeError, ValueError):
            tid = 0
        template = value.strip()
        marker = "{{PERSONAL_INVITE_LINK}}"
        if tid < 1:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That task edit expired. Open Manage Tasks and try again."); return
        if not template or len(template) > 3000:
            await message.reply_text("The template must contain 1–3000 characters. Please send it again."); return
        if template.count(marker) != 1:
            await message.reply_text(
                "The template must contain {{PERSONAL_INVITE_LINK}} exactly once. "
                "Keep that marker unchanged so every user receives their own personal invite link."
            ); return
        with db() as c:
            task = c.execute("SELECT id FROM tasks WHERE id=?", (tid,)).fetchone()
            if task:
                c.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"task_message_template_{tid}", template)
                )
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        if not task:
            await message.reply_text("Task not found. No template was saved."); return
        await message.reply_text(
            "✅ Customer-facing task message updated. The personal invite URL remains generated automatically for each user.",
            reply_markup=kb([[("📋 Review task",f"admintask_view_{tid}")],[("📋 Manage tasks","admin_tasks")]])
        )
        return
    if action == "admin_task_edit":
        if not is_admin(user.id):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        try:
            tid = int(state.get("task_id", 0))
            field = str(state.get("field", ""))
        except (TypeError, ValueError):
            tid, field = 0, ""
        allowed = {"title", "channel", "target", "points", "limit"}
        if tid < 1 or field not in allowed:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That task edit expired. Open Manage Tasks and try again."); return
        with db() as c:
            existing_task = c.execute("SELECT id,completed_count FROM tasks WHERE id=?", (tid,)).fetchone()
        if not existing_task:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("Task not found."); return
        if field == "title":
            if not value or len(value) > 120:
                await message.reply_text("Enter a task title between 1 and 120 characters."); return
            with db() as c:
                c.execute("UPDATE tasks SET title=? WHERE id=?", (value.strip(),tid))
        elif field == "channel":
            if not value.strip():
                await message.reply_text("Enter a channel @username or numeric channel ID."); return
            try:
                chat = await context.bot.get_chat(value.strip())
                bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
                if bot_member.status not in ("administrator", "creator"):
                    await message.reply_text("The bot must be an administrator in that channel. No changes were saved."); return
                test_link = await context.bot.create_chat_invite_link(chat.id, member_limit=1, name=f"KefiaETBot-edit-check-{tid}")
                await context.bot.revoke_chat_invite_link(chat.id, test_link.invite_link)
            except Exception:
                await message.reply_text("I couldn't access that channel or create invite links. Check the channel ID and bot permissions. No changes were saved."); return
            with db() as c:
                c.execute("UPDATE tasks SET channel=? WHERE id=?", (str(chat.id),tid))
        else:
            try:
                number = int(value)
                if number < 0 or (field in {"target","points"} and number < 1):
                    raise ValueError()
            except ValueError:
                await message.reply_text("Enter a valid whole number. Target and points must be at least 1; user limit may be 0 for unlimited."); return
            column = {"target":"target","points":"points","limit":"participant_limit"}[field]
            with db() as c:
                task = c.execute("SELECT completed_count FROM tasks WHERE id=?", (tid,)).fetchone()
                if not task:
                    c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                    await message.reply_text("Task not found."); return
                if field == "target" and number < int(task["completed_count"]):
                    await message.reply_text("Target cannot be lower than the number of verified joins already recorded."); return
                c.execute(f"UPDATE tasks SET {column}=? WHERE id=?", (number,tid))
                if field == "target":
                    c.execute("UPDATE tasks SET active=? WHERE id=?", (1 if number > int(task["completed_count"]) else 0,tid))
        with db() as c:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Task settings updated.", reply_markup=kb([[("📋 Review task",f"admintask_view_{tid}")],[("📋 Manage tasks","admin_tasks")]]))
        return
    if action == "admin_task_title":
        if not is_admin(user.id): return
        parts = [x.strip() for x in value.split("|")]
        if len(parts) not in (4, 5) or not parts[0] or not parts[1]:
            await update.effective_message.reply_text("Format: Task title | @channel | target joins | points | max users (optional; 0 = unlimited)"); return
        try:
            target, points = int(parts[2]), int(parts[3])
            participant_limit = int(parts[4]) if len(parts) == 5 else 0
            if target < 1 or points < 1 or participant_limit < 0: raise ValueError()
        except ValueError:
            await update.effective_message.reply_text("Target and points must be positive whole numbers; max users must be 0 or greater."); return
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
            c.execute("INSERT INTO tasks(title,channel,target,points,participant_limit,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                      (parts[0],str(chat.id),target,points,participant_limit,user.id,now()))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text("✅ Task created. It now appears in Daily Tasks.", reply_markup=kb([[("📋 Manage tasks","admin_tasks")],[("🛡 Admin Dashboard","admin")]]))
    elif action == "withdraw_delivery":
        if not is_admin(user.id):
            await message.reply_text("⛔ Admin access only."); return
        state = decode_pending(data)
        withdrawal_id = int(state.get("withdrawal_id", 0))
        target_uid = int(state.get("target_user_id", 0))
        with db() as c:
            wd = c.execute("SELECT status FROM withdrawals WHERE id=? AND user_id=?", (withdrawal_id, target_uid)).fetchone()
        if not wd or wd["status"] not in ("pending", "approved"):
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("This withdrawal is no longer awaiting delivery."); return
        try:
            await context.bot.send_message(target_uid, f"💸 Update for withdrawal #{withdrawal_id}:\n\n{value}")
        except Exception:
            await message.reply_text("Could not deliver the message to the user. The request remains open."); return
        with db() as c:
            c.execute("UPDATE withdrawals SET status='approved' WHERE id=? AND status='pending'", (withdrawal_id,))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(f"✅ Message delivered to user for withdrawal #{withdrawal_id}.")
        return
    elif action == "withdraw_account_number":
        state = decode_pending(data)
        method = state.get("method")
        if method not in {"Telebirr", "CBE"}:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("Your withdrawal session expired. Please tap Withdraw Points again."); return
        if not (5 <= len(value) <= 40):
            await message.reply_text("Enter a valid Telebirr phone number or CBE account number (5–40 characters)."); return
        state["account_number"] = value
        set_pending(user.id, "withdraw_account_name", state)
        await message.reply_text("Enter the account holder's full name exactly as registered with Telebirr/CBE:", reply_markup=kb([[("❌ Cancel","home")]]))
        return
    elif action == "withdraw_account_name":
        state = decode_pending(data)
        method = state.get("method")
        account_number = str(state.get("account_number", "")).strip()
        if method not in {"Telebirr", "CBE"} or not account_number:
            with db() as c: c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("Your withdrawal session expired. Please start again."); return
        if not (2 <= len(value) <= 100):
            await message.reply_text("Enter the account holder's name (2–100 characters)."); return
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (user.id,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
            minp = int(minimum["value"]) if minimum else 1000
            pending = c.execute("SELECT 1 FROM withdrawals WHERE user_id=? AND status='pending' LIMIT 1", (user.id,)).fetchone()
            if not u or int(u["points"]) < minp:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text(f"Your available points are below the withdrawal minimum ({minp})."); return
            if pending:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("You already have a pending withdrawal request. Please wait for admin review."); return
            payout_details = f"Account number: {account_number} | Account holder: {value}"
            cur = c.execute("INSERT INTO withdrawals(user_id,points,payout_method,payout_details,created_at) VALUES(?,?,?,?,?)",
                            (user.id, int(u["points"]), method, payout_details, now()))
            withdrawal_id = cur.lastrowid
            points_requested = int(u["points"])
            c.execute("UPDATE users SET points=0 WHERE user_id=? AND points>=?", (user.id, points_requested))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text(f"✅ Withdrawal request #{withdrawal_id} submitted!\nPoints: {points_requested}\nMethod: {method}\nAccount: {account_number}\nAccount holder: {value}\n\nAdmins will review your request. Your points are reserved until approval or rejection.")
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    aid,
                    f"💸 WITHDRAWAL REQUEST #{withdrawal_id}\nUser: {user.id} (@{user.username or 'no_username'})\nPoints: {points_requested}\nMethod: {method}\nAccount: {account_number}\nAccount holder: {value}",
                    reply_markup=kb([[("📨 Send payout message / proof",f"withdraw_delivery_{withdrawal_id}")],[("📥 Review queue","admin_queue")]])
                )
            except Exception:
                log.warning("Could not notify admin %s about withdrawal %s", aid, withdrawal_id)
        return
    elif action == "withdraw":
        # Compatibility for older sessions using the previous free-form withdrawal prompt.
        with db() as c:
            u = c.execute("SELECT points FROM users WHERE user_id=?", (user.id,)).fetchone()
            minimum = c.execute("SELECT value FROM settings WHERE key='min_withdraw_points'").fetchone()
            minp = int(minimum["value"]) if minimum else 1000
            if not u or u["points"] < minp:
                c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
                await message.reply_text("Your points are below the withdrawal minimum."); return
            c.execute("INSERT INTO withdrawals(user_id,points,payout_method,payout_details,created_at) VALUES(?,?,?,?,?)",
                      (user.id,u["points"],"user-provided",value,now()))
            c.execute("UPDATE users SET points=0 WHERE user_id=?", (user.id,))
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await message.reply_text("✅ Withdrawal request submitted for admin review. Points are reserved until the request is approved or rejected.")
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
    elif action == "sell_social":
        state = decode_pending(data)
        platform = state.get("platform","Social media")
        short_description = value[:350]
        with db() as c:
            cur = c.execute("INSERT INTO market_listings(user_id,action,asset_type,details,short_description,created_at) VALUES(?,?,?,?,?,?)",
                            (user.id,action,platform,value,short_description,now()))
            listing_id = cur.lastrowid
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
        await update.effective_message.reply_text(f"✅ Your {platform} account listing #{listing_id} was submitted for admin review. It will appear in Buy social media accounts only after an admin approves it, sets the price and publishes it. Never send passwords or verification codes.")
        await notify_admins(context, f"🛍 SOCIAL ACCOUNT LISTING #{listing_id}\nSeller: {user.id}\nPlatform: {platform}\nSubmission: {value}\n\nReview it in Admin Dashboard → Social account listings. Set the public short description and price, approve it in Review requests if needed, then publish.")
    elif action in ("buy_asset","sell_usdt"):
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
    promoter_notice = None
    promoter_uid = None
    task_notice = None
    task_owner_uid = None

    with db() as c:
        mapping = c.execute(
            "SELECT task_id,owner_user_id FROM invite_links WHERE invite_link=?", (link,)
        ).fetchone()

        if not mapping:
            promoter = c.execute(
                "SELECT * FROM promoter_profiles WHERE invite_link=? AND status='active'", (link,)
            ).fetchone()
            if not promoter:
                return

            joined_user = cmu.new_chat_member.user
            joined_id = joined_user.id
            if joined_id == promoter["user_id"] or int(promoter["completed_count"]) >= int(promoter["target_count"]):
                return

            # Channel members do not need to have started the bot first.
            c.execute(
                "INSERT OR IGNORE INTO users(user_id,username,first_name,joined_at) VALUES(?,?,?,?)",
                (joined_id, joined_user.username or "", joined_user.first_name or "", now())
            )
            try:
                c.execute(
                    "INSERT INTO promoter_join_events(promoter_user_id,joined_user_id,invite_link,joined_at) VALUES(?,?,?,?)",
                    (promoter["user_id"], joined_id, link, now())
                )
            except DB_INTEGRITY_ERROR:
                return

            updated = c.execute(
                "UPDATE promoter_profiles SET completed_count=completed_count+1,updated_at=? "
                "WHERE user_id=? AND status='active' AND completed_count<target_count",
                (now(), promoter["user_id"])
            )
            if updated.rowcount != 1:
                return

            reward = int(promoter["points_per_join"])
            c.execute("UPDATE users SET points=points+? WHERE user_id=?", (reward, promoter["user_id"]))
            new_count = int(promoter["completed_count"]) + 1
            target = int(promoter["target_count"])
            completed = new_count >= target
            if completed:
                c.execute("UPDATE promoter_profiles SET status='completed' WHERE user_id=?", (promoter["user_id"],))
            promoter_uid = promoter["user_id"]
            promoter_notice = (
                f"🎉 Verified promoter join recorded! You earned {reward} points.\n"
                f"Campaign progress: {new_count}/{target}."
            )
            if completed:
                promoter_notice += "\n\n🎯 Target reached! you can now request withdrawal of your available points."
        else:
            joined_id = cmu.new_chat_member.user.id
            if joined_id == mapping["owner_user_id"]:
                return
            try:
                c.execute(
                    "INSERT INTO invite_events(invite_link,joined_user_id,joined_at) VALUES(?,?,?)",
                    (link, joined_id, now())
                )
            except DB_INTEGRITY_ERROR:
                return

            task = c.execute(
                "SELECT * FROM tasks WHERE id=? AND active=1", (mapping["task_id"],)
            ).fetchone()
            if not task or task["completed_count"] >= task["target"]:
                return

            c.execute("UPDATE tasks SET completed_count=completed_count+1 WHERE id=?", (task["id"],))
            # Ensure the task owner has a wallet row before crediting the verified reward.
            # Without this, UPDATE silently affects zero rows while the success message still sends.
            c.execute(
                "INSERT OR IGNORE INTO users(user_id,username,first_name,joined_at) VALUES(?,?,?,?)",
                (mapping["owner_user_id"], "", "", now())
            )
            c.execute("UPDATE users SET points=points+? WHERE user_id=?",
                      (task["points"], mapping["owner_user_id"]))
            new_count = task["completed_count"] + 1
            c.execute("UPDATE task_claims SET status='in progress' WHERE task_id=? AND user_id=?",
                      (task["id"], mapping["owner_user_id"]))
            completed = new_count >= task["target"]
            if completed:
                c.execute("UPDATE tasks SET active=0 WHERE id=?", (task["id"],))
                c.execute("UPDATE task_claims SET status='completed' WHERE task_id=?", (task["id"],))
            task_owner_uid = mapping["owner_user_id"]
            task_notice = "🎉 Verified join recorded! You earned " + str(task["points"]) + " points.\n"
            task_notice += "Campaign progress: " + str(new_count) + "/" + str(task["target"]) + "."
            if completed:
                task_notice += "\nThe campaign target has been reached and the task is now closed."

    if promoter_notice and promoter_uid:
        try:
            await context.bot.send_message(promoter_uid, promoter_notice)
        except Exception:
            log.warning("Could not notify promoter %s", promoter_uid)
    elif task_notice and task_owner_uid:
        try:
            await context.bot.send_message(task_owner_uid, task_notice)
        except Exception:
            log.warning("Could not notify task owner %s", task_owner_uid)


async def log_own_membership_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log when Telegram changes KefiaETBot's own membership in a chat."""
    change = update.my_chat_member
    if not change:
        return
    chat = change.chat
    old_status = change.old_chat_member.status
    new_status = change.new_chat_member.status
    actor = change.from_user.id if change.from_user else "unknown"
    log.warning(
        "BOT_MEMBERSHIP_CHANGE chat_id=%s chat_title=%r chat_type=%s old_status=%s new_status=%s actor_user_id=%s",
        chat.id, chat.title or "", chat.type, old_status, new_status, actor
    )


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
    # Run the channel-membership gate before all normal commands and callbacks.
    app.add_handler(TypeHandler(Update, enforce_required_channel), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("set", set_setting))
    app.add_handler(CommandHandler("quote_ad", quote_ad))
    app.add_handler(CommandHandler("receipt", receipt_command))
    app.add_handler(CommandHandler("myads", my_ads_command))
    app.add_handler(CallbackQueryHandler(menu))
    app.add_handler(ChatMemberHandler(track_channel_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(log_own_membership_change, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL, handle_task_message_media), group=-2)
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL, handle_receipt_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)
    log.info("KefiaETBot starting")
    # Python 3.14 no longer implicitly creates a current event loop here.
    # Set one explicitly for python-telegram-bot's run_polling lifecycle.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        app.run_polling(
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"],
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
