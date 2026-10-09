import os
import sqlite3
import logging
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
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
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
        """)


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
    upsert_user(user)
    # A referral is counted once per Telegram account. Reward is optional and admin-configured.
    arg = context.args[0] if context.args else ""
    if arg.startswith("ref_") and arg[4:].isdigit():
        ref = int(arg[4:])
        if ref != user.id:
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


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    action = q.data
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
        await q.edit_message_text("🛍 Marketplace — choose what you want to do:", reply_markup=kb([
            [("🛒 Buy USDT / digital assets","buy_asset")],
            [("📲 Buy social-media promotion/accounts","buy_social")],
            [("💱 Sell USDT","sell_usdt")],
            [("📤 Sell a social-media asset","sell_social")],
            [("📋 My listings","my_market")],
            [("⬅️ Dashboard","home")]
        ]))
    elif action in ("buy_asset","buy_social","sell_usdt","sell_social"):
        labels = {"buy_asset":"buy USDT or another supported digital asset","buy_social":"buy a listed social-media service/asset",
                  "sell_usdt":"sell USDT for ETB","sell_social":"submit a social-media asset for review"}
        with db() as c:
            c.execute("INSERT INTO pending_inputs(user_id,action,data) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,data=excluded.data",
                      (uid,action,labels[action]))
            rate = c.execute("SELECT value FROM settings WHERE key='usdt_etb_rate'").fetchone()
        rate_text = f"Current admin-set rate: {rate['value']} ETB per USDT." if rate else "USDT/ETB rate has not yet been configured by an admin."
        extra = "\n" + rate_text if action in ("buy_asset","sell_usdt") else ""
        await q.edit_message_text(f"🛍 You selected: {labels[action]}.{extra}\n\nSend details in one message: asset/service, amount, link (if applicable), and your expected price. Social-media monetization and ownership are manually reviewed; never send passwords, seed phrases, or private keys.", reply_markup=kb([[("Cancel","home")]]))
    elif action == "admin":
        if not is_admin(uid):
            await q.edit_message_text("⛔ Admin access only."); return
        await q.edit_message_text("🛡 Admin Dashboard\nManage tasks, review payouts and listings, configure prices, and inspect platform statistics.", reply_markup=kb([
            [("➕ Create join task","admin_new_task"),("📊 Statistics","admin_stats")],
            [("📥 Review requests","admin_queue"),("⚙️ Set prices / limits","admin_settings")],
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
        msg += "\n\nSend /set key value to update a setting. Examples: /set min_withdraw_points 1000, /set usdt_etb_rate 150"
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
    with db() as c:
        pending = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
        if not pending or pending["action"] != "ad_receipt":
            return
        request_id = int(pending["data"])
        owned = c.execute("SELECT id FROM ad_requests WHERE id=? AND user_id=? AND status='quoted'", (request_id, user.id)).fetchone()
        if not owned:
            c.execute("DELETE FROM pending_inputs WHERE user_id=?", (user.id,))
            await message.reply_text("That ad request is no longer waiting for payment proof."); return
        if message.photo:
            receipt = "photo:" + message.photo[-1].file_id
        elif message.document:
            receipt = "document:" + message.document.file_id
        else:
            await message.reply_text("Please send a photo or document as payment proof."); return
        c.execute("UPDATE ad_requests SET receipt=?,status='receipt_submitted' WHERE id=? AND user_id=?", (receipt, request_id, user.id))
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
    await notify_admins(context, f"🧾 Payment proof submitted for ad request #{request_id} by user {user.id}. Review it in the Admin Dashboard; verify payment independently.")


async def set_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Admin only."); return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /set key value"); return
    key, value = context.args[0], " ".join(context.args[1:])
    allowed = {"min_withdraw_points","usdt_etb_rate","price_ad_product","price_ad_members","price_ad_views","referral_points"}
    if key not in allowed:
        await update.effective_message.reply_text(f"Allowed keys: {', '.join(sorted(allowed))}"); return
    try:
        if key != "min_withdraw_points" and float(value) < 0: raise ValueError()
        if key == "min_withdraw_points" and int(value) < 1: raise ValueError()
    except ValueError:
        await update.effective_message.reply_text("Please enter a valid positive number."); return
    with db() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,value))
    await update.effective_message.reply_text(f"Updated {key} = {value}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not update.effective_message or not update.effective_message.text: return
    with db() as c:
        p = c.execute("SELECT action,data FROM pending_inputs WHERE user_id=?", (user.id,)).fetchone()
    if not p:
        await update.effective_message.reply_text("Use the dashboard buttons to get started.", reply_markup=home_keyboard(is_admin(user.id))); return
    action, data = p["action"], p["data"]
    value = update.effective_message.text.strip()
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


def main():
    if not TOKEN:
        raise RuntimeError("Set BOT_TOKEN environment variable.")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS is empty. Admin dashboard and approvals will be unavailable.")
    init_db()
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
    app.run_polling(allowed_updates=["message","callback_query","chat_member"])


if __name__ == "__main__":
    main()
