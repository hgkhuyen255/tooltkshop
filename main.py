# -*- coding: utf-8 -*-
"""
Telegram bot bán tool + Machine ID + coupon + payOS auto duyệt + nhắc hạn

Cài:
    pip install pyTelegramBotAPI Flask requests

ENV tối thiểu:
    BOT_TOKEN=...
    ADMIN_IDS=123456789,987654321
    WEBHOOK_SECRET=mot_bi_mat
    PUBLIC_BASE_URL=https://domain-cua-ban.com

ENV payOS:
    PAYOS_CLIENT_ID=...
    PAYOS_API_KEY=...
    PAYOS_CHECKSUM_KEY=...
    PAYOS_BASE_URL=https://api-merchant.payos.vn
    PAYOS_RETURN_URL=https://domain-cua-ban.com/payment-return
    PAYOS_CANCEL_URL=https://domain-cua-ban.com/payment-cancel
    PAYOS_WEBHOOK_PATH=/payos-webhook

Chạy:
    python telegram_tool_sales_bot_v3_payos.py
"""

import os
import re
import hmac
import time
import sqlite3
import hashlib
import secrets
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, request, jsonify
import telebot
from telebot import types

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
DB_PATH = os.getenv("DB_PATH", "tool_bot.sqlite3")
TZ = timezone(timedelta(hours=7))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip() or "change_me"
FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("FLASK_PORT", "8787"))

BANK_NAME = os.getenv("BANK_NAME", "").strip()
BANK_ACCOUNT_NO = os.getenv("BANK_ACCOUNT_NO", "").strip()
BANK_ACCOUNT_NAME = os.getenv("BANK_ACCOUNT_NAME", "").strip()
PAYMENT_NOTE_PREFIX = os.getenv("PAYMENT_NOTE_PREFIX", "TOOL")

PAYOS_CLIENT_ID = os.getenv("PAYOS_CLIENT_ID", "")
PAYOS_API_KEY = os.getenv("PAYOS_API_KEY", "")
PAYOS_CHECKSUM_KEY = os.getenv("PAYOS_CHECKSUM_KEY", "")
PAYOS_BASE_URL = os.getenv("PAYOS_BASE_URL", "https://api-merchant.payos.vn")
PAYOS_RETURN_URL = os.getenv("PAYOS_RETURN_URL", f"{PUBLIC_BASE_URL}/payment-return" if PUBLIC_BASE_URL else "")
PAYOS_CANCEL_URL = os.getenv("PAYOS_CANCEL_URL", f"{PUBLIC_BASE_URL}/payment-cancel" if PUBLIC_BASE_URL else "")
PAYOS_WEBHOOK_PATH = os.getenv("PAYOS_WEBHOOK_PATH", "/payos-webhook")
PAYOS_WEBHOOK_URL = f"{PUBLIC_BASE_URL}{PAYOS_WEBHOOK_PATH}" if PUBLIC_BASE_URL else ""

REMINDER_CHECK_INTERVAL_SECONDS = int(os.getenv("REMINDER_CHECK_INTERVAL_SECONDS", "3600"))
REMINDER_DAYS = [7, 3, 1, 0]

if not BOT_TOKEN:
    raise SystemExit("Thiếu BOT_TOKEN.")
if not ADMIN_IDS:
    raise SystemExit("Thiếu ADMIN_IDS.")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)
BUY_STATE = {}
db = None

def now_vn():
    return datetime.now(TZ)

def iso_now():
    return now_vn().isoformat()

def fmt_money(v):
    try:
        return f"{int(v):,}đ"
    except Exception:
        return f"{v}đ"

def fmt_dt(dt_str):
    if not dt_str:
        return "Chưa có"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(dt_str)

def safe_upper(s):
    return (s or "").strip().upper()

def norm_machine_id(s):
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", "", s)
    for bad in ["MACHINEID=", "MACHINE_ID=", "MACHINE-ID=", "MACHINE ID=", "MACHINE ID :", "MACHINEID:", "MACHINE ID"]:
        s = s.replace(bad, "")
    s = s.replace("=", "").replace(":", "")
    return s

def is_valid_machine_id(s):
    s = norm_machine_id(s)
    return bool(re.fullmatch(r"[A-F0-9]{16,64}", s))

def user_label(user):
    full_name = " ".join(x for x in [getattr(user, "first_name", ""), getattr(user, "last_name", "")] if x).strip()
    return full_name or getattr(user, "username", "") or str(getattr(user, "id", ""))

def notify_admins(text):
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, text)
        except Exception:
            pass

class DB:
    def __init__(self, path):
        self.path = path
        self.init_db()

    def conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def init_db(self):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                is_admin INTEGER DEFAULT 0,
                is_blocked INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS tools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                price INTEGER NOT NULL DEFAULT 0,
                description TEXT DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS licenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tool_code TEXT NOT NULL,
                machine_id TEXT,
                expires_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, tool_code)
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS coupons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                discount_type TEXT NOT NULL,
                discount_value INTEGER NOT NULL,
                max_uses INTEGER NOT NULL DEFAULT 1,
                used_count INTEGER NOT NULL DEFAULT 0,
                expires_at TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS coupon_uses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupon_code TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                used_at TEXT NOT NULL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_code TEXT UNIQUE NOT NULL,
                payos_order_code INTEGER,
                user_id INTEGER NOT NULL,
                tool_code TEXT NOT NULL,
                machine_id TEXT NOT NULL,
                months INTEGER NOT NULL,
                base_price INTEGER NOT NULL,
                coupon_code TEXT,
                discount_amount INTEGER NOT NULL DEFAULT 0,
                final_price INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payment_status TEXT NOT NULL DEFAULT 'unpaid',
                payment_ref TEXT,
                checkout_url TEXT,
                qr_url TEXT,
                qr_code TEXT,
                payment_provider TEXT,
                payment_description TEXT,
                paid_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS reminder_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tool_code TEXT NOT NULL,
                reminder_key TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                UNIQUE(user_id, tool_code, reminder_key)
            )""")
            con.commit()
        for aid in ADMIN_IDS:
            self.upsert_user(aid, "", f"Admin {aid}", True)

    def upsert_user(self, user_id, username, full_name, is_admin_flag=False):
        now = iso_now()
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT user_id, is_admin FROM users WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE users SET username=?, full_name=?, is_admin=?, updated_at=? WHERE user_id=?",
                            (username, full_name, 1 if (is_admin_flag or row['is_admin']) else 0, now, user_id))
            else:
                cur.execute("INSERT INTO users(user_id, username, full_name, is_admin, created_at, updated_at) VALUES(?,?,?,?,?,?)",
                            (user_id, username, full_name, 1 if is_admin_flag else 0, now, now))
            con.commit()

    def is_admin(self, user_id):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT is_admin FROM users WHERE user_id=?", (user_id,))
            row = cur.fetchone()
            return bool(row and row["is_admin"] == 1)

    def add_tool(self, code, name, price, description=""):
        with self.conn() as con:
            con.execute("INSERT INTO tools(code, name, price, description, active, created_at) VALUES(?,?,?,?,?,?)",
                        (safe_upper(code), name.strip(), int(price), description.strip(), 1, iso_now()))
            con.commit()

    def update_tool_price(self, code, price):
        with self.conn() as con:
            con.execute("UPDATE tools SET price=? WHERE code=?", (int(price), safe_upper(code)))
            con.commit()

    def get_tool(self, code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM tools WHERE code=? AND active=1", (safe_upper(code),))
            return cur.fetchone()

    def list_tools(self):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM tools WHERE active=1 ORDER BY id DESC")
            return cur.fetchall()

    def get_license(self, user_id, tool_code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM licenses WHERE user_id=? AND tool_code=?", (user_id, safe_upper(tool_code)))
            return cur.fetchone()

    def list_user_licenses(self, user_id):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("""
                SELECT l.*, t.name, t.price
                FROM licenses l
                LEFT JOIN tools t ON t.code = l.tool_code
                WHERE l.user_id = ?
                ORDER BY l.updated_at DESC
            """, (user_id,))
            return cur.fetchall()

    def extend_license(self, user_id, tool_code, days, machine_id=None):
        now = now_vn()
        now_iso = now.isoformat()
        tool_code = safe_upper(tool_code)
        machine_id = norm_machine_id(machine_id) if machine_id else None
        lic = self.get_license(user_id, tool_code)
        with self.conn() as con:
            cur = con.cursor()
            if lic:
                old_exp = datetime.fromisoformat(lic["expires_at"])
                base = old_exp if old_exp > now else now
                new_exp = base + timedelta(days=int(days))
                if machine_id:
                    cur.execute("UPDATE licenses SET machine_id=?, expires_at=?, status='active', updated_at=? WHERE user_id=? AND tool_code=?",
                                (machine_id, new_exp.isoformat(), now_iso, user_id, tool_code))
                else:
                    cur.execute("UPDATE licenses SET expires_at=?, status='active', updated_at=? WHERE user_id=? AND tool_code=?",
                                (new_exp.isoformat(), now_iso, user_id, tool_code))
            else:
                new_exp = now + timedelta(days=int(days))
                cur.execute("INSERT INTO licenses(user_id, tool_code, machine_id, expires_at, status, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                            (user_id, tool_code, machine_id, new_exp.isoformat(), "active", now_iso, now_iso))
            con.commit()
        return new_exp

    def add_coupon(self, code, discount_type, discount_value, max_uses, expires_at):
        with self.conn() as con:
            con.execute("INSERT INTO coupons(code, discount_type, discount_value, max_uses, used_count, expires_at, active, created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (safe_upper(code), discount_type, int(discount_value), int(max_uses), 0, expires_at, 1, iso_now()))
            con.commit()

    def get_coupon(self, code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM coupons WHERE code=?", (safe_upper(code),))
            return cur.fetchone()

    def user_used_coupon(self, user_id, code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM coupon_uses WHERE user_id=? AND coupon_code=? LIMIT 1", (user_id, safe_upper(code)))
            return cur.fetchone() is not None

    def validate_coupon(self, user_id, code, base_price):
        cp = self.get_coupon(code)
        if not cp:
            return False, "Mã giảm giá không tồn tại.", 0
        if cp["active"] != 1:
            return False, "Mã giảm giá đã bị tắt.", 0
        if cp["expires_at"] and datetime.fromisoformat(cp["expires_at"]) < now_vn():
            return False, "Mã giảm giá đã hết hạn.", 0
        if cp["used_count"] >= cp["max_uses"]:
            return False, "Mã giảm giá đã hết lượt dùng.", 0
        if self.user_used_coupon(user_id, cp["code"]):
            return False, "Bạn đã dùng mã này rồi.", 0
        if cp["discount_type"] == "percent":
            discount = max(0, min(base_price, (base_price * int(cp["discount_value"])) // 100))
        else:
            discount = max(0, min(base_price, int(cp["discount_value"])))
        return True, "OK", discount

    def mark_coupon_used(self, user_id, code):
        with self.conn() as con:
            con.execute("INSERT INTO coupon_uses(coupon_code, user_id, used_at) VALUES(?,?,?)", (safe_upper(code), user_id, iso_now()))
            con.execute("UPDATE coupons SET used_count = used_count + 1 WHERE code=?", (safe_upper(code),))
            con.commit()

    def create_order(self, user_id, tool_code, machine_id, months, base_price, coupon_code, discount_amount, final_price, payos_info):
        order_code = "OD" + secrets.token_hex(4).upper()
        with self.conn() as con:
            con.execute("""
                INSERT INTO orders(order_code, payos_order_code, user_id, tool_code, machine_id, months, base_price, coupon_code, discount_amount, final_price, status, payment_status, payment_ref, checkout_url, qr_url, qr_code, payment_provider, payment_description, paid_at, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                order_code,
                int(payos_info.get("payos_order_code") or 0) if payos_info.get("payos_order_code") else None,
                user_id,
                safe_upper(tool_code),
                norm_machine_id(machine_id),
                int(months),
                int(base_price),
                safe_upper(coupon_code) if coupon_code else None,
                int(discount_amount),
                int(final_price),
                "pending",
                "unpaid",
                None,
                payos_info.get("checkout_url", ""),
                payos_info.get("qr_url", ""),
                payos_info.get("qr_code", ""),
                payos_info.get("provider", "fallback"),
                payos_info.get("description", ""),
                None,
                iso_now(),
                iso_now()
            ))
            con.commit()
        return order_code

    def get_order(self, order_code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM orders WHERE order_code=?", (safe_upper(order_code),))
            return cur.fetchone()

    def get_order_by_payos_code(self, payos_order_code):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM orders WHERE payos_order_code=?", (int(payos_order_code),))
            return cur.fetchone()

    def update_order_payment(self, order_code, status, payment_ref=None, paid=False):
        with self.conn() as con:
            con.execute("""
                UPDATE orders
                SET payment_status=?, payment_ref=COALESCE(?, payment_ref), paid_at=CASE WHEN ? THEN ? ELSE paid_at END, updated_at=?
                WHERE order_code=?
            """, (status, payment_ref, 1 if paid else 0, iso_now(), iso_now(), safe_upper(order_code)))
            con.commit()

    def update_order_status(self, order_code, status):
        with self.conn() as con:
            con.execute("UPDATE orders SET status=?, updated_at=? WHERE order_code=?", (status, iso_now(), safe_upper(order_code)))
            con.commit()

    def all_user_ids(self):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT user_id FROM users WHERE is_blocked=0")
            return [r["user_id"] for r in cur.fetchall()]

    def mark_blocked(self, user_id):
        with self.conn() as con:
            con.execute("UPDATE users SET is_blocked=1, updated_at=? WHERE user_id=?", (iso_now(), user_id))
            con.commit()

    def reminder_sent(self, user_id, tool_code, reminder_key):
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM reminder_logs WHERE user_id=? AND tool_code=? AND reminder_key=? LIMIT 1",
                        (user_id, safe_upper(tool_code), reminder_key))
            return cur.fetchone() is not None

    def mark_reminder_sent(self, user_id, tool_code, reminder_key):
        with self.conn() as con:
            try:
                con.execute("INSERT INTO reminder_logs(user_id, tool_code, reminder_key, sent_at) VALUES(?,?,?,?)",
                            (user_id, safe_upper(tool_code), reminder_key, iso_now()))
                con.commit()
            except sqlite3.IntegrityError:
                pass

    def get_expiring_licenses(self, within_days=7):
        future = now_vn() + timedelta(days=within_days)
        with self.conn() as con:
            cur = con.cursor()
            cur.execute("""
                SELECT l.*, t.name
                FROM licenses l
                LEFT JOIN tools t ON t.code = l.tool_code
                WHERE datetime(l.expires_at) <= datetime(?)
                ORDER BY datetime(l.expires_at) ASC
            """, (future.isoformat(),))
            return cur.fetchall()

db = DB(DB_PATH)

def is_admin(user_id):
    return user_id in ADMIN_IDS or db.is_admin(user_id)

def admin_only(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Bạn không có quyền dùng lệnh admin.")
        return False
    return True

def build_payos_order_code():
    return int(time.time() * 1000) % 900000000000 + 100000000000

def generate_qr(amount, payment_code):
    if BANK_NAME and BANK_ACCOUNT_NO:
        return f"https://img.vietqr.io/image/970436-{BANK_ACCOUNT_NO}-compact2.png?amount={int(amount)}&addInfo={urllib.parse.quote(payment_code)}"
    return ""

def payos_headers():
    return {
        "x-client-id": PAYOS_CLIENT_ID,
        "x-api-key": PAYOS_API_KEY,
        "Content-Type": "application/json",
    }

def sign_payos_payment_request(amount, order_code, description, cancel_url, return_url):
    raw = (
        f"amount={amount}"
        f"&cancelUrl={cancel_url}"
        f"&description={description}"
        f"&orderCode={order_code}"
        f"&returnUrl={return_url}"
    )
    return hmac.new(PAYOS_CHECKSUM_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()

def deep_sort_data(obj):
    if isinstance(obj, dict):
        return {k: deep_sort_data(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [deep_sort_data(item) for item in obj]
    return obj

def flatten_signature_data(data, prefix=""):
    pairs = []
    if isinstance(data, dict):
        for key in sorted(data.keys()):
            new_prefix = f"{prefix}.{key}" if prefix else key
            pairs.extend(flatten_signature_data(data[key], new_prefix))
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            new_prefix = f"{prefix}[{idx}]"
            pairs.extend(flatten_signature_data(item, new_prefix))
    else:
        if data is None:
            value = ""
        elif isinstance(data, bool):
            value = "true" if data else "false"
        else:
            value = str(data)
        pairs.append((prefix, value))
    return pairs

def verify_payos_webhook_signature(payload):
    signature = payload.get("signature")
    data = payload.get("data")
    if not signature or data is None or not PAYOS_CHECKSUM_KEY:
        return False
    sorted_data = deep_sort_data(data)
    pairs = flatten_signature_data(sorted_data)
    raw = "&".join(f"{k}={v}" for k, v in pairs)
    expected = hmac.new(PAYOS_CHECKSUM_KEY.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

def create_payos_payment_link(amount, payment_code, product_name):
    order_code = build_payos_order_code()
    description = payment_code[:25]
    fallback_qr = generate_qr(amount, payment_code)
    result = {
        "payos_order_code": order_code,
        "checkout_url": "",
        "qr_url": fallback_qr,
        "qr_code": "",
        "provider": "fallback",
        "description": description,
    }
    if not (PAYOS_CLIENT_ID and PAYOS_API_KEY and PAYOS_CHECKSUM_KEY and PAYOS_RETURN_URL and PAYOS_CANCEL_URL):
        return result

    payload = {
        "orderCode": order_code,
        "amount": int(amount),
        "description": description,
        "items": [{
            "name": product_name[:25],
            "quantity": 1,
            "price": int(amount),
        }],
        "cancelUrl": PAYOS_CANCEL_URL,
        "returnUrl": PAYOS_RETURN_URL,
    }
    payload["signature"] = sign_payos_payment_request(int(amount), order_code, description, PAYOS_CANCEL_URL, PAYOS_RETURN_URL)
    try:
        r = requests.post(f"{PAYOS_BASE_URL}/v2/payment-requests", headers=payos_headers(), json=payload, timeout=20)
        data = r.json()
        if r.ok and str(data.get("code")) == "00" and data.get("data"):
            info = data["data"]
            qr_raw = info.get("qrCode") or ""
            qr_img = f"https://api.qrserver.com/v1/create-qr-code/?size=512x512&data={urllib.parse.quote(qr_raw)}" if qr_raw else fallback_qr
            result.update({
                "checkout_url": info.get("checkoutUrl", ""),
                "qr_url": qr_img,
                "qr_code": qr_raw,
                "provider": "payos",
                "payment_link_id": info.get("paymentLinkId", ""),
            })
    except Exception as e:
        print(f"create_payos_payment_link error: {e}")
    return result

def get_payos_payment_status(order_code):
    if not (PAYOS_CLIENT_ID and PAYOS_API_KEY):
        return {"ok": False, "error": "payos_not_configured"}
    try:
        r = requests.get(f"{PAYOS_BASE_URL}/v2/payment-requests/{order_code}", headers=payos_headers(), timeout=20)
        data = r.json()
        if not r.ok:
            return {"ok": False, "error": data}
        return {"ok": True, "data": data.get("data") or {}}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def confirm_payos_webhook_url():
    if not (PAYOS_CLIENT_ID and PAYOS_API_KEY and PAYOS_WEBHOOK_URL):
        return
    try:
        r = requests.post(f"{PAYOS_BASE_URL}/confirm-webhook", headers=payos_headers(), json={"webhookUrl": PAYOS_WEBHOOK_URL}, timeout=20)
        print("payOS confirm-webhook:", r.status_code, r.text)
    except Exception as e:
        print(f"payOS confirm-webhook error: {e}")

def main_menu_markup(user_id):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("🛍 Mua tool", callback_data="menu_buy"),
        types.InlineKeyboardButton("📅 Hạn dùng của tôi", callback_data="menu_my")
    )
    mk.add(
        types.InlineKeyboardButton("🎁 Mã giảm giá", callback_data="menu_coupon_help"),
        types.InlineKeyboardButton("☎️ Liên hệ admin", callback_data="menu_contact")
    )
    if is_admin(user_id):
        mk.add(types.InlineKeyboardButton("🛠 Admin", callback_data="menu_admin"))
    return mk

def buy_menu_markup():
    mk = types.InlineKeyboardMarkup(row_width=1)
    for t in db.list_tools():
        mk.add(types.InlineKeyboardButton(f"{t['name']} • {fmt_money(t['price'])}/tháng", callback_data=f"buytool:{t['code']}"))
    mk.add(types.InlineKeyboardButton("⬅️ Về menu", callback_data="back_main"))
    return mk

def months_markup(tool_code):
    mk = types.InlineKeyboardMarkup(row_width=3)
    for m in [1, 3, 6, 12]:
        mk.add(types.InlineKeyboardButton(f"{m} tháng", callback_data=f"months:{tool_code}:{m}"))
    mk.add(types.InlineKeyboardButton("⬅️ Chọn tool khác", callback_data="menu_buy"))
    return mk

def coupon_decision_markup():
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(types.InlineKeyboardButton("🎁 Nhập mã giảm giá", callback_data="enter_coupon"),
           types.InlineKeyboardButton("➡️ Bỏ qua", callback_data="skip_coupon"))
    return mk

def payment_markup(order_code, checkout_url=""):
    mk = types.InlineKeyboardMarkup(row_width=1)
    if checkout_url:
        mk.add(types.InlineKeyboardButton("💳 Thanh toán qua payOS", url=checkout_url))
    mk.add(types.InlineKeyboardButton("🔄 Kiểm tra trạng thái", callback_data=f"checkorder:{order_code}"))
    mk.add(types.InlineKeyboardButton("⬅️ Về menu", callback_data="back_main"))
    return mk

def admin_menu_markup():
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(types.InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
           types.InlineKeyboardButton("⏰ Chạy nhắc hạn", callback_data="admin_run_remind"))
    mk.add(types.InlineKeyboardButton("⬅️ Về menu", callback_data="back_main"))
    return mk

def build_payment_text(order):
    transfer_note = f"{PAYMENT_NOTE_PREFIX} {order['order_code']}"
    lines = [
        "<b>Đơn hàng của bạn</b>",
        f"Mã đơn: <code>{order['order_code']}</code>",
        f"Tool: <b>{order['tool_code']}</b>",
        f"Số tháng: <b>{order['months']}</b>",
        f"Machine ID: <code>{order['machine_id']}</code>",
        f"Giá gốc: <b>{fmt_money(order['base_price'])}</b>",
        f"Giảm giá: <b>{fmt_money(order['discount_amount'])}</b>",
        f"Cần thanh toán: <b>{fmt_money(order['final_price'])}</b>",
    ]
    if order["payment_provider"] == "payos":
        lines += ["", "Bấm nút bên dưới để thanh toán qua payOS. Sau khi thanh toán xong, hệ thống sẽ tự duyệt đơn."]
    else:
        lines += ["", "payOS chưa cấu hình đầy đủ, hiện bot đang dùng thông tin chuyển khoản thủ công."]
        if BANK_NAME and BANK_ACCOUNT_NO and BANK_ACCOUNT_NAME:
            lines += [
                f"Ngân hàng: <b>{BANK_NAME}</b>",
                f"Số tài khoản: <code>{BANK_ACCOUNT_NO}</code>",
                f"Chủ tài khoản: <b>{BANK_ACCOUNT_NAME}</b>",
                f"Nội dung CK: <code>{transfer_note}</code>",
            ]
    return "\n".join(lines)

def approve_paid_order(order_code, payment_ref=None, source="payos"):
    order = db.get_order(order_code)
    if not order:
        return False, "Order không tồn tại."
    if order["status"] == "paid":
        return True, "Order đã duyệt trước đó."
    days = int(order["months"]) * 30
    new_exp = db.extend_license(order["user_id"], order["tool_code"], days, order["machine_id"])
    if order["coupon_code"]:
        try:
            db.mark_coupon_used(order["user_id"], order["coupon_code"])
        except Exception:
            pass
    db.update_order_payment(order_code, "paid", payment_ref, paid=True)
    db.update_order_status(order_code, "paid")

    customer_text = (
        f"✅ <b>Thanh toán thành công</b>\n"
        f"Mã đơn: <code>{order['order_code']}</code>\n"
        f"Tool: <b>{order['tool_code']}</b>\n"
        f"Machine ID: <code>{order['machine_id']}</code>\n"
        f"Thời hạn: <b>{order['months']}</b> tháng\n"
        f"Hết hạn mới: <b>{fmt_dt(new_exp.isoformat())}</b>\n"
        f"Mã tham chiếu: <code>{payment_ref or '-'}</code>"
    )
    try:
        bot.send_message(order["user_id"], customer_text, reply_markup=main_menu_markup(order["user_id"]))
    except Exception:
        pass

    admin_text = (
        f"💰 <b>Đơn đã tự duyệt</b>\n"
        f"Nguồn: <b>{source}</b>\n"
        f"Order: <code>{order['order_code']}</code>\n"
        f"User: <code>{order['user_id']}</code>\n"
        f"Tool: <b>{order['tool_code']}</b>\n"
        f"Machine ID: <code>{order['machine_id']}</code>\n"
        f"Số tháng: <b>{order['months']}</b>\n"
        f"Số tiền: <b>{fmt_money(order['final_price'])}</b>\n"
        f"Ref: <code>{payment_ref or '-'}</code>\n"
        f"Hết hạn mới: <b>{fmt_dt(new_exp.isoformat())}</b>"
    )
    notify_admins(admin_text)
    return True, "OK"

def process_expiry_reminders():
    rows = db.get_expiring_licenses(within_days=max(REMINDER_DAYS))
    now = now_vn()
    admin_lines = []
    for r in rows:
        exp = datetime.fromisoformat(r["expires_at"])
        delta_days = (exp.date() - now.date()).days
        if delta_days not in REMINDER_DAYS:
            continue
        reminder_key = f"{delta_days}_{now.date().isoformat()}"
        if db.reminder_sent(r["user_id"], r["tool_code"], reminder_key):
            continue
        tool_name = r["name"] or r["tool_code"]
        if delta_days > 0:
            user_text = f"⏰ <b>Nhắc hạn tool</b>\nTool: <b>{tool_name}</b>\nMachine ID: <code>{r['machine_id'] or '-'}</code>\nCòn <b>{delta_days}</b> ngày sẽ hết hạn.\nHết hạn lúc: <b>{fmt_dt(r['expires_at'])}</b>"
        else:
            user_text = f"⚠️ <b>Tool đã hết hạn</b>\nTool: <b>{tool_name}</b>\nMachine ID: <code>{r['machine_id'] or '-'}</code>\nHết hạn lúc: <b>{fmt_dt(r['expires_at'])}</b>"
        try:
            bot.send_message(r["user_id"], user_text, reply_markup=main_menu_markup(r["user_id"]))
        except Exception:
            db.mark_blocked(r["user_id"])
        admin_lines.append(f"• User <code>{r['user_id']}</code> | {r['tool_code']} | còn {delta_days} ngày | hết hạn {fmt_dt(r['expires_at'])}")
        db.mark_reminder_sent(r["user_id"], r["tool_code"], reminder_key)
    if admin_lines:
        notify_admins("📋 <b>Danh sách user sắp hết hạn</b>\n" + "\n".join(admin_lines))

def reminder_loop():
    while True:
        try:
            process_expiry_reminders()
        except Exception as e:
            notify_admins(f"⚠️ Reminder loop lỗi: <code>{e}</code>")
        time.sleep(REMINDER_CHECK_INTERVAL_SECONDS)

@bot.message_handler(commands=["start"])
def cmd_start(message):
    db.upsert_user(message.from_user.id, message.from_user.username or "", user_label(message.from_user), is_admin(message.from_user.id))
    text = (
        f"Xin chào <b>{user_label(message.from_user)}</b>\n\n"
        f"Bot bán tool hỗ trợ:\n"
        f"• chọn tool bằng menu bấm\n"
        f"• nhập <b>Machine ID</b> trước khi thanh toán\n"
        f"• payOS tự duyệt đơn sau khi khách thanh toán xong\n"
        f"• gửi đơn cho khách và đồng thời báo admin\n"
        f"• nhắc hạn cho khách và báo admin user sắp hết hạn"
    )
    bot.send_message(message.chat.id, text, reply_markup=main_menu_markup(message.from_user.id))

@bot.message_handler(commands=["help"])
def cmd_help(message):
    bot.send_message(message.chat.id,
        "<b>Lệnh user</b>\n/start\n/tools\n/mylicense\n\n"
        "<b>Lệnh admin</b>\n"
        "/addtool CODE | Tên tool | Giá | Mô tả\n"
        "/setprice CODE | Giá_mới\n"
        "/adduser user_id | TOOL_CODE | số_ngày | MACHINE_ID(optional)\n"
        "/extend user_id | TOOL_CODE | số_ngày | MACHINE_ID(optional)\n"
        "/coupon CODE | percent|fixed | value | max_uses | YYYY-MM-DD hoặc -\n"
        "/approve ORDER_CODE\n"
        "/broadcast nội dung\n"
        "/run_reminders\n")

@bot.message_handler(commands=["tools"])
def cmd_tools(message):
    tools = db.list_tools()
    if not tools:
        bot.send_message(message.chat.id, "Chưa có tool nào.", reply_markup=main_menu_markup(message.from_user.id))
        return
    lines = ["<b>Danh sách tool</b>"]
    for t in tools:
        desc = f"\n  {t['description']}" if t["description"] else ""
        lines.append(f"• <b>{t['code']}</b> — {t['name']} — {fmt_money(t['price'])}/tháng{desc}")
    bot.send_message(message.chat.id, "\n".join(lines), reply_markup=buy_menu_markup())

@bot.message_handler(commands=["mylicense"])
def cmd_mylicense(message):
    rows = db.list_user_licenses(message.from_user.id)
    if not rows:
        bot.send_message(message.chat.id, "Bạn chưa có tool nào được kích hoạt.", reply_markup=main_menu_markup(message.from_user.id))
        return
    now = now_vn()
    lines = ["<b>Hạn dùng của bạn</b>"]
    for r in rows:
        exp = datetime.fromisoformat(r["expires_at"])
        remain = (exp.date() - now.date()).days
        status = "Còn hạn" if exp > now else "Hết hạn"
        name = r["name"] or r["tool_code"]
        lines.append(
            f"• <b>{name}</b> ({r['tool_code']})\n"
            f"  Machine ID: <code>{r['machine_id'] or '-'}</code>\n"
            f"  Hết hạn: <b>{fmt_dt(r['expires_at'])}</b>\n"
            f"  Trạng thái: <b>{status}</b> | Còn: <b>{remain}</b> ngày"
        )
    bot.send_message(message.chat.id, "\n\n".join(lines), reply_markup=main_menu_markup(message.from_user.id))

@bot.callback_query_handler(func=lambda c: True)
def callbacks(call):
    user_id = call.from_user.id
    try:
        if call.data == "back_main":
            bot.edit_message_text("Chọn chức năng bên dưới:", call.message.chat.id, call.message.message_id, reply_markup=main_menu_markup(user_id))
            return
        if call.data == "menu_buy":
            bot.edit_message_text("<b>Chọn tool bạn muốn mua</b>", call.message.chat.id, call.message.message_id, reply_markup=buy_menu_markup())
            return
        if call.data == "menu_my":
            rows = db.list_user_licenses(user_id)
            if not rows:
                txt = "Bạn chưa có tool nào được kích hoạt."
            else:
                now = now_vn()
                parts = ["<b>Hạn dùng của bạn</b>"]
                for r in rows:
                    exp = datetime.fromisoformat(r["expires_at"])
                    remain = (exp.date() - now.date()).days
                    parts.append(f"• <b>{r['tool_code']}</b> | hết hạn {fmt_dt(r['expires_at'])} | còn {remain} ngày\n  Machine ID: <code>{r['machine_id'] or '-'}</code>")
                txt = "\n\n".join(parts)
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=main_menu_markup(user_id))
            return
        if call.data == "menu_coupon_help":
            bot.answer_callback_query(call.id, "Coupon sẽ nhập ở bước mua hàng.")
            bot.send_message(call.message.chat.id, "Khi mua tool, bot sẽ hỏi mã giảm giá trước khi tới bước thanh toán.")
            return
        if call.data == "menu_contact":
            bot.answer_callback_query(call.id, "Liên hệ admin.")
            bot.send_message(call.message.chat.id, "Nhắn trực tiếp admin để được hỗ trợ nhanh.")
            return
        if call.data == "menu_admin":
            if not is_admin(user_id):
                bot.answer_callback_query(call.id, "Không có quyền.")
                return
            bot.edit_message_text("<b>Khu vực admin</b>", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_markup())
            return
        if call.data == "admin_broadcast":
            if not is_admin(user_id):
                return
            msg = bot.send_message(call.message.chat.id, "Gửi nội dung broadcast ngay sau tin nhắn này:")
            bot.register_next_step_handler(msg, handle_admin_broadcast)
            return
        if call.data == "admin_run_remind":
            if not is_admin(user_id):
                return
            process_expiry_reminders()
            bot.answer_callback_query(call.id, "Đã chạy nhắc hạn.")
            bot.send_message(call.message.chat.id, "Đã chạy nhắc hạn xong.")
            return
        if call.data.startswith("buytool:"):
            tool_code = call.data.split(":", 1)[1]
            tool = db.get_tool(tool_code)
            if not tool:
                bot.answer_callback_query(call.id, "Tool không tồn tại.")
                return
            bot.edit_message_text(f"<b>{tool['name']}</b>\nGiá: <b>{fmt_money(tool['price'])}/tháng</b>\n\nChọn thời hạn:", call.message.chat.id, call.message.message_id, reply_markup=months_markup(tool['code']))
            return
        if call.data.startswith("months:"):
            _, tool_code, months = call.data.split(":")
            tool = db.get_tool(tool_code)
            if not tool:
                bot.answer_callback_query(call.id, "Tool không tồn tại.")
                return
            BUY_STATE[user_id] = {"tool_code": tool_code, "months": int(months), "coupon_code": None, "machine_id": None}
            msg = bot.send_message(call.message.chat.id, f"Bạn đã chọn <b>{tool['name']}</b> — <b>{months}</b> tháng.\n\nVui lòng nhập <b>Machine ID</b> của khách.\nVí dụ:\n<code>Machine ID=B8A8334E67D60DCE1D38FFE40CDA3F1F</code>")
            bot.register_next_step_handler(msg, handle_machine_id_step)
            return
        if call.data == "enter_coupon":
            msg = bot.send_message(call.message.chat.id, "Nhập mã giảm giá:")
            bot.register_next_step_handler(msg, handle_coupon_step)
            return
        if call.data == "skip_coupon":
            create_order_and_show_payment(call.message.chat.id, user_id)
            return
        if call.data.startswith("checkorder:"):
            order_code = call.data.split(":", 1)[1]
            order = db.get_order(order_code)
            if not order or order["user_id"] != user_id:
                bot.answer_callback_query(call.id, "Không tìm thấy order.")
                return
            if order["status"] != "paid" and order["payos_order_code"]:
                status_resp = get_payos_payment_status(order["payos_order_code"])
                if status_resp.get("ok"):
                    st = str((status_resp.get("data") or {}).get("status") or "").upper()
                    amount_paid = int((status_resp.get("data") or {}).get("amountPaid") or 0)
                    if st == "PAID" or amount_paid >= int(order["final_price"]):
                        approve_paid_order(order["order_code"], payment_ref="manual_check", source="payos_status_sync")
                        order = db.get_order(order_code)
            txt = (
                f"Mã đơn: <code>{order['order_code']}</code>\n"
                f"Trạng thái đơn: <b>{order['status']}</b>\n"
                f"Trạng thái thanh toán: <b>{order['payment_status']}</b>\n"
                f"Tool: <b>{order['tool_code']}</b>\n"
                f"Machine ID: <code>{order['machine_id']}</code>\n"
                f"Số tiền: <b>{fmt_money(order['final_price'])}</b>"
            )
            bot.answer_callback_query(call.id, f"Order {order['status']} / payment {order['payment_status']}")
            bot.send_message(call.message.chat.id, txt, reply_markup=payment_markup(order_code, order["checkout_url"]))
            return
    except Exception as e:
        bot.send_message(call.message.chat.id, f"Lỗi xử lý callback: <code>{e}</code>")

def handle_machine_id_step(message):
    user_id = message.from_user.id
    state = BUY_STATE.get(user_id)
    if not state:
        bot.send_message(message.chat.id, "Flow mua hàng đã hết hạn. Vui lòng chọn lại tool.", reply_markup=buy_menu_markup())
        return
    machine_id = norm_machine_id(message.text)
    if not is_valid_machine_id(machine_id):
        msg = bot.send_message(message.chat.id, "Machine ID chưa hợp lệ.\nChỉ nhận ký tự HEX 0-9 A-F, độ dài 16-64 ký tự.\nVí dụ:\n<code>B8A8334E67D60DCE1D38FFE40CDA3F1F</code>\n\nNhập lại Machine ID:")
        bot.register_next_step_handler(msg, handle_machine_id_step)
        return
    state["machine_id"] = machine_id
    BUY_STATE[user_id] = state
    bot.send_message(message.chat.id, f"✅ Đã nhận Machine ID:\n<code>{machine_id}</code>\n\nBạn có muốn nhập mã giảm giá không?", reply_markup=coupon_decision_markup())

def handle_coupon_step(message):
    user_id = message.from_user.id
    state = BUY_STATE.get(user_id)
    if not state:
        bot.send_message(message.chat.id, "Flow mua hàng đã hết hạn. Vui lòng thao tác lại.")
        return
    code = safe_upper(message.text)
    tool = db.get_tool(state["tool_code"])
    base_price = int(tool["price"]) * int(state["months"])
    ok, msg, discount = db.validate_coupon(user_id, code, base_price)
    if not ok:
        msg_obj = bot.send_message(message.chat.id, f"❌ {msg}\nNhập lại mã khác hoặc gửi <code>SKIP</code> để bỏ qua:")
        bot.register_next_step_handler(msg_obj, handle_coupon_retry_step)
        return
    state["coupon_code"] = code
    BUY_STATE[user_id] = state
    bot.send_message(message.chat.id, f"✅ Áp dụng mã <b>{code}</b> thành công.\nGiảm: <b>{fmt_money(discount)}</b>")
    create_order_and_show_payment(message.chat.id, user_id)

def handle_coupon_retry_step(message):
    if message.text.strip().upper() == "SKIP":
        create_order_and_show_payment(message.chat.id, message.from_user.id)
        return
    handle_coupon_step(message)

def create_order_and_show_payment(chat_id, user_id):
    state = BUY_STATE.get(user_id)
    if not state:
        bot.send_message(chat_id, "Flow mua hàng đã hết hạn.")
        return
    tool = db.get_tool(state["tool_code"])
    if not tool:
        bot.send_message(chat_id, "Tool không còn tồn tại.")
        return
    base_price = int(tool["price"]) * int(state["months"])
    discount_amount = 0
    if state.get("coupon_code"):
        ok, msg, discount_amount = db.validate_coupon(user_id, state["coupon_code"], base_price)
        if not ok:
            state["coupon_code"] = None
            BUY_STATE[user_id] = state
            bot.send_message(chat_id, f"Mã giảm giá không còn hợp lệ: {msg}\nHệ thống sẽ bỏ coupon và tiếp tục tạo đơn.")
            discount_amount = 0
    final_price = max(0, base_price - discount_amount)
    payos_info = create_payos_payment_link(final_price, "ODTMP" + secrets.token_hex(3).upper(), tool["name"])
    order_code = db.create_order(user_id, tool["code"], state["machine_id"], state["months"], base_price, state.get("coupon_code"), discount_amount, final_price, payos_info)
    order = db.get_order(order_code)
    bot.send_message(chat_id, build_payment_text(order), reply_markup=payment_markup(order_code, order["checkout_url"]))
    notify_admins(f"🧾 Đơn mới chờ thanh toán\nOrder: <code>{order['order_code']}</code>\nUser: <code>{user_id}</code>\nTool: <b>{order['tool_code']}</b>\nTháng: <b>{order['months']}</b>\nMachine ID: <code>{order['machine_id']}</code>\nCoupon: <b>{order['coupon_code'] or 'Không'}</b>\nTổng tiền: <b>{fmt_money(order['final_price'])}</b>")
    BUY_STATE.pop(user_id, None)

@bot.message_handler(commands=["addtool"])
def cmd_addtool(message):
    if not admin_only(message):
        return
    raw = message.text.replace("/addtool", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 3:
        bot.reply_to(message, "Ví dụ:\n/addtool GROKTOOL | Tool Auto Grok | 50000 | Mô tả ngắn")
        return
    code, name, price_s = parts[:3]
    desc = parts[3] if len(parts) >= 4 else ""
    if not price_s.isdigit():
        bot.reply_to(message, "Giá phải là số.")
        return
    try:
        db.add_tool(code, name, int(price_s), desc)
        bot.reply_to(message, f"Đã thêm tool <b>{safe_upper(code)}</b>.")
    except sqlite3.IntegrityError:
        bot.reply_to(message, "Mã tool đã tồn tại.")

@bot.message_handler(commands=["setprice"])
def cmd_setprice(message):
    if not admin_only(message):
        return
    raw = message.text.replace("/setprice", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) != 2 or not parts[1].isdigit():
        bot.reply_to(message, "Ví dụ:\n/setprice GROKTOOL | 59000")
        return
    db.update_tool_price(parts[0], int(parts[1]))
    bot.reply_to(message, "Đã cập nhật giá.")

@bot.message_handler(commands=["adduser"])
def cmd_adduser(message):
    if not admin_only(message):
        return
    raw = message.text.replace("/adduser", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 3:
        bot.reply_to(message, "Ví dụ:\n/adduser 123456789 | GROKTOOL | 30 | B8A8334E67D60DCE1D38FFE40CDA3F1F")
        return
    user_id_s, tool_code, days_s = parts[:3]
    machine_id = parts[3] if len(parts) >= 4 else None
    if not user_id_s.isdigit() or not re.fullmatch(r"-?\d+", days_s):
        bot.reply_to(message, "user_id và số ngày phải hợp lệ.")
        return
    if machine_id and not is_valid_machine_id(machine_id):
        bot.reply_to(message, "Machine ID không hợp lệ.")
        return
    if not db.get_tool(tool_code):
        bot.reply_to(message, "Tool code không tồn tại.")
        return
    new_exp = db.extend_license(int(user_id_s), tool_code, int(days_s), machine_id)
    bot.reply_to(message, f"Đã cấp user <code>{user_id_s}</code> tới <b>{fmt_dt(new_exp.isoformat())}</b>.")
    try:
        bot.send_message(int(user_id_s), f"✅ Bạn đã được cấp/gia hạn tool <b>{safe_upper(tool_code)}</b>\nMachine ID: <code>{norm_machine_id(machine_id) if machine_id else '-'}</code>\nHết hạn: <b>{fmt_dt(new_exp.isoformat())}</b>")
    except Exception:
        pass

@bot.message_handler(commands=["extend"])
def cmd_extend(message):
    if not admin_only(message):
        return
    raw = message.text.replace("/extend", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 3:
        bot.reply_to(message, "Ví dụ:\n/extend 123456789 | GROKTOOL | 30 | B8A8334E67D60DCE1D38FFE40CDA3F1F")
        return
    user_id_s, tool_code, days_s = parts[:3]
    machine_id = parts[3] if len(parts) >= 4 else None
    if not user_id_s.isdigit() or not re.fullmatch(r"-?\d+", days_s):
        bot.reply_to(message, "user_id và số ngày phải hợp lệ.")
        return
    if machine_id and not is_valid_machine_id(machine_id):
        bot.reply_to(message, "Machine ID không hợp lệ.")
        return
    if not db.get_tool(tool_code):
        bot.reply_to(message, "Tool code không tồn tại.")
        return
    new_exp = db.extend_license(int(user_id_s), tool_code, int(days_s), machine_id)
    bot.reply_to(message, f"Đã gia hạn tới <b>{fmt_dt(new_exp.isoformat())}</b>.")
    try:
        bot.send_message(int(user_id_s), f"🎉 Tool <b>{safe_upper(tool_code)}</b> của bạn đã được gia hạn.\nMachine ID: <code>{norm_machine_id(machine_id) if machine_id else '-'}</code>\nHết hạn mới: <b>{fmt_dt(new_exp.isoformat())}</b>")
    except Exception:
        pass

@bot.message_handler(commands=["coupon"])
def cmd_coupon(message):
    if not admin_only(message):
        return
    raw = message.text.replace("/coupon", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) != 5:
        bot.reply_to(message, "Ví dụ:\n/coupon SALE10 | percent | 10 | 100 | 2026-12-31\nhoặc\n/coupon KM50K | fixed | 50000 | 20 | -")
        return
    code, dtype, value_s, max_uses_s, exp_date = parts
    dtype = dtype.lower()
    if dtype not in {"percent", "fixed"}:
        bot.reply_to(message, "discount_type chỉ nhận percent hoặc fixed.")
        return
    if not value_s.isdigit() or not max_uses_s.isdigit():
        bot.reply_to(message, "value và max_uses phải là số.")
        return
    expires_at = None
    if exp_date != "-":
        try:
            expires_at = datetime.strptime(exp_date, "%Y-%m-%d").replace(tzinfo=TZ, hour=23, minute=59, second=59).isoformat()
        except ValueError:
            bot.reply_to(message, "Ngày hết hạn phải dạng YYYY-MM-DD hoặc dùng -")
            return
    try:
        db.add_coupon(code, dtype, int(value_s), int(max_uses_s), expires_at)
        bot.reply_to(message, f"Đã tạo coupon <b>{safe_upper(code)}</b>.")
    except sqlite3.IntegrityError:
        bot.reply_to(message, "Coupon đã tồn tại.")

@bot.message_handler(commands=["approve"])
def cmd_approve(message):
    if not admin_only(message):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Ví dụ: /approve ODAB12CD")
        return
    ok, msg = approve_paid_order(parts[1], source="admin")
    bot.reply_to(message, msg)

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not admin_only(message):
        return
    content = message.text.replace("/broadcast", "", 1).strip()
    if not content:
        bot.reply_to(message, "Ví dụ:\n/broadcast Shop đang có ưu đãi gia hạn hôm nay")
        return
    sent = 0
    failed = 0
    for uid in db.all_user_ids():
        try:
            bot.send_message(uid, content)
            sent += 1
        except Exception:
            failed += 1
            db.mark_blocked(uid)
    bot.reply_to(message, f"Broadcast xong.\nThành công: {sent}\nThất bại: {failed}")

def handle_admin_broadcast(message):
    if not admin_only(message):
        return
    sent = 0
    failed = 0
    for uid in db.all_user_ids():
        try:
            bot.send_message(uid, message.text)
            sent += 1
        except Exception:
            failed += 1
            db.mark_blocked(uid)
    bot.send_message(message.chat.id, f"Broadcast xong.\nThành công: {sent}\nThất bại: {failed}")

@bot.message_handler(commands=["run_reminders"])
def cmd_run_reminders(message):
    if not admin_only(message):
        return
    process_expiry_reminders()
    bot.reply_to(message, "Đã chạy nhắc hạn xong.")

@bot.message_handler(func=lambda m: True, content_types=["text"])
def fallback(message):
    bot.send_message(message.chat.id, "Chọn menu bên dưới hoặc dùng /help để xem lệnh.", reply_markup=main_menu_markup(message.from_user.id))

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "payos_webhook_path": PAYOS_WEBHOOK_PATH,
        "payment_return": "/payment-return",
        "public_base_url": PUBLIC_BASE_URL,
        "server_time": iso_now()
    })

@app.route(PAYOS_WEBHOOK_PATH, methods=["POST"])
def payos_webhook():
    payload = request.get_json(silent=True) or {}
    if not verify_payos_webhook_signature(payload):
        return jsonify({"error": 1, "message": "invalid signature"}), 400

    data = payload.get("data") or {}
    payos_order_code = data.get("orderCode")
    amount = int(data.get("amount") or data.get("amountPaid") or 0)
    payment_ref = str(data.get("reference") or data.get("paymentLinkId") or data.get("code") or "")
    desc = str(data.get("description") or "")
    status = str(data.get("status") or "").upper()

    order = db.get_order_by_payos_code(payos_order_code)
    if not order:
        return jsonify({"error": 0, "message": "order not found but webhook accepted"})

    if order["status"] == "paid":
        return jsonify({"error": 0, "message": "already paid"})

    if status == "PAID" or amount >= int(order["final_price"]) or (desc and order["order_code"][:8] in desc):
        ok, msg = approve_paid_order(order["order_code"], payment_ref=payment_ref, source="payos_webhook")
        return jsonify({"error": 0 if ok else 1, "message": msg})

    db.update_order_payment(order["order_code"], status.lower() if status else "pending", payment_ref, paid=False)
    return jsonify({"error": 0, "message": f"status updated {status or 'PENDING'}"})

@app.route("/payment-return", methods=["GET"])
def payment_return():
    order_code = request.args.get("order_code", "")
    order = db.get_order(order_code) if order_code else None
    if not order:
        return "Order không tồn tại.", 404
    return f"Đơn {order['order_code']} | trạng thái {order['status']} | payment {order['payment_status']}"

@app.route("/payment-cancel", methods=["GET"])
def payment_cancel():
    return "Thanh toán đã bị hủy."

def run_flask():
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    confirm_payos_webhook_url()
    notify_admins("🤖 Bot bán tool payOS đang khởi động.")
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=reminder_loop, daemon=True).start()
    bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
