import asyncio
import json
import os
import random
import string
import sqlite3
import tempfile
import time
import hashlib
import aiohttp
import logging
import html
import platform
import sys
import signal
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)
from aiogram.exceptions import TelegramBadRequest
import resvg_py

try:
    from groq import Groq
except Exception:
    Groq = None

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
TOKEN = "8788323258:AAESyyBf_-S2MHuklb0bTJrgls_am0Wazm4"
GROQ_API_KEY = "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"
CRYPTOBOT_TOKEN = "555759:AAzSWk3aRAtKoZ9Aq7egw7mgvY33g4roLGU"
AI_MODEL_EASY = "meta-llama/llama-prompt-guard-2-86m"
AI_MODEL_NORMAL = "llama-3.3-70b-versatile"
AI_MODEL_HARD = "openai/gpt-oss-120b"
ADMIN_USER_ID = 7053001262
ADMIN_USER_IDS = {7053001262, 7719220317}
STATS_DB_PATH = Path("casino_stats.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres.jqiomtvtvtsizubzunhb:My%20happy%20life64@aws-1-eu-north-1.pooler.supabase.com:5432/postgres").strip()
REQUIRED_CHANNEL = os.environ.get("REQUIRED_CHANNEL", "-1003691561522").strip()
REQUIRED_CHANNEL_URL = os.environ.get("REQUIRED_CHANNEL_URL", "https://t.me/durak_cart_channel").strip()
NEWS_CHANNEL_URL = os.environ.get("NEWS_CHANNEL_URL", "https://t.me/durak_cart_channel").strip()
LOBBY_IDLE_TIMEOUT = 300  # 5 минут
AFK_PROMPT_DELAY = 120    # 2 минуты без хода
AFK_PROMPT_WINDOW = 60    # окно подтверждения (до 3-й минуты)
AFK_FORFEIT_DELAY = 180   # 3 минуты без хода

AFK_MAX_PROMPTS = 2

# Ставки в USD (фиксированные)
STAKE_AMOUNTS = {
    1.00: {"rub": 80, "pool": 2.00, "commission": 0.3125, "winner": 1.6875},
    1.50: {"rub": 120, "pool": 3.00, "commission": 0.4375, "winner": 2.5625},
    2.50: {"rub": 200, "pool": 5.00, "commission": 0.6250, "winner": 4.3750},
    0.02: {"rub": 2, "pool": 0.04, "commission": 0.005, "winner": 0.035, "name": "Проверь удачу"},
}

CRYPTOBOT_FEE_PERCENT = 3.0
PAYMENT_TIMEOUT = 900  # 15 минут
PAYMENT_CHECK_INTERVAL = 3

# =============================================================================
# ЦВЕТА
# =============================================================================
COLORS = ["red", "blue", "green", "yellow"]
COLOR_EMOJI = {
    "red": "\U0001f7e5",
    "blue": "\U0001f7e6",
    "green": "\U0001f7e9",
    "yellow": "\U0001f7e8",
    "black": "\u2b1b",
}
COLOR_NAME_RU = {
    "red": "красный",
    "blue": "синий",
    "green": "зелёный",
    "yellow": "жёлтый",
    "black": "чёрный",
}

# =============================================================================
# УТИЛИТЫ
# =============================================================================
MENU_BUTTON_STYLES = ("primary", "success", "danger")
def now_ts() -> float:
    return time.time()

START_TS = now_ts()
SUBSCRIPTION_CACHE: Dict[int, float] = {}
SUBSCRIPTION_TTL = 300.0
NOTICE_CACHE: Dict[str, Optional[object]] = {"ts": 0.0, "text": None}
USER_SETTINGS_CACHE: Dict[int, Tuple[float, dict]] = {}
USER_SETTINGS_TTL = 60.0
SHUTDOWN_EVENT = asyncio.Event()

def gen_code(k: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(k))

def log_message(message: str):
    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def check_db_connection() -> None:
    try:
        with db._get_connection() as conn:
            cur = conn.cursor()
            if db.db_kind == "postgres":
                cur.execute("SELECT 1")
            else:
                cur.execute("SELECT 1")
            cur.fetchone()
        log_message(f"DB подключение OK ({db.db_kind})")
    except Exception as e:
        log_message(f"DB подключение НЕ удалось ({db.db_kind}): {e}")


def validate_config() -> bool:
    ok = True
    if not TOKEN:
        log_message("ОШИБКА: BOT_TOKEN не задан. Бот не может быть запущен.")
        ok = False
    if not DATABASE_URL:
        log_message("Внимание: DATABASE_URL не задан — будет использоваться локальная SQLite.")
    if not CRYPTOBOT_TOKEN:
        log_message("Внимание: CRYPTOBOT_TOKEN не задан — ставки будут отключены.")
        try:
            db.set_bool_setting("betting_enabled", False)
        except Exception:
            pass
    if not REQUIRED_CHANNEL:
        log_message("Внимание: REQUIRED_CHANNEL не задан — доступ без проверки подписки.")
    return ok

# =============================================================================
# БАЗА ДАННЫХ (УМНАЯ, ВСЁ В ОДНОМ ФАЙЛЕ)
# =============================================================================
class Database:
    _instance: Optional['Database'] = None
    
    def __new__(cls) -> 'Database':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self.db_path = STATS_DB_PATH
        self.db_url = DATABASE_URL
        self.db_kind = "postgres" if self.db_url else "sqlite"
        self._init_db()
        self._initialized = True
        log_message("База данных инициализирована")
    
    def _get_connection(self):
        if self.db_kind == "postgres":
            try:
                import psycopg2
                import psycopg2.extras
            except Exception as e:
                raise RuntimeError("psycopg2 is required for PostgreSQL") from e
            class AdaptCursor(psycopg2.extras.DictCursor):
                def execute(self, query, vars=None):
                    query = query.replace("?", "%s")
                    return super().execute(query, vars)
            conn_kwargs = {"cursor_factory": AdaptCursor}
            if self.db_url and "sslmode=" not in self.db_url:
                conn_kwargs["sslmode"] = "require"
            conn = psycopg2.connect(self.db_url, **conn_kwargs)
            return conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _adapt_sql(self, sql: str) -> str:
        if self.db_kind == "postgres":
            return sql.replace("?", "%s")
        return sql

    def _exec(self, cursor, sql: str, params: tuple = ()):
        cursor.execute(self._adapt_sql(sql), params)
        return cursor
    
    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.db_kind == "postgres":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_events (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    ts DOUBLE PRECISION NOT NULL
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kv_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_matches (
                    match_id TEXT PRIMARY KEY,
                    lobby_id TEXT NOT NULL,
                    stake_amount DOUBLE PRECISION NOT NULL,
                    player1_id BIGINT NOT NULL,
                    player2_id BIGINT NOT NULL,
                    winner_id BIGINT,
                    status TEXT DEFAULT 'waiting_payment',
                    commission_amount DOUBLE PRECISION,
                    payout_amount DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    crypto_invoice_id_p1 TEXT,
                    crypto_invoice_id_p2 TEXT,
                    payout_check_hash TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_payments (
                    id SERIAL PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    invoice_id TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    status TEXT DEFAULT 'pending',
                    paid_at TIMESTAMP,
                    refunded_at TIMESTAMP,
                    crypto_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_payouts (
                    id SERIAL PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    check_hash TEXT,
                    check_url TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    claimed_at TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_balances (
                    user_id BIGINT PRIMARY KEY,
                    balance DOUBLE PRECISION DEFAULT 0.0,
                    total_deposited DOUBLE PRECISION DEFAULT 0.0,
                    total_withdrawn DOUBLE PRECISION DEFAULT 0.0,
                    total_won DOUBLE PRECISION DEFAULT 0.0,
                    total_lost DOUBLE PRECISION DEFAULT 0.0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id BIGINT PRIMARY KEY,
                    show_card_photos INTEGER DEFAULT 1,
                    allow_broadcast INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_broadcasts (
                    id SERIAL PRIMARY KEY,
                    text TEXT NOT NULL,
                    start_at DOUBLE PRECISION NOT NULL,
                    end_at DOUBLE PRECISION,
                    status TEXT DEFAULT 'scheduled',
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    activated_at DOUBLE PRECISION,
                    stopped_at DOUBLE PRECISION
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    text TEXT NOT NULL,
                    category TEXT DEFAULT 'Другое',
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS support_messages (
                    id SERIAL PRIMARY KEY,
                    ticket_id INTEGER NOT NULL,
                    sender_id BIGINT NOT NULL,
                    is_admin INTEGER DEFAULT 0,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            # Существующие таблицы
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    ts REAL NOT NULL
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kv_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            # Новые таблицы для ставок
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_matches (
                    match_id TEXT PRIMARY KEY,
                    lobby_id TEXT NOT NULL,
                    stake_amount REAL NOT NULL,
                    player1_id INTEGER NOT NULL,
                    player2_id INTEGER NOT NULL,
                    winner_id INTEGER,
                    status TEXT DEFAULT 'waiting_payment',
                    commission_amount REAL,
                    payout_amount REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    crypto_invoice_id_p1 TEXT,
                    crypto_invoice_id_p2 TEXT,
                    payout_check_hash TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    invoice_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    paid_at TIMESTAMP,
                    refunded_at TIMESTAMP,
                    crypto_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS betting_payouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    check_hash TEXT,
                    check_url TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    claimed_at TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_balances (
                    user_id INTEGER PRIMARY KEY,
                    balance REAL DEFAULT 0.0,
                    total_deposited REAL DEFAULT 0.0,
                    total_withdrawn REAL DEFAULT 0.0,
                    total_won REAL DEFAULT 0.0,
                    total_lost REAL DEFAULT 0.0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER PRIMARY KEY,
                    show_card_photos INTEGER DEFAULT 1,
                    allow_broadcast INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text TEXT NOT NULL,
                    start_at REAL NOT NULL,
                    end_at REAL,
                    status TEXT DEFAULT 'scheduled',
                    created_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    activated_at REAL,
                    stopped_at REAL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    text TEXT NOT NULL,
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS support_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender_id INTEGER NOT NULL,
                    is_admin INTEGER DEFAULT 0,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        # Индексы
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_events_type_ts ON user_events(event_type, ts)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_events_user ON user_events(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_betting_matches_status ON betting_matches(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_betting_payments_status ON betting_payments(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON support_tickets(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_broadcasts_status ON admin_broadcasts(status)")
        
        # Настройки по умолчанию
        if self.db_kind == "postgres":
            self._exec(
                cursor,
                "INSERT INTO kv_settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING",
                ("admin_msg_enabled", "1"),
            )
            self._exec(
                cursor,
                "INSERT INTO kv_settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING",
                ("betting_enabled", "1"),
            )
        else:
            cursor.execute(
                "INSERT OR IGNORE INTO kv_settings (key, value) VALUES (?, ?)",
                ("admin_msg_enabled", "1"),
            )
            cursor.execute(
                "INSERT OR IGNORE INTO kv_settings (key, value) VALUES (?, ?)",
                ("betting_enabled", "1"),
            )
        
        conn.commit()
        conn.close()

        self._ensure_column("support_tickets", "category", "TEXT DEFAULT 'Другое'")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                if self.db_kind == "postgres":
                    self._exec(
                        cur,
                        "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
                        (table, column),
                    )
                    if cur.fetchone():
                        return
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                else:
                    cur = conn.execute(f"PRAGMA table_info({table})")
                    cols = {row[1] for row in cur.fetchall()}
                    if column in cols:
                        return
                    cur = conn.cursor()
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                conn.commit()
        except Exception:
            pass
    
    # Существующие методы
    def record_user_event(self, user_id: int, event_type: str) -> None:
        if user_id <= 0:
            return
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                self._exec(
                    cur,
                    "INSERT INTO user_events (user_id, event_type, ts) VALUES (?, ?, ?)",
                    (user_id, event_type, now_ts()),
                )
                conn.commit()
        except Exception:
            pass
    
    def count_unique_users(self, event_type: str, since_ts: float) -> int:
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                self._exec(
                    cur,
                    "SELECT COUNT(DISTINCT user_id) FROM user_events WHERE event_type = ? AND ts >= ?",
                    (event_type, since_ts),
                )
                row = cur.fetchone()
                return int(row[0] or 0)
        except Exception:
            return 0
    
    def get_setting(self, key: str, default: str) -> str:
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                self._exec(cur, "SELECT value FROM kv_settings WHERE key = ?", (key,))
                row = cur.fetchone()
                return row[0] if row else default
        except Exception:
            return default
    
    def set_setting(self, key: str, value: str) -> None:
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                self._exec(
                    cur,
                    "INSERT INTO kv_settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )
                conn.commit()
        except Exception:
            pass
    
    def get_bool_setting(self, key: str, default: bool = True) -> bool:
        val = self.get_setting(key, "1" if default else "0")
        return val == "1"
    
    def set_bool_setting(self, key: str, value: bool) -> None:
        self.set_setting(key, "1" if value else "0")
    
    # Новые методы для ставок
    def create_betting_match(self, match_id: str, lobby_id: str, stake: float,
                            player1: int, player2: int) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        info = STAKE_AMOUNTS.get(stake) or STAKE_AMOUNTS.get(round(stake, 2))
        if info:
            commission = float(info.get("commission", stake * 2 * 0.12))
            payout = float(info.get("winner", stake * 2 - commission))
        else:
            commission = stake * 2 * 0.12
            payout = stake * 2 - commission
        
        try:
            cursor.execute("""
                INSERT INTO betting_matches (match_id, lobby_id, stake_amount,
                    player1_id, player2_id, status, commission_amount, payout_amount)
                VALUES (?, ?, ?, ?, ?, 'waiting_payment', ?, ?)
            """, (match_id, lobby_id, stake, player1, player2, commission, payout))
            conn.commit()
            return True
        except Exception as e:
            log_message(f"Ошибка создания матча: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def update_match_invoice(self, match_id: str, user_id: int, invoice_id: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT player1_id FROM betting_matches WHERE match_id = ?", (match_id,)
            )
            is_p1 = cursor.fetchone()
            
            if is_p1 and is_p1[0] == user_id:
                cursor.execute(
                    "UPDATE betting_matches SET crypto_invoice_id_p1 = ? WHERE match_id = ?",
                    (invoice_id, match_id)
                )
            else:
                cursor.execute(
                    "UPDATE betting_matches SET crypto_invoice_id_p2 = ? WHERE match_id = ?",
                    (invoice_id, match_id)
                )
            conn.commit()
            return True
        except Exception as e:
            log_message(f"Ошибка обновления инвойса: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def create_payment_record(self, match_id: str, user_id: int,
                             invoice_id: str, amount: float) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.db_kind == "postgres":
                self._exec(
                    cursor,
                    """
                    INSERT INTO betting_payments (match_id, user_id, invoice_id, amount, status)
                    VALUES (?, ?, ?, ?, 'pending') RETURNING id
                    """,
                    (match_id, user_id, invoice_id, amount),
                )
                payment_id = cursor.fetchone()[0]
            else:
                cursor.execute("""
                    INSERT INTO betting_payments (match_id, user_id, invoice_id, amount, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (match_id, user_id, invoice_id, amount))
                payment_id = cursor.lastrowid
            conn.commit()
            return payment_id
        except Exception as e:
            log_message(f"Ошибка создания платежа: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def confirm_payment(self, invoice_id: str, crypto_hash: str) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, match_id, user_id, amount FROM betting_payments
                WHERE invoice_id = ? AND status = 'pending'
            """, (invoice_id,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            payment_id, match_id, user_id, amount = row
            
            cursor.execute("""
                UPDATE betting_payments SET status = 'paid', paid_at = CURRENT_TIMESTAMP, crypto_hash = ?
                WHERE id = ?
            """, (crypto_hash, payment_id))
            
            cursor.execute(
                "SELECT player1_id, player2_id, status FROM betting_matches WHERE match_id = ?",
                (match_id,)
            )
            match = cursor.fetchone()
            
            if match:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM betting_payments
                    WHERE match_id = ? AND user_id = ? AND status = 'paid'
                    """,
                    (match_id, match[0]),
                )
                p1_paid = cursor.fetchone()[0] > 0
                
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM betting_payments
                    WHERE match_id = ? AND user_id = ? AND status = 'paid'
                    """,
                    (match_id, match[1]),
                )
                p2_paid = cursor.fetchone()[0] > 0
                
                if p1_paid and p2_paid and match[2] == 'waiting_payment':
                    cursor.execute(
                        "UPDATE betting_matches SET status = 'ready_to_start' WHERE match_id = ?",
                        (match_id,)
                    )
            
            conn.commit()
            
            return {"user_id": user_id, "amount": amount, "match_id": match_id}
        except Exception as e:
            log_message(f"Ошибка подтверждения платежа: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def finish_match(self, match_id: str, winner_id: int) -> Optional[float]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE betting_matches SET status = 'finished', winner_id = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE match_id = ? AND status IN ('ready_to_start', 'playing')
            """, (winner_id, match_id))
            
            if cursor.rowcount == 0:
                conn.close()
                return None
            
            cursor.execute("""
                SELECT payout_amount, commission_amount, player1_id, player2_id
                FROM betting_matches WHERE match_id = ?
            """, (match_id,))
            result = cursor.fetchone()
            
            if result:
                payout, commission, p1, p2 = result
                
                cursor.execute("""
                    UPDATE user_balances SET
                        balance = balance + ?,
                        total_won = total_won + ?
                    WHERE user_id = ?
                """, (payout, payout, winner_id))
                
                loser_id = p2 if winner_id == p1 else p1
                cursor.execute("""
                    UPDATE user_balances SET
                        total_lost = total_lost + ?
                    WHERE user_id = ?
                """, (payout / 2, loser_id))
                
                conn.commit()
                conn.close()
                return payout
            else:
                conn.close()
                return None
        except Exception as e:
            log_message(f"Ошибка завершения матча: {e}")
            conn.rollback()
            conn.close()
            return None

    def finish_match_no_winner(self, match_id: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE betting_matches
                SET status = 'finished', winner_id = NULL, finished_at = CURRENT_TIMESTAMP
                WHERE match_id = ? AND status IN ('ready_to_start', 'playing')
            """, (match_id,))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            log_message(f"Ошибка завершения матча без победителя: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def create_payout_check(self, match_id: str, user_id: int,
                           amount: float, check_hash: str, check_url: str) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.db_kind == "postgres":
                self._exec(
                    cursor,
                    """
                    INSERT INTO betting_payouts (match_id, user_id, amount, check_hash, check_url, status)
                    VALUES (?, ?, ?, ?, ?, 'pending') RETURNING id
                    """,
                    (match_id, user_id, amount, check_hash, check_url),
                )
                payout_id = cursor.fetchone()[0]
            else:
                cursor.execute("""
                    INSERT INTO betting_payouts (match_id, user_id, amount, check_hash, check_url, status)
                    VALUES (?, ?, ?, ?, ?, 'pending')
                """, (match_id, user_id, amount, check_hash, check_url))
                payout_id = cursor.lastrowid
            conn.commit()
            return payout_id
        except Exception as e:
            log_message(f"Ошибка создания выплаты: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def has_payout(self, match_id: str, user_id: int) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM betting_payouts WHERE match_id = ? AND user_id = ?",
                (match_id, user_id)
            )
            return cursor.fetchone()[0] > 0
        finally:
            conn.close()
    def get_user_balance(self, user_id: int) -> float:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT balance FROM user_balances WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return row['balance'] if row else 0.0
        finally:
            conn.close()
    
    def update_user_balance(self, user_id: int, amount: float) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO user_balances (user_id, balance) VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    balance = balance + ?,
                    last_updated = CURRENT_TIMESTAMP
            """, (user_id, amount, amount))
            conn.commit()
            return True
        except Exception as e:
            log_message(f"Ошибка обновления баланса: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def refund_payment(self, match_id: str, user_id: int) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE betting_payments SET status = 'refunded', refunded_at = CURRENT_TIMESTAMP
                WHERE match_id = ? AND user_id = ? AND status IN ('pending', 'paid')
            """, (match_id, user_id))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            log_message(f"Ошибка возврата: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_match_by_lobby(self, lobby_id: str) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT * FROM betting_matches WHERE lobby_id = ? AND status IN ('waiting_payment', 'ready_to_start', 'playing')
            """, (lobby_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_match_by_id(self, match_id: str) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM betting_matches WHERE match_id = ?", (match_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_admin_stats(self) -> dict:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            stats = {}
            
            cursor.execute("SELECT SUM(commission_amount) FROM betting_matches WHERE status = 'finished'")
            stats['total_commission'] = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT COUNT(*) FROM betting_matches WHERE status = 'finished'")
            stats['total_matches'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM betting_matches WHERE status IN ('waiting_payment', 'ready_to_start', 'playing')")
            stats['active_matches'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(balance) FROM user_balances")
            stats['total_user_balance'] = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT COUNT(*) FROM user_balances")
            stats['total_users'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(amount) FROM betting_payouts WHERE status = 'pending'")
            stats['pending_payouts'] = cursor.fetchone()[0] or 0

            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'")
            stats['open_tickets'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM admin_broadcasts WHERE status = 'active'")
            stats['active_broadcasts'] = cursor.fetchone()[0]
            
            return stats
        finally:
            conn.close()

    def ping_ms(self) -> Tuple[bool, Optional[float], Optional[str]]:
        conn = None
        try:
            start = time.perf_counter()
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            ms = (time.perf_counter() - start) * 1000.0
            return True, ms, None
        except Exception as e:
            return False, None, str(e)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    def reset_all_data(self) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        tables = [
            "support_messages",
            "support_tickets",
            "betting_payouts",
            "betting_payments",
            "betting_matches",
            "user_events",
            "user_balances",
            "user_settings",
            "admin_broadcasts",
            "kv_settings",
        ]
        try:
            for t in tables:
                cursor.execute(f"DELETE FROM {t}")
            # Базовые настройки
            self._exec(
                cursor,
                "INSERT INTO kv_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                ("admin_msg_enabled", "1"),
            )
            self._exec(
                cursor,
                "INSERT INTO kv_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                ("betting_enabled", "1"),
            )
            conn.commit()
            return True
        except Exception as e:
            log_message(f"Ошибка очистки БД: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def cleanup_older_than(self, seconds: float) -> Dict[str, int]:
        conn = self._get_connection()
        cursor = conn.cursor()
        counts: Dict[str, int] = {}
        seconds_i = max(1, int(round(seconds)))
        cutoff_ts = now_ts() - seconds_i
        try:
            cursor.execute("DELETE FROM user_events WHERE ts < ?", (cutoff_ts,))
            counts["user_events"] = cursor.rowcount

            if self.db_kind == "postgres":
                cursor.execute(
                    "DELETE FROM support_messages WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["support_messages"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM support_tickets WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["support_tickets"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_payouts WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["betting_payouts"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_payments WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["betting_payments"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_matches WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["betting_matches"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM admin_broadcasts WHERE created_at < NOW() - (%s * INTERVAL '1 second')",
                    (seconds_i,),
                )
                counts["admin_broadcasts"] = cursor.rowcount
            else:
                delta = f"-{seconds_i} seconds"
                cursor.execute(
                    "DELETE FROM support_messages WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["support_messages"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM support_tickets WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["support_tickets"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_payouts WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["betting_payouts"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_payments WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["betting_payments"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM betting_matches WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["betting_matches"] = cursor.rowcount
                cursor.execute(
                    "DELETE FROM admin_broadcasts WHERE created_at < datetime('now', ?)",
                    (delta,),
                )
                counts["admin_broadcasts"] = cursor.rowcount

            conn.commit()
            return counts
        except Exception as e:
            log_message(f"Ошибка очистки БД по времени: {e}")
            conn.rollback()
            return {}
        finally:
            conn.close()

    def get_user_settings(self, user_id: int) -> dict:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM user_settings WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return {"user_id": user_id, "show_card_photos": 1, "allow_broadcast": 1}
        finally:
            conn.close()

    def set_user_setting(self, user_id: int, key: str, value: int) -> None:
        if key not in ("show_card_photos", "allow_broadcast"):
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO user_settings (user_id, show_card_photos, allow_broadcast) "
                "VALUES (?, 1, 1) ON CONFLICT(user_id) DO NOTHING",
                (user_id,),
            )
            cursor.execute(
                f"UPDATE user_settings SET {key} = ? WHERE user_id = ?",
                (value, user_id),
            )
            conn.commit()
        except Exception as e:
            log_message(f"Ошибка настроек пользователя: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_user_profile(self, user_id: int) -> dict:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM user_balances WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return {
                "user_id": user_id,
                "balance": 0.0,
                "total_deposited": 0.0,
                "total_withdrawn": 0.0,
                "total_won": 0.0,
                "total_lost": 0.0,
            }
        finally:
            conn.close()

    def get_all_user_ids(self) -> List[int]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DISTINCT user_id FROM user_events
                UNION SELECT user_id FROM user_balances
                UNION SELECT user_id FROM support_tickets
                """
            )
            rows = cursor.fetchall()
            return [int(r[0]) for r in rows if r and r[0]]
        finally:
            conn.close()

    def create_broadcast(self, text: str, start_at: float, end_at: Optional[float], created_by: int) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.db_kind == "postgres":
                self._exec(
                    cursor,
                    "INSERT INTO admin_broadcasts (text, start_at, end_at, status, created_by) "
                    "VALUES (?, ?, ?, 'scheduled', ?) RETURNING id",
                    (text, start_at, end_at, created_by),
                )
                bid = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO admin_broadcasts (text, start_at, end_at, status, created_by) "
                    "VALUES (?, ?, ?, 'scheduled', ?)",
                    (text, start_at, end_at, created_by),
                )
                bid = cursor.lastrowid
            conn.commit()
            return int(bid)
        except Exception as e:
            log_message(f"Ошибка создания уведомления: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def activate_due_broadcasts(self, now: float) -> List[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM admin_broadcasts WHERE status = 'scheduled' AND start_at <= ?",
                (now,),
            )
            rows = cursor.fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                cursor.execute(
                    f"UPDATE admin_broadcasts SET status = 'active', activated_at = ? "
                    f"WHERE id IN ({','.join('?' for _ in ids)})",
                    (now, *ids),
                )
            conn.commit()
            return [dict(r) for r in rows]
        except Exception as e:
            log_message(f"Ошибка активации уведомлений: {e}")
            conn.rollback()
            return []
        finally:
            conn.close()

    def expire_broadcasts(self, now: float) -> None:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE admin_broadcasts SET status = 'ended' "
                "WHERE status = 'active' AND end_at IS NOT NULL AND end_at <= ?",
                (now,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            conn.close()

    def stop_all_broadcasts(self) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE admin_broadcasts SET status = 'stopped', stopped_at = ? "
                "WHERE status IN ('scheduled', 'active')",
                (now_ts(),),
            )
            conn.commit()
            return cursor.rowcount
        except Exception:
            conn.rollback()
            return 0
        finally:
            conn.close()

    def get_broadcast_stats(self) -> dict:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            stats = {}
            cursor.execute("SELECT COUNT(*) FROM admin_broadcasts WHERE status = 'scheduled'")
            stats["scheduled"] = cursor.fetchone()[0] or 0
            cursor.execute("SELECT COUNT(*) FROM admin_broadcasts WHERE status = 'active'")
            stats["active"] = cursor.fetchone()[0] or 0
            return stats
        finally:
            conn.close()

    def get_active_broadcast(self, now: float) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT * FROM admin_broadcasts
                WHERE status = 'active'
                AND (end_at IS NULL OR end_at > ?)
                ORDER BY activated_at DESC
                LIMIT 1
                """,
                (now,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def create_support_ticket(self, user_id: int, username: str, text: str, category: str) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.db_kind == "postgres":
                self._exec(
                    cursor,
                    "INSERT INTO support_tickets (user_id, username, text, category, status) "
                    "VALUES (?, ?, ?, ?, 'open') RETURNING id",
                    (user_id, username, text, category),
                )
                ticket_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO support_tickets (user_id, username, text, category, status) VALUES (?, ?, ?, ?, 'open')",
                    (user_id, username, text, category),
                )
                ticket_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO support_messages (ticket_id, sender_id, is_admin, text) VALUES (?, ?, 0, ?)",
                (ticket_id, user_id, text),
            )
            conn.commit()
            return int(ticket_id)
        except Exception as e:
            log_message(f"Ошибка создания тикета: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    def add_support_message(self, ticket_id: int, sender_id: int, is_admin: bool, text: str) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO support_messages (ticket_id, sender_id, is_admin, text) VALUES (?, ?, ?, ?)",
                (ticket_id, sender_id, 1 if is_admin else 0, text),
            )
            conn.commit()
            return True
        except Exception as e:
            log_message(f"Ошибка сообщения тикета: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def close_support_ticket(self, ticket_id: int) -> bool:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE support_tickets SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (ticket_id,),
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            log_message(f"Ошибка закрытия тикета: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_support_ticket(self, ticket_id: int) -> Optional[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM support_tickets WHERE id = ?", (ticket_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_support_tickets(self, limit: int = 10, status: Optional[str] = None) -> List[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if status:
                cursor.execute(
                    "SELECT * FROM support_tickets WHERE status = ? ORDER BY id DESC LIMIT ?",
                    (status, limit),
                )
            else:
                cursor.execute(
                    "SELECT * FROM support_tickets ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_support_messages(self, ticket_id: int, limit: int = 5) -> List[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT sender_id, is_admin, text, created_at
                FROM support_messages
                WHERE ticket_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (ticket_id, limit),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows][::-1]
        finally:
            conn.close()

    def get_recent_matches(self, limit: int = 10) -> List[dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT match_id, stake_amount, status, winner_id, created_at, finished_at
                FROM betting_matches
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

db = Database()

# =============================================================================
# CRYPTOBOT API CLIENT
# =============================================================================
class CryptoBotClient:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://pay.crypt.bot/api"
        self.enabled = bool(token)
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _request(self, method: str, data: dict = None) -> dict:
        session = await self._get_session()
        headers = {
            "Crypto-Pay-API-Token": self.token,
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/{method}"
        
        try:
            async with session.post(url, json=data or {}, headers=headers, timeout=30) as response:
                result = await response.json()
                if result.get("ok"):
                    return result.get("result", {})
                else:
                    error = result.get("error", "Unknown error")
                    log_message(f"CryptoBot API error: {error}")
                    raise Exception(f"{error} (status: {response.status})")
        except aiohttp.ClientError as e:
            log_message(f"CryptoBot network error: {e}")
            raise
        except Exception as e:
            log_message(f"CryptoBot request error: {e}")
            raise
    
    async def create_invoice(self, amount: float, asset: str = "USDT",
                            description: str = "", expires_in: int = 900) -> dict:
        data = {
            "amount": str(amount),
            "asset": asset,
            "currency_type": "crypto",
            "description": description[:1024] if description else "",
            "expires_in": expires_in,
            "allow_comments": False,
            "allow_anonymous": True,
        }
        return await self._request("createInvoice", data)
    
    async def get_invoices(self, invoice_id: int = None, count: int = 10) -> list:
        data = {
            "invoice_ids": str(invoice_id) if invoice_id else None,
            "count": min(count, 1000),
        }
        result = await self._request("getInvoices", data)
        if isinstance(result, dict):
            items = result.get("items")
            return items if isinstance(items, list) else []
        return result if isinstance(result, list) else []
    
    async def check_payment(self, invoice_id: int) -> Tuple[bool, Optional[str]]:
        try:
            invoices = await self.get_invoices(invoice_id, count=1)
            if invoices and len(invoices) > 0:
                invoice = invoices[0]
                status = invoice.get("status", "")
                if status == "paid":
                    return True, invoice.get("hash", "")
                elif status in ["expired", "canceled"]:
                    return False, None
            return False, None
        except Exception as e:
            log_message(f"Ошибка проверки оплаты: {e}")
            return False, None
    
    async def create_check(self, asset: str, amount: float,
                          description: str = "") -> dict:
        data = {
            "asset": asset,
            "amount": str(amount),
            "description": description[:1024] if description else "",
        }
        return await self._request("createCheck", data)

cryptobot = CryptoBotClient(CRYPTOBOT_TOKEN)

# =============================================================================
# КАРТЫ / МОДЕЛИ
# =============================================================================
class Suit(str, Enum):
    clubs = "clubs"
    diamonds = "diamonds"
    hearts = "hearts"
    spades = "spades"
    
    @property
    def symbol(self) -> str:
        return {
            Suit.clubs: "\u2663\ufe0f",
            Suit.diamonds: "\u2666\ufe0f",
            Suit.hearts: "\u2665\ufe0f",
            Suit.spades: "\u2660\ufe0f",
        }[self]

RANKS = ["6", "7", "8", "9", "10", "J", "Q", "K", "A"]
RANK_VALUE = {r: i for i, r in enumerate(RANKS, start=6)}
RANK_RU = {
    "6": "6",
    "7": "7",
    "8": "8",
    "9": "9",
    "10": "10",
    "J": "Валет",
    "Q": "Дама",
    "K": "Король",
    "A": "Туз",
}
SUIT_RU = {
    Suit.clubs: "трефы",
    Suit.diamonds: "бубны",
    Suit.hearts: "червы",
    Suit.spades: "пики",
}

@dataclass(frozen=True)
class Card:
    rank: str
    suit: Suit
    
    @property
    def rank_value(self) -> int:
        return RANK_VALUE[self.rank]
    
    @property
    def label_ru(self) -> str:
        return f"{RANK_RU[self.rank]} {self.suit.symbol}"
    
    @property
    def label_ru_long(self) -> str:
        return f"{RANK_RU[self.rank]} {self.suit.symbol} ({SUIT_RU[self.suit]})"
    
    @property
    def compact(self) -> str:
        r = RANK_RU[self.rank]
        if r.isdigit():
            return f"{r}{self.suit.symbol}"
        return f"{r} {self.suit.symbol}"
    
    def to_code(self) -> str:
        return f"{self.rank}|{self.suit.value}"
    
    @staticmethod
    def from_code(code: str) -> "Card":
        rank, suit_s = code.split("|", 1)
        return Card(rank=rank, suit=Suit(suit_s))
    
    def svg_path(self) -> Path:
        base = Path(".") / self.suit.value
        base.mkdir(exist_ok=True)
        rank_tokens = []
        if self.rank in ("A", "K", "Q", "J"):
            rank_tokens = [self.rank, self.rank.lower()]
        else:
            rank_tokens = [self.rank]
        suit_tokens = [
            self.suit.value,
            self.suit.value.lower(),
            self.suit.name,
            self.suit.name.lower(),
            self.suit.symbol.replace("\ufe0f", ""),
        ]
        candidates: List[Path] = []
        for rt in rank_tokens:
            for st in suit_tokens:
                candidates += [
                    base / f"{rt}_of_{st}.svg",
                    base / f"{rt}_of_{self.suit.value}.svg",
                    base / f"{rt}_of_{self.suit.name}.svg",
                    base / f"{rt}{st}.svg",
                    base / f"{rt}_{st}.svg",
                    base / f"{rt}-{st}.svg",
                    base / f"{rt}.svg",
                ]
        verbose_rank = {
            "A": ["ace", "tuz", "туз"],
            "K": ["king", "korol", "король"],
            "Q": ["queen", "dama", "дама"],
            "J": ["jack", "valet", "валет"],
        }.get(self.rank, [])
        for vr in verbose_rank:
            for st in suit_tokens:
                candidates += [
                    base / f"{vr}_of_{st}.svg",
                    base / f"{vr}_of_{self.suit.value}.svg",
                    base / f"{vr}_of_{self.suit.name}.svg",
                    base / f"{vr}{st}.svg",
                    base / f"{vr}_{st}.svg",
                    base / f"{vr}-{st}.svg",
                ]
        for p in candidates:
            if p.exists():
                return p
        if base.exists():
            rank_needles = [t.lower() for t in (rank_tokens + verbose_rank)]
            suit_needles = [self.suit.value.lower(), self.suit.name.lower(), self.suit.symbol.replace("\ufe0f", "")]
            for p in base.glob("*.svg"):
                name = p.stem.lower()
                if any(rn in name for rn in rank_needles) and any(sn in name for sn in suit_needles):
                    return p
        return candidates[0]

@dataclass
class Player:
    user_id: int
    name: str
    color: Optional[str] = None
    hand: List[Card] = field(default_factory=list)
    seat: int = 0
    ui_chat_id: Optional[int] = None
    ui_message_id: Optional[int] = None
    is_ai: bool = False
    has_paid: bool = False

    def sort_hand(self, trump: Suit) -> None:
        suit_order = {
            Suit.clubs: 0,
            Suit.diamonds: 1,
            Suit.hearts: 2,
            Suit.spades: 3,
        }
        def key(c: Card):
            return (c.suit == trump, suit_order.get(c.suit, 99), c.rank_value)
        self.hand.sort(key=key)
    
    def remove_card(self, card: Card) -> None:
        for i, c in enumerate(self.hand):
            if c == card:
                self.hand.pop(i)
                return
        raise ValueError("Card not in hand")

class LobbyMode(str, Enum):
    open = "open"
    closed = "closed"
    ai = "ai"
    betting = "betting"

class LobbyStatus(str, Enum):
    waiting = "waiting"
    playing = "playing"
    finished = "finished"

class AIDifficulty(str, Enum):
    easy = "easy"
    normal = "normal"
    hard = "hard"

@dataclass
class Lobby:
    lobby_id: str
    mode: LobbyMode
    code: Optional[str]
    owner_id: int
    display_id: int = 0
    status: LobbyStatus = LobbyStatus.waiting
    players: List[Player] = field(default_factory=list)
    created_at: float = field(default_factory=now_ts)
    last_activity_ts: float = field(default_factory=now_ts)
    is_listed: bool = True
    ai_difficulty: Optional[AIDifficulty] = None
    ai_model: Optional[str] = None
    stake_amount: Optional[float] = None
    match_id: Optional[str] = None
    max_players: int = 4
    is_locked: bool = False

    def get_player(self, user_id: int) -> Optional[Player]:
        return next((p for p in self.players if p.user_id == user_id), None)

class TurnPhase(str, Enum):
    attack_select = "attack_select"
    defend = "defend"
    throwin_select = "throwin_select"
    finished = "finished"

@dataclass
class TablePair:
    attack: Card
    defense: Optional[Card] = None
    
    def is_covered(self) -> bool:
        return self.defense is not None

@dataclass
class GameState:
    lobby_id: str
    deck: List[Card]
    trump: Suit
    trump_card: Card
    discard: List[Card] = field(default_factory=list)
    attacker_seat: int = 0
    defender_seat: int = 1
    phase: TurnPhase = TurnPhase.attack_select
    table: List[TablePair] = field(default_factory=list)
    took: bool = False
    last_action_ts: float = field(default_factory=now_ts)
    pending_attack: Dict[int, List[Card]] = field(default_factory=dict)
    pending_throwin: Dict[int, List[Card]] = field(default_factory=dict)
    table_photo_message_ids: Dict[int, List[int]] = field(default_factory=dict)
    ai_lock: bool = False
    winners_user_ids: List[int] = field(default_factory=list)
    loser_user_id: Optional[int] = None
    end_reason: Optional[str] = None
    last_play_ts: float = field(default_factory=now_ts)
    afk_prompt_active: bool = False
    afk_prompt_started: float = 0.0
    afk_prompt_count: int = 0
    afk_prompt_responses: Set[int] = field(default_factory=set)
    afk_last_prompt_ts: float = 0.0
    
    def all_table_ranks(self) -> Set[str]:
        ranks = set()
        for p in self.table:
            ranks.add(p.attack.rank)
            if p.defense:
                ranks.add(p.defense.rank)
        return ranks
    
    def max_attack_cards(self, defender_hand_size: int) -> int:
        return min(4, defender_hand_size)
    
    def is_all_covered(self) -> bool:
        return len(self.table) > 0 and all(p.is_covered() for p in self.table)

# =============================================================================
# СЕРВИСЫ: КАРТЫ
# =============================================================================
class CardsService:
    @staticmethod
    def new_deck36() -> List[Card]:
        deck = []
        for suit in [Suit.clubs, Suit.diamonds, Suit.hearts, Suit.spades]:
            for rank in RANKS:
                deck.append(Card(rank=rank, suit=suit))
        random.shuffle(deck)
        return deck
    
    @staticmethod
    def lowest_trump_attacker(players: List[Player], trump: Suit) -> int:
        lowest: Optional[Tuple[int, int]] = None
        for p in players:
            trumps = [c for c in p.hand if c.suit == trump]
            if not trumps:
                continue
            rv = min(c.rank_value for c in trumps)
            if lowest is None or rv < lowest[1]:
                lowest = (p.seat, rv)
        return lowest[0] if lowest else 0
    
    @staticmethod
    def beats(defense: Card, attack: Card, trump: Suit) -> bool:
        if defense.suit == attack.suit and defense.rank_value > attack.rank_value:
            return True
        if defense.suit == trump and attack.suit != trump:
            return True
        return False
    
    @staticmethod
    def deal_in_order(players_in_order: List[Player], deck: List[Card], trump: Suit):
        for p in players_in_order:
            while len(p.hand) < 6 and deck:
                p.hand.append(deck.pop(0))
            p.sort_hand(trump)

# =============================================================================
# МЕНЕДЖЕР ЛОББИ
# =============================================================================
class LobbyManager:
    def __init__(self):
        self.lobbies: Dict[str, Lobby] = {}
        self.player_to_lobby: Dict[int, str] = {}
        self.open_queue: List[str] = []
        self.display_id_counter: int = 1

    def _next_display_id(self) -> int:
        did = self.display_id_counter
        self.display_id_counter += 1
        return did

    def _touch(self, lobby: Lobby) -> None:
        lobby.last_activity_ts = now_ts()

    def _is_joinable(self, lobby: Lobby) -> bool:
        if lobby.status != LobbyStatus.waiting:
            return False
        if lobby.is_locked:
            return False
        if len(lobby.players) >= lobby.max_players:
            return False
        return True

    def _refresh_open_queue(self, lobby: Lobby) -> None:
        if lobby.mode != LobbyMode.open:
            return
        joinable = self._is_joinable(lobby)
        if joinable and lobby.lobby_id not in self.open_queue:
            self.open_queue.append(lobby.lobby_id)
        if not joinable and lobby.lobby_id in self.open_queue:
            self.open_queue.remove(lobby.lobby_id)

    def get_lobby_by_player(self, user_id: int) -> Optional[Lobby]:
        lid = self.player_to_lobby.get(user_id)
        if not lid:
            return None
        return self.lobbies.get(lid)

    def get_lobby_by_display_id(self, display_id: int) -> Optional[Lobby]:
        for lobby in self.lobbies.values():
            if lobby.display_id == display_id:
                return lobby
        return None
    
    def create_lobby(self, owner: Player, mode: LobbyMode, max_players: Optional[int] = None) -> Lobby:
        lobby_id = gen_code(10)
        code = gen_code(6) if mode == LobbyMode.closed else None
        if max_players is None:
            if mode == LobbyMode.betting:
                max_players = 2
            elif mode == LobbyMode.ai:
                max_players = 2
            else:
                max_players = 4
        lobby = Lobby(
            lobby_id=lobby_id,
            mode=mode,
            code=code,
            owner_id=owner.user_id,
            display_id=self._next_display_id(),
            max_players=max_players,
        )
        owner.seat = 0
        lobby.players.append(owner)
        self.lobbies[lobby_id] = lobby
        self.player_to_lobby[owner.user_id] = lobby_id
        self._touch(lobby)
        if mode == LobbyMode.open:
            self._refresh_open_queue(lobby)
        return lobby

    def _try_join(self, lobby: Lobby, player: Player) -> bool:
        if not self._is_joinable(lobby):
            return False
        player.seat = len(lobby.players)
        lobby.players.append(player)
        self.player_to_lobby[player.user_id] = lobby.lobby_id
        self._touch(lobby)
        self._refresh_open_queue(lobby)
        return True
    
    def join_open(self, player: Player) -> Lobby:
        for lid in list(self.open_queue):
            lobby = self.lobbies.get(lid)
            if not lobby or lobby.mode != LobbyMode.open:
                if lid in self.open_queue:
                    self.open_queue.remove(lid)
                continue
            if now_ts() - lobby.last_activity_ts > LOBBY_IDLE_TIMEOUT:
                self._remove_lobby(lobby)
                continue
            if not self._is_joinable(lobby):
                if lid in self.open_queue:
                    self.open_queue.remove(lid)
                continue
            if self._try_join(lobby, player):
                return lobby
        return self.create_lobby(player, LobbyMode.open, max_players=4)
    
    def join_closed(self, player: Player, code: str) -> Optional[Lobby]:
        for lobby in self.lobbies.values():
            if lobby.mode in (LobbyMode.closed, LobbyMode.betting) and lobby.status == LobbyStatus.waiting and lobby.code == code:
                if lobby.is_locked:
                    return None
                if lobby.mode == LobbyMode.betting and len(lobby.players) >= 2:
                    return None
                if len(lobby.players) >= lobby.max_players:
                    return None
                if self._try_join(lobby, player):
                    return lobby
                return None
        return None
    
    def leave(self, user_id: int) -> Optional[Lobby]:
        lobby = self.get_lobby_by_player(user_id)
        if not lobby:
            return None
        lobby.players = [p for p in lobby.players if p.user_id != user_id]
        self.player_to_lobby.pop(user_id, None)
        for i, p in enumerate(lobby.players):
            p.seat = i
        if not lobby.players:
            self.lobbies.pop(lobby.lobby_id, None)
            if lobby.lobby_id in self.open_queue:
                self.open_queue.remove(lobby.lobby_id)
            return lobby
        if lobby.owner_id == user_id:
            lobby.owner_id = lobby.players[0].user_id
        self._touch(lobby)
        self._refresh_open_queue(lobby)
        return lobby

    def _remove_lobby(self, lobby: Lobby) -> None:
        for p in lobby.players:
            self.player_to_lobby.pop(p.user_id, None)
        self.lobbies.pop(lobby.lobby_id, None)
        if lobby.lobby_id in self.open_queue:
            self.open_queue.remove(lobby.lobby_id)

    async def cleanup_stale(self, bot: Bot) -> None:
        now = now_ts()
        for lobby in list(self.lobbies.values()):
            if lobby.status != LobbyStatus.waiting:
                continue
            if now - lobby.last_activity_ts < LOBBY_IDLE_TIMEOUT:
                continue
            # Возврат ставок при простое
            if lobby.mode == LobbyMode.betting and lobby.match_id:
                for p in lobby.players:
                    if not p.is_ai and p.has_paid:
                        try:
                            db.refund_payment(lobby.match_id, p.user_id)
                        except Exception:
                            pass
            for p in lobby.players:
                if p.is_ai:
                    continue
                try:
                    await bot.send_message(p.user_id, "Лобби закрыто из‑за неактивности (5 минут).")
                except Exception:
                    pass
            self._remove_lobby(lobby)

lobbies = LobbyManager()

# =============================================================================
# ИГРОВОЙ ДВИЖОК
# =============================================================================
class GameEngine:
    def __init__(self):
        self.games: Dict[str, GameState] = {}
    
    def get_game(self, lobby_id: str) -> Optional[GameState]:
        return self.games.get(lobby_id)
    
    def start_game(self, lobby: Lobby) -> GameState:
        deck = CardsService.new_deck36()
        trump_card = deck[-1]
        trump = trump_card.suit
        for p in lobby.players:
            p.hand = []
        for _ in range(6):
            for p in lobby.players:
                p.hand.append(deck.pop(0))
        for p in lobby.players:
            p.sort_hand(trump)
        attacker_seat = CardsService.lowest_trump_attacker(lobby.players, trump)
        defender_seat = (attacker_seat + 1) % len(lobby.players)
        gs = GameState(
            lobby_id=lobby.lobby_id,
            deck=deck,
            trump=trump,
            trump_card=trump_card,
            attacker_seat=attacker_seat,
            defender_seat=defender_seat,
            phase=TurnPhase.attack_select,
            table=[],
        )
        self.games[lobby.lobby_id] = gs
        return gs

    @staticmethod
    def _mark_play(gs: GameState) -> None:
        now = now_ts()
        gs.last_play_ts = now
        gs.afk_prompt_active = False
        gs.afk_prompt_responses = set()
        gs.afk_prompt_started = 0.0
        gs.afk_last_prompt_ts = now
    
    def seat_player(self, lobby: Lobby, seat: int) -> Optional[Player]:
        for p in lobby.players:
            if p.seat == seat:
                return p
        return None
    
    def normalize_turn_seats_after_leave(self, lobby: Lobby, gs: GameState):
        n = len(lobby.players)
        if n == 0:
            return
        if gs.attacker_seat >= n:
            gs.attacker_seat = 0
        if gs.defender_seat >= n:
            gs.defender_seat = 0
        if gs.defender_seat == gs.attacker_seat:
            gs.defender_seat = (gs.attacker_seat + 1) % n if n > 1 else gs.attacker_seat
        if n < 2:
            lobby.status = LobbyStatus.finished
            gs.phase = TurnPhase.finished
            gs.end_reason = "Игра прервана: не хватает игроков."
    
    def is_player_out(self, player: Player, gs: GameState) -> bool:
        return len(player.hand) == 0 and len(gs.deck) == 0
    
    def _deal_order_from(self, lobby: Lobby, start_seat: int) -> List[Player]:
        order = []
        for i in range(len(lobby.players)):
            seat = (start_seat + i) % len(lobby.players)
            p = self.seat_player(lobby, seat)
            if p:
                order.append(p)
        return order
    
    def _check_endgame(self, lobby: Lobby):
        gs = self.get_game(lobby.lobby_id)
        if not gs or lobby.status == LobbyStatus.finished:
            return
        out = [p for p in lobby.players if self.is_player_out(p, gs)]
        remaining = [p for p in lobby.players if not self.is_player_out(p, gs)]
        if len(out) >= len(lobby.players) - 1:
            lobby.status = LobbyStatus.finished
            gs.phase = TurnPhase.finished
            gs.winners_user_ids = [p.user_id for p in out]
            if len(remaining) == 1:
                gs.loser_user_id = remaining[0].user_id
                gs.end_reason = "Остался последний с картами — дурак."
            else:
                gs.loser_user_id = None
                gs.end_reason = "Все вышли одновременно."

    def _ranks_allowed_for_throw(self, gs: GameState) -> Set[str]:
        return gs.all_table_ranks() if gs.table else set()

    def can_select_attack_card(self, lobby: Lobby, gs: GameState, attacker: Player, card: Card) -> Tuple[bool, str]:
        if lobby.status != LobbyStatus.playing:
            return False, "Игра не идёт."
        if attacker.seat != gs.attacker_seat:
            return False, "Сейчас не твоя очередь атаковать."
        if gs.phase != TurnPhase.attack_select:
            return False, "Сейчас нельзя выбирать карты для атаки."
        defender = self.seat_player(lobby, gs.defender_seat)
        if not defender:
            return False, "Соперник не найден."
        pending = gs.pending_attack.get(attacker.seat, [])
        if card not in attacker.hand:
            return False, "Этой карты нет у тебя в руке."
        if not gs.table and not pending:
            return True, "ok"
        ranks = gs.all_table_ranks() | {c.rank for c in pending}
        if card.rank not in ranks:
            return False, "Подкидывать можно только по номиналам, которые уже есть на столе."
        if len(gs.table) + len(pending) >= gs.max_attack_cards(len(defender.hand)):
            return False, "Больше подкинуть нельзя (лимит 4 карты на столе и по руке защитника)."
        return True, "ok"

    def toggle_attack_select(self, lobby: Lobby, attacker: Player, card: Card) -> Tuple[bool, str]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена."
        pending = gs.pending_attack.get(attacker.seat, [])
        if card in pending:
            pending.remove(card)
            gs.pending_attack[attacker.seat] = pending
            return True, "ok"
        ok, err = self.can_select_attack_card(lobby, gs, attacker, card)
        if not ok:
            return False, err
        pending.append(card)
        gs.pending_attack[attacker.seat] = pending
        gs.last_action_ts = now_ts()
        return True, "ok"

    def commit_attack(self, lobby: Lobby, attacker: Player) -> Tuple[bool, str, List[Card]]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена.", []
        if attacker.seat != gs.attacker_seat:
            return False, "Сейчас не твоя очередь атаковать.", []
        if gs.phase != TurnPhase.attack_select:
            return False, "Сейчас нельзя ходить.", []
        pending = gs.pending_attack.get(attacker.seat, [])
        if not pending:
            return False, "Выбери хотя бы одну карту.", []
        defender = self.seat_player(lobby, gs.defender_seat)
        if not defender:
            return False, "Соперник не найден.", []
        temp_table = list(gs.table)
        temp_pending: List[Card] = []
        for c in pending:
            ranks = {x.attack.rank for x in temp_table} | {x.defense.rank for x in temp_table if x.defense} | {
                x.rank for x in temp_pending
            }
            if temp_table or temp_pending:
                if c.rank not in ranks:
                    return False, "Среди выбранных есть карта, которую нельзя подкинуть по номиналу.", []
            if len(temp_table) + len(temp_pending) >= gs.max_attack_cards(len(defender.hand)):
                return False, "Слишком много карт (лимит 4 и по руке защитника).", []
            temp_pending.append(c)
        applied = list(pending)
        for c in applied:
            attacker.remove_card(c)
            gs.table.append(TablePair(attack=c, defense=None))
        gs.pending_attack[attacker.seat] = []
        gs.phase = TurnPhase.defend
        gs.last_action_ts = now_ts()
        self._mark_play(gs)
        return True, "ok", applied

    def can_defend(self, lobby: Lobby, gs: GameState, defender: Player, pair_index: int, card: Card) -> Tuple[bool, str]:
        if lobby.status != LobbyStatus.playing:
            return False, "Игра не идёт."
        if defender.seat != gs.defender_seat:
            return False, "Сейчас не твоя очередь защищаться."
        if gs.phase != TurnPhase.defend:
            return False, "Сейчас нельзя отбиваться."
        if gs.took:
            return False, "Ты уже нажал(а) «Взять»."
        if not gs.table:
            return False, "На столе нет карт."
        if pair_index < 0 or pair_index >= len(gs.table):
            return False, "Некорректная цель."
        pair = gs.table[pair_index]
        if pair.defense is not None:
            return False, "Эта карта уже побита."
        if card not in defender.hand:
            return False, "Этой карты нет у тебя в руке."
        if not CardsService.beats(card, pair.attack, gs.trump):
            return False, "Этой картой нельзя побить."
        return True, "ok"

    def defend(self, lobby: Lobby, defender: Player, pair_index: int, card: Card) -> Tuple[bool, str]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена."
        ok, err = self.can_defend(lobby, gs, defender, pair_index, card)
        if not ok:
            return False, err
        defender.remove_card(card)
        gs.table[pair_index].defense = card
        gs.last_action_ts = now_ts()
        self._mark_play(gs)
        if gs.is_all_covered():
            gs.phase = TurnPhase.attack_select
        else:
            gs.phase = TurnPhase.defend
        return True, "ok"

    def defender_take(self, lobby: Lobby, defender: Player) -> Tuple[bool, str]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена."
        if defender.seat != gs.defender_seat:
            return False, "Сейчас не твоя очередь защищаться."
        if not gs.table:
            return False, "На столе нет карт."
        if gs.took:
            return False, "Уже выбрано «Взять»."
        if gs.phase not in (TurnPhase.defend, TurnPhase.attack_select):
            return False, "Сейчас нельзя взять."
        gs.took = True
        gs.phase = TurnPhase.throwin_select
        gs.pending_throwin = {}
        gs.last_action_ts = now_ts()
        self._mark_play(gs)
        return True, "ok"

    def can_select_throwin(self, lobby: Lobby, gs: GameState, player: Player, card: Card) -> Tuple[bool, str]:
        if gs.phase != TurnPhase.throwin_select:
            return False, "Сейчас нельзя подкидывать."
        if player.seat == gs.defender_seat:
            return False, "Защитник не подкидывает."
        if card not in player.hand:
            return False, "Этой карты нет у тебя в руке."
        allowed = self._ranks_allowed_for_throw(gs)
        if not allowed:
            return False, "На столе нет карт."
        if card.rank not in allowed:
            return False, "Подкидывать можно только по номиналам на столе."
        defender = self.seat_player(lobby, gs.defender_seat)
        if not defender:
            return False, "Соперник не найден."
        total_pending = sum(len(v) for v in gs.pending_throwin.values())
        if len(gs.table) + total_pending >= gs.max_attack_cards(len(defender.hand)):
            return False, "Больше подкинуть нельзя (лимит 4 и по руке защитника)."
        return True, "ok"

    def toggle_throwin_select(self, lobby: Lobby, player: Player, card: Card) -> Tuple[bool, str]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена."
        pending = gs.pending_throwin.get(player.seat, [])
        if card in pending:
            pending.remove(card)
            gs.pending_throwin[player.seat] = pending
            return True, "ok"
        ok, err = self.can_select_throwin(lobby, gs, player, card)
        if not ok:
            return False, err
        pending.append(card)
        gs.pending_throwin[player.seat] = pending
        gs.last_action_ts = now_ts()
        return True, "ok"

    def commit_throwin_done(self, lobby: Lobby) -> Tuple[bool, str, List[Card]]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена.", []
        if gs.phase != TurnPhase.throwin_select:
            return False, "Сейчас нельзя.", []
        all_cards: List[Tuple[int, Card]] = []
        for seat, cards in gs.pending_throwin.items():
            for c in cards:
                all_cards.append((seat, c))
        if not all_cards:
            self._resolve_take(lobby)
            self._mark_play(gs)
            return True, "ok", []
        all_cards.sort(key=lambda x: x[0])
        applied: List[Card] = []
        for seat, c in all_cards:
            pl = self.seat_player(lobby, seat)
            if not pl:
                continue
            ok, err = self.can_select_throwin(lobby, gs, pl, c)
            if not ok:
                return False, err, []
            pl.remove_card(c)
            gs.table.append(TablePair(attack=c, defense=None))
            applied.append(c)
        gs.pending_throwin = {}
        self._resolve_take(lobby)
        self._mark_play(gs)
        return True, "ok", applied

    def attacker_bito(self, lobby: Lobby, attacker: Player) -> Tuple[bool, str]:
        gs = self.get_game(lobby.lobby_id)
        if not gs:
            return False, "Игра не найдена."
        if lobby.status != LobbyStatus.playing:
            return False, "Игра не идёт."
        if attacker.seat != gs.attacker_seat:
            return False, "Сейчас не твоя очередь завершать ход."
        if not gs.is_all_covered():
            return False, "Нельзя: не все карты побиты."
        self._resolve_bito(lobby)
        self._mark_play(gs)
        return True, "ok"

    def _resolve_bito(self, lobby: Lobby):
        gs = self.get_game(lobby.lobby_id)
        assert gs
        for p in gs.table:
            gs.discard.append(p.attack)
            if p.defense:
                gs.discard.append(p.defense)
        gs.table = []
        gs.took = False
        gs.pending_attack = {}
        gs.pending_throwin = {}
        gs.attacker_seat = gs.defender_seat
        gs.defender_seat = (gs.attacker_seat + 1) % len(lobby.players)
        order = self._deal_order_from(lobby, gs.attacker_seat)
        CardsService.deal_in_order(order, gs.deck, gs.trump)
        gs.phase = TurnPhase.attack_select
        gs.last_action_ts = now_ts()
        self._check_endgame(lobby)

    def _resolve_take(self, lobby: Lobby):
        gs = self.get_game(lobby.lobby_id)
        assert gs
        defender = self.seat_player(lobby, gs.defender_seat)
        if defender:
            for p in gs.table:
                defender.hand.append(p.attack)
                if p.defense:
                    defender.hand.append(p.defense)
            defender.sort_hand(gs.trump)
        gs.table = []
        gs.attacker_seat = gs.attacker_seat
        gs.defender_seat = (gs.defender_seat + 1) % len(lobby.players)
        order = self._deal_order_from(lobby, gs.attacker_seat)
        CardsService.deal_in_order(order, gs.deck, gs.trump)
        gs.took = False
        gs.pending_attack = {}
        gs.pending_throwin = {}
        gs.phase = TurnPhase.attack_select
        gs.last_action_ts = now_ts()
        self._check_endgame(lobby)

engine = GameEngine()

class GroqDurakAI:
    def __init__(self, api_key: str):
        self.api_key = api_key.strip()
        self.enabled = Groq is not None and bool(self.api_key)
    def _client(self):
        return Groq(api_key=self.api_key)
    @staticmethod
    def _cards_list_str(cards: List[Card]) -> str:
        return ", ".join([c.label_ru for c in cards]) if cards else "—"
    @staticmethod
    def _table_str(gs: GameState) -> str:
        if not gs.table:
            return "Стол пуст."
        lines = []
        for i, pair in enumerate(gs.table, start=1):
            if pair.defense:
                lines.append(f"{i}) {pair.attack.label_ru} побито {pair.defense.label_ru}")
            else:
                lines.append(f"{i}) {pair.attack.label_ru} не побито")
        return "\n".join(lines)
    @staticmethod
    def _all_cards() -> List[Card]:
        cards: List[Card] = []
        for suit in [Suit.clubs, Suit.diamonds, Suit.hearts, Suit.spades]:
            for rank in RANKS:
                cards.append(Card(rank=rank, suit=suit))
        return cards
    def _ai_state_path(self, lobby_id: str) -> Path:
        return Path(tempfile.gettempdir()) / f"durak_ai_{lobby_id}.json"
    def _build_counting_snapshot(self, lobby: Lobby, gs: GameState, ai_player: Player) -> Dict:
        known_codes: Set[str] = set()
        known_codes.update(c.to_code() for c in ai_player.hand)
        known_codes.update(c.to_code() for c in gs.discard)
        known_codes.add(gs.trump_card.to_code())
        for pair in gs.table:
            known_codes.add(pair.attack.to_code())
            if pair.defense:
                known_codes.add(pair.defense.to_code())
        all_codes = [c.to_code() for c in self._all_cards()]
        unseen_codes = [c for c in all_codes if c not in known_codes]
        unseen_by_suit = {s.value: 0 for s in Suit}
        unseen_by_rank = {r: 0 for r in RANKS}
        for code in unseen_codes:
            rank, suit = code.split("|", 1)
            unseen_by_rank[rank] += 1
            unseen_by_suit[suit] += 1
        return {
            "lobby_id": lobby.lobby_id,
            "updated_at_ts": now_ts(),
            "phase": gs.phase.value,
            "attacker_seat": gs.attacker_seat,
            "defender_seat": gs.defender_seat,
            "trump": gs.trump.value,
            "trump_card": gs.trump_card.to_code(),
            "deck_left": len(gs.deck),
            "ai_hand": [c.to_code() for c in ai_player.hand],
            "table": [
                {"attack": pair.attack.to_code(), "defense": pair.defense.to_code() if pair.defense else None}
                for pair in gs.table
            ],
            "discard": [c.to_code() for c in gs.discard],
            "known_cards": sorted(known_codes),
            "unseen_cards": unseen_codes,
            "unseen_by_suit": unseen_by_suit,
            "unseen_by_rank": unseen_by_rank,
            "opponent_hand_sizes": {str(p.seat): len(p.hand) for p in lobby.players if not p.is_ai},
        }
    def _write_ai_state_file(self, lobby: Lobby, gs: GameState, ai_player: Player) -> Dict:
        snapshot = self._build_counting_snapshot(lobby, gs, ai_player)
        path = self._ai_state_path(lobby.lobby_id)
        try:
            path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return snapshot
    async def choose_action(self, lobby: Lobby, gs: GameState, ai_player: Player) -> Dict:
        diff = lobby.ai_difficulty or AIDifficulty.hard
        prob_use_model = {
            AIDifficulty.easy: 0.25,
            AIDifficulty.normal: 0.70,
            AIDifficulty.hard: 0.95,
        }[diff]
        counting_snapshot: Optional[Dict] = None
        if diff == AIDifficulty.hard:
            counting_snapshot = self._write_ai_state_file(lobby, gs, ai_player)
        if random.random() > prob_use_model or not self.enabled:
            return {"type": "heuristic"}
        allowed = self._enumerate_allowed_moves(lobby, gs, ai_player)
        if len(allowed) == 1:
            return allowed[0]
        model = lobby.ai_model or AI_MODEL_HARD
        system = (
            "Ты играешь в русского «Дурака» (подкидной). Верни ТОЛЬКО JSON без текста. "
            "Выбирай действие строго из ALLOWED_MOVES_JSON.\n"
            "Форматы:\n"
            '{"type":"attack","cards":["rank|suit", ...]}\n'
            '{"type":"defend","pair_index":0,"card":"rank|suit"}\n'
            '{"type":"take"}\n'
            '{"type":"throwin_done","cards":["rank|suit", ...]}\n'
            '{"type":"bito"}\n'
            '{"type":"wait"}\n'
        )
        if diff == AIDifficulty.hard:
            system += "Используй CARD_COUNTING_SNAPSHOT_JSON для сильной игры.\n"
        user_lines = [
            f"TRUMP: {gs.trump.value}",
            f"YOUR_HAND: {self._cards_list_str(ai_player.hand)}",
            f"TABLE:\n{self._table_str(gs)}",
            f"DECK_LEFT: {len(gs.deck)}",
            f"PHASE: {gs.phase.value}",
            f"YOU_ARE_ATTACKER: {ai_player.seat == gs.attacker_seat}",
            f"YOU_ARE_DEFENDER: {ai_player.seat == gs.defender_seat}",
            "",
        ]
        if counting_snapshot:
            user_lines.append("CARD_COUNTING_SNAPSHOT_JSON:")
            user_lines.append(json.dumps(counting_snapshot, ensure_ascii=False))
            user_lines.append("")
        user_lines.append(f"ALLOWED_MOVES_JSON:\n{json.dumps(allowed, ensure_ascii=False)}\n")
        user_lines.append("Выбери лучший ход и верни один JSON.")
        user = {"role": "user", "content": "\n".join(user_lines)}
        def call_sync() -> str:
            client = self._client()
            completion = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": system}, user],
                temperature=0.7 if diff != AIDifficulty.easy else 1.0,
                top_p=1,
                max_completion_tokens=500 if diff == AIDifficulty.easy else 700,
                reasoning_effort="low"
                if diff == AIDifficulty.easy
                else ("medium" if diff == AIDifficulty.normal else "high"),
                stream=False,
                stop=None,
            )
            return completion.choices[0].message.content or ""
        try:
            raw = await asyncio.to_thread(call_sync)
        except Exception:
            return {"type": "heuristic"}
        action = self._parse_json_action(raw)
        if not action:
            return {"type": "heuristic"}
        if not self._action_is_allowed(action, allowed):
            return {"type": "heuristic"}
        return action
    @staticmethod
    def _parse_json_action(raw: str) -> Optional[Dict]:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`").replace("json", "", 1).strip()
        try:
            return json.loads(raw)
        except Exception:
            start = raw.find("{")
            end = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(raw[start : end + 1])
                except Exception:
                    return None
        return None
    def _enumerate_allowed_moves(self, lobby: Lobby, gs: GameState, ai: Player) -> List[Dict]:
        allowed: List[Dict] = []
        trump = gs.trump
        if ai.seat == gs.attacker_seat:
            if gs.is_all_covered() and gs.table:
                allowed.append({"type": "bito"})
            if gs.phase == TurnPhase.attack_select:
                defender = next((p for p in lobby.players if p.seat == gs.defender_seat), None)
                if not defender:
                    return [{"type": "wait"}]
                max_cards = gs.max_attack_cards(len(defender.hand))
                ranks_on_table = gs.all_table_ranks()
                def can_add(card: Card, current: List[Card]) -> bool:
                    if len(gs.table) + len(current) >= max_cards:
                        return False
                    if not gs.table and not current:
                        return True
                    ranks = ranks_on_table | {c.rank for c in current}
                    return card.rank in ranks
                hand_sorted = sorted(ai.hand, key=lambda c: (c.suit == trump, c.rank_value))
                for c in hand_sorted[:12]:
                    if can_add(c, []):
                        allowed.append({"type": "attack", "cards": [c.to_code()]})
                by_rank: Dict[str, List[Card]] = {}
                for c in hand_sorted:
                    by_rank.setdefault(c.rank, []).append(c)
                for _, cards in by_rank.items():
                    group = []
                    for c in cards:
                        if can_add(c, group):
                            group.append(c)
                    if len(group) >= 2:
                        allowed.append({"type": "attack", "cards": [x.to_code() for x in group[:max_cards]]})
                if not allowed:
                    allowed.append({"type": "wait"})
                return allowed[:50]
            return allowed or [{"type": "wait"}]
        if ai.seat == gs.defender_seat:
            if gs.phase == TurnPhase.defend:
                uncovered = [(i, p.attack) for i, p in enumerate(gs.table) if p.defense is None]
                for idx, atk in uncovered:
                    for c in ai.hand:
                        if CardsService.beats(c, atk, trump):
                            allowed.append({"type": "defend", "pair_index": idx, "card": c.to_code()})
                if gs.table:
                    allowed.append({"type": "take"})
                if not allowed:
                    allowed.append({"type": "take"})
                return allowed[:80]
            return [{"type": "wait"}]
        if gs.phase == TurnPhase.throwin_select and ai.seat != gs.defender_seat:
            allowed_ranks = gs.all_table_ranks()
            allowed.append({"type": "throwin_done", "cards": []})
            if not allowed_ranks:
                return allowed
            defender = next((p for p in lobby.players if p.seat == gs.defender_seat), None)
            if not defender:
                return allowed
            max_cards = gs.max_attack_cards(len(defender.hand))
            cap = max(0, max_cards - len(gs.table))
            candidates = [c for c in ai.hand if c.rank in allowed_ranks]
            for c in candidates[:10]:
                allowed.append({"type": "throwin_done", "cards": [c.to_code()]})
            if cap >= 2:
                by_rank: Dict[str, List[Card]] = {}
                for c in candidates:
                    by_rank.setdefault(c.rank, []).append(c)
                for _, cards in by_rank.items():
                    if len(cards) >= 2:
                        allowed.append({"type": "throwin_done", "cards": [x.to_code() for x in cards[:cap]]})
            return allowed[:30]
        return [{"type": "wait"}]
    @staticmethod
    def _action_is_allowed(action: Dict, allowed: List[Dict]) -> bool:
        def canon(x: Dict) -> str:
            return json.dumps(x, sort_keys=True)
        aset = {canon(a) for a in allowed}
        return canon(action) in aset

# =============================================================================
# СОСТОЯНИЕ
# =============================================================================
awaiting_code: Set[int] = set()
awaiting_admin_message: Set[int] = set()
awaiting_support_message: Set[int] = set()
awaiting_support_category: Dict[int, str] = {}
awaiting_support_reply: Dict[int, int] = {}
awaiting_admin_broadcast: Dict[int, dict] = {}
awaiting_admin_cleanup: Dict[int, dict] = {}
awaiting_payment: Dict[int, dict] = {}
router = Router()
ai_service = GroqDurakAI(api_key=GROQ_API_KEY) if Groq else None
last_say_ts: Dict[int, float] = {}
payout_locks: Dict[str, asyncio.Lock] = {}

# =============================================================================
# CALLBACKS / UI / HELPERS
# =============================================================================
class CB:
    MENU_OPEN = "m:open"
    MENU_OPEN_CREATE = "m:open:create"
    MENU_OPEN_AUTO = "m:open:auto"
    MENU_OPEN_LIST = "m:open:list"
    MENU_OPEN_FRIEND = "m:open:friend"
    MENU_CLOSED = "m:closed"
    MENU_JOIN = "m:join"
    MENU_HELP = "m:help"
    MENU_AI = "m:ai"
    MENU_BETTING = "m:betting"
    MENU_ADMIN_MSG = "m:admin_msg"
    MENU_PROFILE = "m:profile"
    AI_DIFF = "ai:diff:"
    BETTING_SELECT = "betting:select:"
    BETTING_CREATE = "betting:create:"
    BETTING_AUTO = "betting:auto:"
    BETTING_LIST = "betting:list:"
    BETTING_JOIN = "betting:join:"
    BETTING_FRIEND = "betting:friend:"
    OPEN_CREATE = "open:create:"
    OPEN_FRIEND = "open:friend:"
    OPEN_JOIN = "open:join:"
    LOBBY_REFRESH = "l:refresh"
    LOBBY_START = "l:start"
    LOBBY_LEAVE = "l:leave"
    LOBBY_LEAVE_CONFIRM = "l:leave_confirm"
    LOBBY_LEAVE_YES = "l:leave_yes"
    LOBBY_LEAVE_NO = "l:leave_no"
    LOBBY_COLOR = "l:color:"
    LOBBY_SETTINGS = "l:settings"
    LOBBY_LOCK_TOGGLE = "l:lock"
    LOBBY_SET_MAX = "l:setmax:"
    LOBBY_SETTINGS_BACK = "l:back"
    GAME_REFRESH = "g:refresh"
    GAME_LEAVE = "g:leave"
    GAME_LEAVE_CONFIRM = "g:leave_confirm"
    GAME_LEAVE_YES = "g:leave_yes"
    GAME_LEAVE_NO = "g:leave_no"
    GAME_TAKE = "g:take"
    GAME_BITO = "g:bito"
    GAME_SELECT = "g:sel:"
    GAME_DONE = "g:done"
    GAME_CLEAR = "g:clear"
    GAME_DEFEND = "g:def:"
    NOOP = "noop"
    AFK_OK = "g:afk_ok"
    ADMIN_REFRESH = "a:refresh"
    ADMIN_SETTINGS = "a:settings"
    ADMIN_SUPPORT = "a:support"
    ADMIN_SUPPORT_OPEN = "a:support:open"
    ADMIN_SUPPORT_ALL = "a:support:all"
    ADMIN_SUPPORT_CLOSED = "a:support:closed"
    ADMIN_MATCHES = "a:matches"
    ADMIN_NOTIFY = "a:notify"
    ADMIN_NOTIFY_CREATE = "a:notify:create"
    ADMIN_NOTIFY_STOP = "a:notify:stop"
    ADMIN_NOTIFY_CANCEL = "a:notify:cancel"
    ADMIN_SYSTEM = "a:system"
    ADMIN_TOGGLE_MSG = "a:toggle_msg"
    ADMIN_TOGGLE_BETTING = "a:toggle_betting"
    ADMIN_CLEANUP = "a:cleanup"
    ADMIN_CLEANUP_FULL = "a:cleanup:full"
    ADMIN_CLEANUP_FULL_CONFIRM = "a:cleanup:full:confirm"
    ADMIN_CLEANUP_TIME = "a:cleanup:time"
    ADMIN_CLEANUP_UNIT = "a:cleanup:unit:"
    ADMIN_CLEANUP_CANCEL = "a:cleanup:cancel"
    ADMIN_MSG_CANCEL = "a:cancel"
    SUPPORT_CANCEL = "s:cancel"
    SUPPORT_TYPE = "s:type:"
    SUPPORT_REPLY = "s:reply:"
    SUPPORT_CLOSE = "s:close:"
    PROFILE_TOGGLE_PHOTO = "p:photo"
    PROFILE_TOGGLE_NOTIFY = "p:notify"
    CHECK_SUB = "m:check_sub"


SUPPORT_CATEGORIES = [
    ("payout", "Не пришла выплата"),
    ("game", "Ошибка в игре"),
    ("payment", "Проблема с оплатой"),
    ("other", "Другое"),
]


def is_admin(uid: int) -> bool:
    return uid in ADMIN_USER_IDS


def is_admin_msg_enabled() -> bool:
    return db.get_bool_setting("admin_msg_enabled")


def is_betting_enabled() -> bool:
    return db.get_bool_setting("betting_enabled")


async def safe_answer(call: CallbackQuery, text: str = None, show_alert: bool = False):
    try:
        await call.answer(text or "", show_alert=show_alert)
    except TelegramBadRequest:
        pass
    except Exception:
        pass


def _channel_url() -> str:
    if REQUIRED_CHANNEL_URL:
        return REQUIRED_CHANNEL_URL
    if REQUIRED_CHANNEL:
        if REQUIRED_CHANNEL.startswith("@"):
            return f"https://t.me/{REQUIRED_CHANNEL[1:]}"
        if REQUIRED_CHANNEL.startswith("https://"):
            return REQUIRED_CHANNEL
    return ""


def _news_url() -> str:
    if NEWS_CHANNEL_URL:
        return NEWS_CHANNEL_URL
    return _channel_url()


def _extract_tg_username(url: str) -> str:
    if not url:
        return ""
    if "t.me/" not in url:
        return ""
    part = url.split("t.me/", 1)[1]
    part = part.split("?", 1)[0].split("/", 1)[0].strip()
    if not part:
        return ""
    if not part.startswith("@"):
        part = f"@{part}"
    return part


def _channel_candidates() -> List[object]:
    candidates: List[object] = []
    seen: Set[str] = set()

    def _add(item: object):
        key = str(item)
        if key in seen:
            return
        seen.add(key)
        candidates.append(item)

    rc = REQUIRED_CHANNEL.strip()
    if rc:
        if rc.startswith("https://"):
            uname = _extract_tg_username(rc)
            if uname:
                _add(uname)
        elif rc.startswith("@"):
            _add(rc)
        else:
            try:
                _add(int(rc))
            except Exception:
                _add(rc)
    url = _channel_url()
    uname = _extract_tg_username(url)
    if uname:
        _add(uname)
    return candidates


async def is_user_subscribed(bot: Bot, user_id: int) -> bool:
    if is_admin(user_id):
        return True
    if not REQUIRED_CHANNEL:
        return True
    cached_ts = SUBSCRIPTION_CACHE.get(user_id, 0.0)
    if now_ts() - cached_ts < SUBSCRIPTION_TTL:
        return True
    for chat_id in _channel_candidates():
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            status = getattr(member, "status", "")
            is_member = getattr(member, "is_member", False)
            if status in ("creator", "administrator", "member", "restricted") or is_member:
                SUBSCRIPTION_CACHE[user_id] = now_ts()
                return True
        except Exception:
            continue
    return False


def kb_subscribe() -> InlineKeyboardMarkup:
    rows = []
    url = _channel_url()
    if url:
        rows.append([InlineKeyboardButton(text="Подписаться", url=url)])
    rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data=CB.CHECK_SUB)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def ensure_subscribed(bot: Bot, chat_id: int, user) -> bool:
    if await is_user_subscribed(bot, user.id):
        return True
    kb = kb_subscribe()
    text = "Для доступа к боту подпишитесь на канал и нажмите «Я подписался»."
    await bot.send_message(chat_id, text, reply_markup=kb)
    return False


async def ensure_subscribed_for_call(call: CallbackQuery, bot: Bot) -> bool:
    if await is_user_subscribed(bot, call.from_user.id):
        return True
    await safe_answer(call)
    text = "Для доступа к боту подпишитесь на канал и нажмите «Я подписался»."
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_subscribe(),
    )
    return False


def get_cached_notice_text() -> Optional[str]:
    now = now_ts()
    if now - float(NOTICE_CACHE.get("ts") or 0) < 10:
        return NOTICE_CACHE.get("text")
    try:
        active_notice = db.get_active_broadcast(now)
        text = active_notice.get("text") if active_notice else None
    except Exception:
        text = None
    NOTICE_CACHE["ts"] = now
    NOTICE_CACHE["text"] = text
    return text


def get_cached_user_settings(user_id: int) -> dict:
    cached = USER_SETTINGS_CACHE.get(user_id)
    if cached:
        ts, data = cached
        if now_ts() - ts < USER_SETTINGS_TTL:
            return data
    data = db.get_user_settings(user_id)
    USER_SETTINGS_CACHE[user_id] = (now_ts(), data)
    return data


def update_cached_user_settings(user_id: int, data: dict) -> None:
    USER_SETTINGS_CACHE[user_id] = (now_ts(), data)


def kb_ai_difficulty() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Лёгкий", callback_data=CB.AI_DIFF + "easy")],
            [InlineKeyboardButton(text="Нормальный", callback_data=CB.AI_DIFF + "normal")],
            [InlineKeyboardButton(text="Тяжёлый", callback_data=CB.AI_DIFF + "hard")],
            [InlineKeyboardButton(text="Назад", callback_data="back:menu")],
        ]
    )


def kb_menu() -> InlineKeyboardMarkup:
    items = [
        ("\U0001f3b2 Открытая игра", CB.MENU_OPEN),
        ("\U0001f512 Закрытая игра", CB.MENU_CLOSED),
        ("\U0001f511 Войти по коду", CB.MENU_JOIN),
        ("\U0001f916 Игра против ИИ", CB.MENU_AI),
        ("\U0001f4b0 Игра на ставки", CB.MENU_BETTING),
        ("\U0001f4d6 Правила", CB.MENU_HELP),
    ]
    rows = []
    for i, (text, cb) in enumerate(items):
        style = MENU_BUTTON_STYLES[i % len(MENU_BUTTON_STYLES)]
        rows.append([InlineKeyboardButton(text=text, callback_data=cb, style=style)])
    news_url = _news_url()
    if news_url:
        style = MENU_BUTTON_STYLES[len(rows) % len(MENU_BUTTON_STYLES)]
        rows.append([InlineKeyboardButton(text="\U0001f4f0 Новости", url=news_url, style=style)])
    style = MENU_BUTTON_STYLES[len(rows) % len(MENU_BUTTON_STYLES)]
    rows.append([InlineKeyboardButton(text="\U0001f464 Профиль", callback_data=CB.MENU_PROFILE, style=style)])
    style = MENU_BUTTON_STYLES[len(rows) % len(MENU_BUTTON_STYLES)]
    rows.append([InlineKeyboardButton(text="\U0001f198 Поддержка", callback_data=CB.MENU_ADMIN_MSG, style=style)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back:menu")]]
    )


def kb_open_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать новое лобби", callback_data=CB.MENU_OPEN_CREATE)],
            [InlineKeyboardButton(text="Игра с другом", callback_data=CB.MENU_OPEN_FRIEND)],
            [InlineKeyboardButton(text="Авто подбор", callback_data=CB.MENU_OPEN_AUTO)],
            [InlineKeyboardButton(text="Выбрать лобби", callback_data=CB.MENU_OPEN_LIST)],
            [InlineKeyboardButton(text="Назад", callback_data="back:menu")],
        ]
    )


def kb_open_create_max() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="2 игрока", callback_data=f"{CB.OPEN_CREATE}2")],
            [InlineKeyboardButton(text="3 игрока", callback_data=f"{CB.OPEN_CREATE}3")],
            [InlineKeyboardButton(text="4 игрока", callback_data=f"{CB.OPEN_CREATE}4")],
            [InlineKeyboardButton(text="Назад", callback_data=CB.MENU_OPEN)],
        ]
    )


def kb_open_friend_max() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="2 игрока", callback_data=f"{CB.OPEN_FRIEND}2")],
            [InlineKeyboardButton(text="3 игрока", callback_data=f"{CB.OPEN_FRIEND}3")],
            [InlineKeyboardButton(text="4 игрока", callback_data=f"{CB.OPEN_FRIEND}4")],
            [InlineKeyboardButton(text="Назад", callback_data=CB.MENU_OPEN)],
        ]
    )


def kb_open_list(lobbies_list: List[Lobby]) -> InlineKeyboardMarkup:
    buttons = []
    for lb in lobbies_list:
        text = f"#{lb.display_id} • {len(lb.players)}/{lb.max_players}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"{CB.OPEN_JOIN}{lb.display_id}")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data=CB.MENU_OPEN)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_betting_open_menu(amount: float) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать лобби", callback_data=f"{CB.BETTING_CREATE}{amount}")],
            [InlineKeyboardButton(text="Игра с другом", callback_data=f"{CB.BETTING_FRIEND}{amount}")],
            [InlineKeyboardButton(text="Авто подбор", callback_data=f"{CB.BETTING_AUTO}{amount}")],
            [InlineKeyboardButton(text="Выбрать лобби", callback_data=f"{CB.BETTING_LIST}{amount}")],
            [InlineKeyboardButton(text="Назад", callback_data=CB.MENU_BETTING)],
        ]
    )


def kb_betting_list(amount: float, lobbies_list: List[Lobby]) -> InlineKeyboardMarkup:
    buttons = []
    for lb in lobbies_list:
        text = f"#{lb.display_id} • {len(lb.players)}/{lb.max_players}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"{CB.BETTING_JOIN}{lb.display_id}")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data=f"{CB.BETTING_SELECT}{amount}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_lobby_settings(lobby: Lobby) -> InlineKeyboardMarkup:
    rows = []
    lock_text = "Разблокировать лобби" if lobby.is_locked else "Заблокировать лобби"
    rows.append([InlineKeyboardButton(text=lock_text, callback_data=CB.LOBBY_LOCK_TOGGLE)])
    if lobby.mode in (LobbyMode.open, LobbyMode.closed):
        rows.append([
            InlineKeyboardButton(text="2 игрока", callback_data=f"{CB.LOBBY_SET_MAX}2"),
            InlineKeyboardButton(text="3 игрока", callback_data=f"{CB.LOBBY_SET_MAX}3"),
            InlineKeyboardButton(text="4 игрока", callback_data=f"{CB.LOBBY_SET_MAX}4"),
        ])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=CB.LOBBY_SETTINGS_BACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_confirm_lobby_leave() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да, выйти", callback_data=CB.LOBBY_LEAVE_YES)],
            [InlineKeyboardButton(text="Отмена", callback_data=CB.LOBBY_LEAVE_NO)],
        ]
    )


def kb_confirm_game_leave() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да, выйти", callback_data=CB.GAME_LEAVE_YES)],
            [InlineKeyboardButton(text="Отмена", callback_data=CB.GAME_LEAVE_NO)],
        ]
    )


def kb_betting_menu() -> InlineKeyboardMarkup:
    buttons = []
    for amount, info in STAKE_AMOUNTS.items():
        name = info.get("name", f"${amount}")
        rub = info.get("rub", int(amount * 80))
        winner = info.get("winner", amount * 2 * 0.88)
        text = f"\U0001f4b5 {name} (~{rub}\u20bd) → Выигрыш: ${winner:.2f}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"{CB.BETTING_SELECT}{amount}")])
    buttons.append([InlineKeyboardButton(text="\U0001f511 Войти по коду", callback_data=CB.MENU_JOIN)])
    buttons.append([InlineKeyboardButton(text="\U0001f519 Назад", callback_data="back:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Обновить", callback_data=CB.ADMIN_REFRESH),
                InlineKeyboardButton(text="Настройки", callback_data=CB.ADMIN_SETTINGS),
            ],
            [
                InlineKeyboardButton(text="Поддержка", callback_data=CB.ADMIN_SUPPORT),
                InlineKeyboardButton(text="Матчи", callback_data=CB.ADMIN_MATCHES),
            ],
            [
                InlineKeyboardButton(text="Уведомления", callback_data=CB.ADMIN_NOTIFY),
                InlineKeyboardButton(text="Система", callback_data=CB.ADMIN_SYSTEM),
            ],
            [InlineKeyboardButton(text="Очистка БД", callback_data=CB.ADMIN_CLEANUP)],
            [InlineKeyboardButton(text="Назад", callback_data="back:menu")],
        ]
    )


def kb_admin_cleanup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Полная очистка", callback_data=CB.ADMIN_CLEANUP_FULL)],
            [InlineKeyboardButton(text="Очистка по времени", callback_data=CB.ADMIN_CLEANUP_TIME)],
            [InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)],
        ]
    )


def kb_admin_cleanup_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да, очистить", callback_data=CB.ADMIN_CLEANUP_FULL_CONFIRM)],
            [InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_CLEANUP_CANCEL)],
        ]
    )


def kb_admin_cleanup_units() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Секунды", callback_data=f"{CB.ADMIN_CLEANUP_UNIT}sec"),
                InlineKeyboardButton(text="Минуты", callback_data=f"{CB.ADMIN_CLEANUP_UNIT}min"),
            ],
            [
                InlineKeyboardButton(text="Часы", callback_data=f"{CB.ADMIN_CLEANUP_UNIT}hour"),
                InlineKeyboardButton(text="Дни", callback_data=f"{CB.ADMIN_CLEANUP_UNIT}day"),
            ],
            [
                InlineKeyboardButton(text="Недели", callback_data=f"{CB.ADMIN_CLEANUP_UNIT}week"),
            ],
            [InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_CLEANUP_CANCEL)],
        ]
    )


def kb_admin_cleanup_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_CLEANUP_CANCEL)]]
    )


def kb_profile(show_photos: bool, allow_broadcast: bool) -> InlineKeyboardMarkup:
    photo_state = "Вкл" if show_photos else "Выкл"
    notif_state = "Вкл" if allow_broadcast else "Выкл"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Фото карт: {photo_state}", callback_data=CB.PROFILE_TOGGLE_PHOTO)],
            [InlineKeyboardButton(text=f"Уведомления: {notif_state}", callback_data=CB.PROFILE_TOGGLE_NOTIFY)],
            [InlineKeyboardButton(text="Назад", callback_data="back:menu")],
        ]
    )

def kb_admin_settings() -> InlineKeyboardMarkup:
    msg_state = "Вкл" if is_admin_msg_enabled() else "Выкл"
    bet_state = "Вкл" if is_betting_enabled() else "Выкл"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Сообщения админу: {msg_state}", callback_data=CB.ADMIN_TOGGLE_MSG)],
            [InlineKeyboardButton(text=f"Ставки: {bet_state}", callback_data=CB.ADMIN_TOGGLE_BETTING)],
            [InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)],
        ]
    )


def render_admin_text() -> str:
    stats = db.get_admin_stats()
    db_ok, db_ping_ms, _ = db.ping_ms()
    db_status = "✅" if db_ok else "❌"
    db_ping_txt = f"{db_ping_ms:.0f} ms" if db_ping_ms is not None else "—"
    total_starts = db.count_unique_users("start", 0)
    return (
        f"<b>Админ панель</b>\n\n"
        f"💰 Доход (комиссия): ${stats['total_commission']:.2f}\n"
        f"🎮 Всего матчей: {stats['total_matches']}\n"
        f"🎯 Активных: {stats['active_matches']}\n"
        f"👥 Пользователей: {stats['total_users']}\n"
        f"🚀 Первых запусков: {total_starts}\n"
        f"💵 Балансы: ${stats['total_user_balance']:.2f}\n"
        f"🆘 Открытых тикетов: {stats['open_tickets']}\n"
        f"📢 Уведомлений активно: {stats['active_broadcasts']}\n"
        f"🗄️ DB: {db_status} • {db_ping_txt}\n"
    )


def render_admin_support_text(tickets: List[dict]) -> str:
    if not tickets:
        return "<b>Поддержка</b>\n\nОбращений пока нет."
    lines = ["<b>Поддержка — последние обращения</b>", ""]
    for t in tickets:
        tid = t.get("id")
        status = t.get("status", "open")
        user_id = t.get("user_id")
        username = html.escape(t.get("username") or "-")
        category = html.escape(t.get("category") or "Другое")
        text = html.escape((t.get("text") or "").strip())
        if len(text) > 80:
            text = text[:77] + "..."
        lines.append(f"#{tid} • {status} • {category}")
        lines.append(f"• {username} ({user_id})")
        lines.append(f"• {text}")
    return "\n".join(lines)


def render_admin_matches_text(matches: List[dict]) -> str:
    if not matches:
        return "<b>Матчи</b>\n\nМатчей пока нет."
    lines = ["<b>Матчи — последние</b>", ""]
    for m in matches:
        mid = m.get("match_id")
        stake = m.get("stake_amount")
        status = m.get("status")
        winner = m.get("winner_id") or "-"
        lines.append(f"{mid} • ${stake:.2f} • {status} • победитель: {winner}")
    return "\n".join(lines)


def render_admin_notify_text() -> str:
    stats = db.get_broadcast_stats()
    active = db.get_active_broadcast(now_ts())
    lines = ["<b>Уведомления</b>", ""]
    lines.append(f"Активные: {stats.get('active', 0)}")
    lines.append(f"Запланированные: {stats.get('scheduled', 0)}")
    if active and active.get("text"):
        text = html.escape(active.get("text", ""))
        if len(text) > 120:
            text = text[:117] + "..."
        lines.append(" ")
        lines.append("Текущее уведомление:")
        lines.append(text)
    return "\n".join(lines)


def render_admin_system_text(db_ok: bool, db_ping_ms: Optional[float], db_err: Optional[str], bot_ping_ms: Optional[float]) -> str:
    uptime = int(now_ts() - START_TS)
    hours = uptime // 3600
    minutes = (uptime % 3600) // 60
    db_size = 0
    try:
        db_size = os.path.getsize(STATS_DB_PATH)
    except Exception:
        pass
    status_icon = "✅" if cryptobot.enabled else "❌"
    ai_icon = "✅" if ai_service else "❌"
    token_mask = "нет"
    if TOKEN:
        token_mask = TOKEN[:4] + "..." + TOKEN[-4:]
    crypto_mask = "нет"
    if CRYPTOBOT_TOKEN:
        crypto_mask = CRYPTOBOT_TOKEN[:4] + "..." + CRYPTOBOT_TOKEN[-4:]
    lines = ["<b>Система</b>", ""]
    lines.append(f"Uptime: {hours}ч {minutes}м")
    if db.db_kind == "postgres":
        lines.append("DB size: remote")
    else:
        lines.append(f"DB size: {db_size / 1024:.1f} KB")
    lines.append(f"DB: {'OK' if db_ok else 'Ошибка'} • {db.db_kind}")
    if db_ping_ms is not None:
        lines.append(f"DB ping: {db_ping_ms:.0f} ms")
    if (not db_ok) and db_err:
        lines.append(f"DB error: {html.escape(db_err)[:120]}")
    lines.append(f"Активных лобби: {len(lobbies.lobbies)}")
    lines.append(f"Активных игр: {len(engine.games)}")
    lines.append(f"Ожидают оплату: {len(awaiting_payment)}")
    lines.append(f"CryptoBot: {status_icon}")
    lines.append(f"AI сервис: {ai_icon}")
    if bot_ping_ms is not None:
        lines.append(f"Bot API ping: {bot_ping_ms:.0f} ms")
    else:
        lines.append("Bot API ping: —")
    lines.append(f"Token TG: {token_mask}")
    lines.append(f"Token CryptoBot: {crypto_mask}")
    lines.append(f"Host: {platform.system()} {platform.release()}")
    lines.append(f"Python: {platform.python_version()}")
    return "\n".join(lines)


async def measure_bot_ping(bot: Bot) -> Optional[float]:
    try:
        start = time.perf_counter()
        await bot.get_me()
        return (time.perf_counter() - start) * 1000.0
    except Exception:
        return None


async def show_admin_panel(bot: Bot, chat_id: int, user, message: Message = None):
    if not is_admin(user.id):
        await bot.send_message(chat_id, "Нет доступа.")
        return
    text = render_admin_text()
    if message:
        await safe_edit_text(bot, chat_id, message.message_id, text, kb_admin())
        return
    await bot.send_message(chat_id, text, reply_markup=kb_admin(), parse_mode=ParseMode.HTML)


def kb_admin_msg_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_MSG_CANCEL)]]
    )


def kb_support_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.SUPPORT_CANCEL)]]
    )


def kb_support_categories() -> InlineKeyboardMarkup:
    rows = []
    for key, label in SUPPORT_CATEGORIES:
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{CB.SUPPORT_TYPE}{key}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="back:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_admin_support(tickets: List[dict], active_filter: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="Открытые", callback_data=CB.ADMIN_SUPPORT_OPEN),
            InlineKeyboardButton(text="Все", callback_data=CB.ADMIN_SUPPORT_ALL),
            InlineKeyboardButton(text="Закрытые", callback_data=CB.ADMIN_SUPPORT_CLOSED),
        ]
    ]
    for t in tickets:
        tid = t.get("id")
        rows.append(
            [
                InlineKeyboardButton(text=f"Ответить #{tid}", callback_data=f"{CB.SUPPORT_REPLY}{tid}"),
                InlineKeyboardButton(text=f"Закрыть #{tid}", callback_data=f"{CB.SUPPORT_CLOSE}{tid}"),
            ]
        )
    refresh_cb = {
        "open": CB.ADMIN_SUPPORT_OPEN,
        "closed": CB.ADMIN_SUPPORT_CLOSED,
        "all": CB.ADMIN_SUPPORT_ALL,
    }.get(active_filter, CB.ADMIN_SUPPORT_OPEN)
    rows.append([InlineKeyboardButton(text="Обновить", callback_data=refresh_cb)])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_admin_matches() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)]]
    )


def kb_admin_reply_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_SUPPORT)]]
    )


def kb_admin_notify() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать уведомление", callback_data=CB.ADMIN_NOTIFY_CREATE)],
            [InlineKeyboardButton(text="Остановить все", callback_data=CB.ADMIN_NOTIFY_STOP)],
            [InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)],
        ]
    )


def kb_admin_notify_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_NOTIFY_CANCEL)]]
    )


def kb_lobby(lobby: Lobby, me_id: int) -> InlineKeyboardMarkup:
    used = {p.color for p in lobby.players if p.color and not p.is_ai}
    rows = []
    color_buttons = []
    for c in COLORS:
        taken = c in used
        txt = f"{COLOR_EMOJI[c]} {COLOR_NAME_RU[c]}"
        if taken:
            txt += " (занят)"
        color_buttons.append(
            InlineKeyboardButton(text=txt, callback_data=(CB.LOBBY_COLOR + c) if not taken else CB.NOOP)
        )
    if color_buttons:
        rows.append(color_buttons[:2])
        rows.append(color_buttons[2:])

    all_colors = all(p.color for p in lobby.players if not p.is_ai)
    can_start = len(lobby.players) >= 2 and all_colors
    if lobby.mode == LobbyMode.betting:
        can_start = len(lobby.players) == 2 and all(p.has_paid for p in lobby.players) and all_colors
    if me_id == lobby.owner_id and lobby.status == LobbyStatus.waiting and can_start:
        rows.insert(0, [InlineKeyboardButton(text="Начать игру", callback_data=CB.LOBBY_START)])
    if me_id == lobby.owner_id and lobby.status == LobbyStatus.waiting:
        rows.insert(1, [InlineKeyboardButton(text="Параметры", callback_data=CB.LOBBY_SETTINGS)])

    rows.append(
        [
            InlineKeyboardButton(text="Обновить", callback_data=CB.LOBBY_REFRESH),
            InlineKeyboardButton(text="Выйти", callback_data=CB.LOBBY_LEAVE),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_game(lobby: Lobby, gs: GameState, me: Player) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if lobby.status == LobbyStatus.finished or gs.phase == TurnPhase.finished:
        rows.append(
            [
                InlineKeyboardButton(text="Обновить", callback_data=CB.GAME_REFRESH),
                InlineKeyboardButton(text="Выйти", callback_data=CB.GAME_LEAVE),
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if me.seat == gs.defender_seat and gs.phase == TurnPhase.defend and not gs.took:
        uncovered = [i for i, p in enumerate(gs.table) if p.defense is None]
        for idx in uncovered:
            pair = gs.table[idx]
            rows.append([InlineKeyboardButton(text=f"Отбить {idx + 1}: {pair.attack.compact}", callback_data=CB.NOOP)])
            btns = []
            for c in me.hand:
                if CardsService.beats(c, pair.attack, gs.trump):
                    payload = f"{idx}|{c.rank}|{c.suit.value}"
                    btns.append(InlineKeyboardButton(text=c.compact, callback_data=CB.GAME_DEFEND + payload))
            for i in range(0, len(btns), 3):
                rows.append(btns[i:i+3])
    else:
        btns = []
        if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
            selected = set(gs.pending_attack.get(me.seat, []))
            for c in me.hand:
                mark = "✅ " if c in selected else ""
                payload = f"a|{c.rank}|{c.suit.value}"
                btns.append(InlineKeyboardButton(text=mark + c.compact, callback_data=CB.GAME_SELECT + payload))
        elif gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
            selected = set(gs.pending_throwin.get(me.seat, []))
            for c in me.hand:
                mark = "✅ " if c in selected else ""
                payload = f"t|{c.rank}|{c.suit.value}"
                btns.append(InlineKeyboardButton(text=mark + c.compact, callback_data=CB.GAME_SELECT + payload))
        else:
            for c in me.hand:
                btns.append(InlineKeyboardButton(text=c.compact, callback_data=CB.NOOP))
        for i in range(0, len(btns), 3):
            rows.append(btns[i:i+3])

    action_row: List[InlineKeyboardButton] = []
    if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
        action_row.append(InlineKeyboardButton(text="Кинуть выбранные", callback_data=CB.GAME_DONE))
        action_row.append(InlineKeyboardButton(text="Сбросить выбор", callback_data=CB.GAME_CLEAR))
    if me.seat == gs.defender_seat:
        action_row.append(InlineKeyboardButton(text="Взять", callback_data=CB.GAME_TAKE))
    if me.seat == gs.attacker_seat and gs.is_all_covered() and gs.table:
        action_row.append(InlineKeyboardButton(text="Бито", callback_data=CB.GAME_BITO))
    if gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
        action_row.append(InlineKeyboardButton(text="Подкинуть выбранные", callback_data=CB.GAME_DONE))
        action_row.append(InlineKeyboardButton(text="Сбросить выбор", callback_data=CB.GAME_CLEAR))
    if action_row:
        rows.append(action_row)

    rows.append(
        [
            InlineKeyboardButton(text="Обновить", callback_data=CB.GAME_REFRESH),
            InlineKeyboardButton(text="Выйти", callback_data=CB.GAME_LEAVE),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def rules_text() -> str:
    return (
        "<b>📘 Правила и инструкция</b>\n\n"
        "<b>ВАЖНО: ВСЕ ОПЛАТЫ И ВЫПЛАТЫ ПРОИСХОДЯТ ЧЕРЕЗ @CryptoBot</b>\n\n"
        "<b>1. Механика игры «Дурак»</b>\n"
        "• Колода: 36 карт (6–A). Козырь — нижняя карта колоды.\n"
        "• Игроки: 2–4 (ставки — 2).\n"
        "• Цель: избавиться от всех карт. Последний с картами — дурак.\n"
        "• Первый ход: игрок с младшим козырем.\n"
        "• Атака: кладёшь карту на стол; затем можно подкидывать по номиналам на столе.\n"
        "• Защита: карта должна побить атаку (той же мастью старше или козырем).\n"
        "• «Взять» — защитник забирает все карты со стола, затем подкидывают оставшиеся.\n"
        "• «Бито» — все карты побиты, атакующий завершает ход.\n"
        "• Лимиты: не больше 4 карт на столе и не больше карт у защитника.\n"
        "• Добор: игроки добирают до 6 карт по очереди.\n\n"
        "<b>2. Режимы игры</b>\n"
        "• Открытая игра:\n"
        "  1) Создать новое лобби — выбираешь 2/3/4 игроков.\n"
        "  2) Авто подбор — бот ищет лобби до 30 секунд.\n"
        "  3) Выбрать лобби — список доступных лобби.\n"
        "• Игра с другом — приватное лобби с кодом, не видно в списках.\n"
        "• Закрытая игра — вход только по коду.\n"
        "• Игра против ИИ — одиночный матч.\n"
        "• Игра на ставки — только 2 игрока.\n\n"
        "<b>3. Лобби и параметры</b>\n"
        "• Хост задаёт лимит игроков и может заблокировать лобби.\n"
        "• Приватные лобби (с другом) не показываются в поиске и автоподборе.\n"
        "• Если матч не запускается 5 минут — лобби закрывается автоматически.\n\n"
        "<b>4. Ставки, оплата и выплаты</b>\n"
        "• После создания лобби на ставку обоим игрокам приходит счёт (USDT).\n"
        "• Матч начинается только после оплаты обоими игроками.\n"
        "• Если счёт не оплачен вовремя — ставка возвращается.\n"
        "• Победителю автоматически создаётся чек в @CryptoBot.\n"
        "• Комиссия уже учтена в сумме выигрыша.\n"
        "• Сумма выигрыша отображается в лобби и на экране матча.\n\n"
        "<b>5. AFK в ставках</b>\n"
        "• AFK считается, если на стол не кладутся карты.\n"
        "• Через 2 минуты без ходов обоим приходит кнопка «Вы в сети?».\n"
        "• Кнопка приходит максимум 2 раза за матч.\n"
        "• Если один нажал, второй — нет: победа тому, кто нажал (комиссия учтена).\n"
        "• Если оба не нажали: ставки сгорают, победителя нет.\n"
        "• Через 3 минуты без ходов матч завершается по AFK.\n\n"
        "<b>6. Профиль и персональные настройки</b>\n"
        "• В меню есть кнопка «Профиль».\n"
        "• В профиле отображаются: ID, имя, баланс, статистика выигрышей/проигрышей.\n"
        "• Настройки:\n"
        "  – «Фото карт»: выключи, если не хочешь получать картинки карт.\n"
        "  – «Уведомления»: управление сообщениями от администрации.\n\n"
        "<b>7. Поддержка</b>\n"
        "• Нажми «Поддержка» и выбери тип проблемы:\n"
        "  – Не пришла выплата\n"
        "  – Ошибка в игре\n"
        "  – Проблема с оплатой\n"
        "  – Другое\n"
        "• После выбора опиши ситуацию по шаблону.\n"
        "• Ты получишь номер обращения — по нему отвечает администрация.\n\n"
        "<b>8. Уведомления от администрации</b>\n"
        "• В главном меню может появляться блок «Уведомление».\n"
        "• Его можно отключить в «Профиле».\n\n"
        "<b>9. Полезные советы</b>\n"
        "• Следи за козырем и номиналами на столе.\n"
        "• Не затягивай ход — AFK‑правила работают в ставках.\n"
    )


def render_lobby_text(lobby: Lobby) -> str:
    lines = []
    lines.append(f"<b>Лобби</b> #{lobby.display_id} <code>{lobby.lobby_id}</code>")
    mode_ru = {
        LobbyMode.open: "открытое",
        LobbyMode.closed: "закрытое",
        LobbyMode.ai: "против ИИ",
        LobbyMode.betting: "на ставки",
    }[lobby.mode]
    lines.append(f"Режим: <b>{mode_ru}</b>")
    if lobby.mode == LobbyMode.ai and lobby.ai_difficulty:
        diff_ru = {"easy": "лёгкий", "normal": "нормальный", "hard": "тяжёлый"}[lobby.ai_difficulty.value]
        lines.append(f"Сложность: <b>{diff_ru}</b>")
    if lobby.mode in (LobbyMode.closed, LobbyMode.betting) and lobby.code:
        lines.append(f"Код приглашения: <code>{lobby.code}</code>")
    lines.append(f"Игроки: <b>{len(lobby.players)}/{lobby.max_players}</b>")
    if lobby.mode in (LobbyMode.open, LobbyMode.betting, LobbyMode.closed):
        access = "закрыто" if lobby.is_locked else "открыто"
        lines.append(f"Доступ: <b>{access}</b>")
    if lobby.mode == LobbyMode.betting and lobby.stake_amount:
        info = STAKE_AMOUNTS.get(lobby.stake_amount, {})
        winner = info.get("winner", lobby.stake_amount * 2 * 0.88)
        lines.append(f"Ставка: <b>${lobby.stake_amount}</b> → Выигрыш: <b>${winner:.2f}</b>")
    status_ru = "ожидание" if lobby.status == LobbyStatus.waiting else ("игра" if lobby.status == LobbyStatus.playing else "завершено")
    lines.append(f"Статус: <b>{status_ru}</b>")
    lines.append("")
    lines.append("<b>Игроки</b>")
    for p in lobby.players:
        col = COLOR_EMOJI.get(p.color, "⚪")
        owner = " (хост)" if p.user_id == lobby.owner_id else ""
        ai = " 🤖" if p.is_ai else ""
        pay = " ✅" if (lobby.mode == LobbyMode.betting and p.has_paid) else (" ❌" if lobby.mode == LobbyMode.betting else "")
        lines.append(f"• {p.name}{ai}{owner} — {col} {COLOR_NAME_RU.get(p.color, 'без цвета')}{pay}")
    lines.append("")
    if lobby.status == LobbyStatus.waiting:
        if lobby.mode == LobbyMode.betting:
            lines.append("Ожидание оплаты обоих игроков.")
        else:
            lines.append("Выберите цвет и нажмите «Начать игру» (если вы хост).")
    return "\n".join(lines)


def _result_block_for_player(lobby: Lobby, gs: GameState, me: Player) -> str:
    if lobby.status != LobbyStatus.finished and gs.phase != TurnPhase.finished:
        return ""
    reason = gs.end_reason or "Игра завершена."
    if gs.loser_user_id is None and gs.winners_user_ids:
        if me.user_id in gs.winners_user_ids:
            return f"\n\n<b>🏁 Игра окончена.</b>\n<b>Ничья / одновременный выход.</b>\n{reason}"
        return f"\n\n<b>🏁 Игра окончена.</b>\n{reason}"
    if gs.loser_user_id == me.user_id:
        return f"\n\n<b>🏁 Игра окончена.</b>\n<b>❌ Вы проиграли (вы — дурак).</b>\n{reason}"
    if me.user_id in gs.winners_user_ids:
        return f"\n\n<b>🏁 Игра окончена.</b>\n<b>✅ Победа!</b>\n{reason}"
    return f"\n\n<b>🏁 Игра окончена.</b>\n{reason}"


def render_game_text(lobby: Lobby, gs: GameState, me: Player, engine: GameEngine) -> str:
    def seat_name(seat: int) -> str:
        p = engine.seat_player(lobby, seat)
        if not p:
            return "—"
        col = COLOR_EMOJI.get(p.color, "⚪")
        ai = " 🤖" if p.is_ai else ""
        return f"{col} {p.name}{ai}"

    lines = []
    lines.append(f"<b>Дурак</b> • Лобби <code>{lobby.lobby_id}</code>")
    if lobby.mode == LobbyMode.betting and lobby.stake_amount:
        info = STAKE_AMOUNTS.get(lobby.stake_amount, {})
        winner = info.get("winner", lobby.stake_amount * 2 * 0.88)
        lines.append(f"Ставка: <b>${lobby.stake_amount}</b> → Выигрыш: <b>${winner:.2f}</b>")
    lines.append(f"Козырь: <b>{gs.trump.symbol}</b> • Козырная карта: <b>{gs.trump_card.label_ru}</b>")
    lines.append(f"В колоде: <b>{len(gs.deck)}</b> • Сброс: <b>{len(gs.discard)}</b>")
    lines.append(f"Ходит: <b>{seat_name(gs.attacker_seat)}</b> • Защищается: <b>{seat_name(gs.defender_seat)}</b>")
    lines.append("")
    if gs.table:
        lines.append("<b>Стол</b>")
        for i, pair in enumerate(gs.table, start=1):
            if pair.defense:
                lines.append(f"{i}. {pair.attack.compact} → {pair.defense.compact}")
            else:
                lines.append(f"{i}. {pair.attack.compact} → …")
    else:
        lines.append("Стол пуст.")
    lines.append("")
    lines.append(f"<b>Твоя рука</b> ({len(me.hand)}):")
    lines.append(" ".join([c.compact for c in me.hand]) or "—")

    pending = []
    if gs.phase == TurnPhase.attack_select:
        pending = gs.pending_attack.get(me.seat, [])
    elif gs.phase == TurnPhase.throwin_select:
        pending = gs.pending_throwin.get(me.seat, [])
    if pending:
        lines.append("")
        lines.append("<b>Выбрано:</b> " + ", ".join([c.compact for c in pending]))

    lines.append(_result_block_for_player(lobby, gs, me))
    return "\n".join(lines)


# ------------------------
# AI
# ------------------------

def heuristic_ai_action(lobby: Lobby, gs: GameState, ai: Player) -> Dict:
    trump = gs.trump
    diff = lobby.ai_difficulty or AIDifficulty.hard
    take_bias = {AIDifficulty.easy: 0.45, AIDifficulty.normal: 0.18, AIDifficulty.hard: 0.05}[diff]
    if ai.seat == gs.attacker_seat and gs.table and gs.is_all_covered():
        if diff == AIDifficulty.easy and random.random() < 0.35:
            pass
        else:
            return {"type": "bito"}
    if gs.phase == TurnPhase.attack_select and ai.seat == gs.attacker_seat:
        defender = next((p for p in lobby.players if p.seat == gs.defender_seat), None)
        if not defender:
            return {"type": "wait"}
        max_cards = gs.max_attack_cards(len(defender.hand))
        ranks_on_table = gs.all_table_ranks()
        def can_add(card: Card, current: List[Card]) -> bool:
            if len(gs.table) + len(current) >= max_cards:
                return False
            if not gs.table and not current:
                return True
            ranks = ranks_on_table | {c.rank for c in current}
            return card.rank in ranks
        if diff == AIDifficulty.easy:
            legal = [c for c in ai.hand if can_add(c, [])]
            if not legal:
                return {"type": "wait"}
            c = random.choice(legal)
            return {"type": "attack", "cards": [c.to_code()]}
        hand_sorted = sorted(ai.hand, key=lambda c: (c.suit == trump, c.rank_value))
        for c in hand_sorted:
            if can_add(c, []):
                return {"type": "attack", "cards": [c.to_code()]}
        return {"type": "wait"}
    if gs.phase == TurnPhase.defend and ai.seat == gs.defender_seat:
        if random.random() < take_bias:
            return {"type": "take"}
        uncovered = [(i, p.attack) for i, p in enumerate(gs.table) if p.defense is None]
        for idx, atk in uncovered:
            beaters = [c for c in ai.hand if CardsService.beats(c, atk, trump)]
            if beaters:
                if diff == AIDifficulty.easy:
                    c = random.choice(beaters)
                else:
                    beaters_sorted = sorted(beaters, key=lambda c: (c.suit == trump, c.rank_value))
                    c = beaters_sorted[0]
                return {"type": "defend", "pair_index": idx, "card": c.to_code()}
        return {"type": "take"}
    if gs.phase == TurnPhase.throwin_select and ai.seat != gs.defender_seat:
        allowed = gs.all_table_ranks()
        candidates = [c for c in ai.hand if c.rank in allowed]
        if not candidates:
            return {"type": "throwin_done", "cards": []}
        if diff == AIDifficulty.easy:
            c = random.choice(candidates)
            return {"type": "throwin_done", "cards": [c.to_code()]}
        candidates_sorted = sorted(candidates, key=lambda c: (c.suit == trump, c.rank_value))
        return {"type": "throwin_done", "cards": [candidates_sorted[0].to_code()]}
    return {"type": "wait"}


async def run_ai_loop_until_human_turn(bot: Bot, lobby: Lobby, gs: GameState, max_steps: int = 10):
    if lobby.mode != LobbyMode.ai or lobby.status != LobbyStatus.playing:
        return
    if gs.phase == TurnPhase.finished:
        return
    if gs.ai_lock:
        return
    ai_player = next((p for p in lobby.players if p.is_ai), None)
    human = next((p for p in lobby.players if not p.is_ai), None)
    if not ai_player or not human:
        return
    gs.ai_lock = True
    try:
        for _ in range(max_steps):
            if lobby.status != LobbyStatus.playing or gs.phase == TurnPhase.finished:
                return
            human_to_act = False
            if gs.phase == TurnPhase.attack_select and gs.attacker_seat == human.seat:
                human_to_act = True
            if gs.phase == TurnPhase.defend and gs.defender_seat == human.seat:
                human_to_act = True
            if gs.phase == TurnPhase.throwin_select and human.seat != gs.defender_seat:
                human_to_act = True
            if human_to_act:
                return
            ai_can_act = False
            if gs.phase == TurnPhase.attack_select and ai_player.seat == gs.attacker_seat:
                ai_can_act = True
            elif gs.phase == TurnPhase.defend and ai_player.seat == gs.defender_seat:
                ai_can_act = True
            elif gs.phase == TurnPhase.throwin_select and ai_player.seat != gs.defender_seat:
                ai_can_act = True
            elif ai_player.seat == gs.attacker_seat and gs.is_all_covered() and gs.table:
                ai_can_act = True
            if not ai_can_act:
                return
            await asyncio.sleep(0.55)
            action = await ai_service.choose_action(lobby, gs, ai_player)
            if action.get("type") in (None, "wait", "heuristic"):
                action = heuristic_ai_action(lobby, gs, ai_player)
            if action.get("type") == "attack":
                if gs.phase != TurnPhase.attack_select or ai_player.seat != gs.attacker_seat:
                    return
                codes = action.get("cards") or []
                gs.pending_attack[ai_player.seat] = []
                for code in codes:
                    try:
                        c = Card.from_code(code)
                    except Exception:
                        continue
                    ok, _ = engine.toggle_attack_select(lobby, ai_player, c)
                    if not ok:
                        continue
                ok, _, applied = engine.commit_attack(lobby, ai_player)
                if ok and applied:
                    await broadcast_table_card_photos(bot, lobby, gs, applied, "Атака (ИИ)")
                    await update_game_ui(bot, lobby, gs)
            elif action.get("type") == "defend":
                if gs.phase != TurnPhase.defend or ai_player.seat != gs.defender_seat:
                    return
                try:
                    pair_index = int(action.get("pair_index"))
                    c = Card.from_code(action.get("card"))
                except Exception:
                    return
                ok, _ = engine.defend(lobby, ai_player, pair_index, c)
                if ok:
                    await broadcast_table_card_photos(bot, lobby, gs, [c], "Защита (ИИ)")
                    await update_game_ui(bot, lobby, gs)
            elif action.get("type") == "take":
                if ai_player.seat != gs.defender_seat:
                    return
                ok, _ = engine.defender_take(lobby, ai_player)
                if ok:
                    await broadcast_lobby_notice(bot, lobby, f"👐 {ai_player.name} берет карты.")
                    await update_game_ui(bot, lobby, gs)
            elif action.get("type") == "throwin_done":
                if gs.phase != TurnPhase.throwin_select or ai_player.seat == gs.defender_seat:
                    return
                codes = action.get("cards") or []
                gs.pending_throwin[ai_player.seat] = []
                for code in codes:
                    try:
                        c = Card.from_code(code)
                    except Exception:
                        continue
                    ok, _ = engine.toggle_throwin_select(lobby, ai_player, c)
                    if not ok:
                        continue
                ok, _, applied = engine.commit_throwin_done(lobby)
                if ok:
                    if applied:
                        await broadcast_table_card_photos(bot, lobby, gs, applied, "Подкинули (ИИ)")
                    await cleanup_table_photos(bot, gs)
                    await update_game_ui(bot, lobby, gs)
            elif action.get("type") == "bito":
                if ai_player.seat != gs.attacker_seat:
                    return
                ok, _ = engine.attacker_bito(lobby, ai_player)
                if ok:
                    await cleanup_table_photos(bot, gs)
                    await update_game_ui(bot, lobby, gs)
            else:
                return
    finally:
        gs.ai_lock = False


# ------------------------
# ПЛАТЕЖИ И СТАВКИ
# ------------------------

async def send_betting_invoice(bot: Bot, lobby: Lobby, player: Player):
    if not lobby.stake_amount:
        return
    if not cryptobot.enabled:
        await bot.send_message(player.user_id, "Ой, что-то пошло не так.")
        return
    match = db.get_match_by_id(lobby.match_id or lobby.lobby_id) if lobby.match_id else db.get_match_by_lobby(lobby.lobby_id)
    if match:
        if player.user_id == match.get("player1_id") and match.get("crypto_invoice_id_p1"):
            return
        if player.user_id == match.get("player2_id") and match.get("crypto_invoice_id_p2"):
            return
    if player.user_id in awaiting_payment:
        return
    try:
        invoice = await cryptobot.create_invoice(
            amount=lobby.stake_amount,
            asset="USDT",
            description=f"Ставка ${lobby.stake_amount}",
            expires_in=PAYMENT_TIMEOUT,
        )
    except Exception as e:
        log_message(f"Ошибка создания счета: {e}")
        await bot.send_message(player.user_id, "Ой, что-то пошло не так.")
        return
    invoice_id = int(invoice.get("invoice_id", 0))
    invoice_url = invoice.get("bot_invoice_url", "")
    if lobby.match_id:
        db.update_match_invoice(lobby.match_id, player.user_id, str(invoice_id))
        db.create_payment_record(lobby.match_id, player.user_id, str(invoice_id), lobby.stake_amount)
    awaiting_payment[player.user_id] = {
        "invoice_id": invoice_id,
        "lobby_id": lobby.lobby_id,
        "start_time": now_ts(),
    }
    await bot.send_message(
        player.user_id,
        f"💳 <b>Оплата ставки ${lobby.stake_amount}</b>\n\n"
        f"Для старта матча оплатите счёт.\n\n"
        f"Ссылка: {invoice_url}",
        parse_mode=ParseMode.HTML,
    )
    asyncio.create_task(check_payment_loop(bot, player.user_id, invoice_id, lobby.stake_amount, lobby.lobby_id))


async def check_payment_loop(bot: Bot, user_id: int, invoice_id: int, amount: float, lobby_id: str):
    start_time = now_ts()
    while True:
        await asyncio.sleep(PAYMENT_CHECK_INTERVAL)
        paid, crypto_hash = await cryptobot.check_payment(invoice_id)
        if paid:
            db.confirm_payment(str(invoice_id), crypto_hash or "")
            lobby = lobbies.get_lobby_by_player(user_id)
            if lobby:
                p = lobby.get_player(user_id)
                if p:
                    p.has_paid = True
                await update_lobby_ui(bot, lobby)
                match = db.get_match_by_lobby(lobby_id)
                if match and match.get("status") == "ready_to_start":
                    for pl in lobby.players:
                        if pl.is_ai:
                            continue
                        try:
                            await bot.send_message(
                                pl.user_id,
                                "✅ Оплата подтверждена. Хост, нажмите «Начать игру».",
                            )
                        except Exception:
                            pass
            awaiting_payment.pop(user_id, None)
            return
        if now_ts() - start_time > PAYMENT_TIMEOUT:
            lobby = lobbies.get_lobby_by_player(user_id)
            if lobby:
                db.refund_payment(lobby.match_id or lobby_id, user_id)
                await bot.send_message(user_id, "⏰ Время оплаты истекло. Ставка возвращена.")
                await update_lobby_ui(bot, lobby)
            awaiting_payment.pop(user_id, None)
            return


async def prepare_betting_match(bot: Bot, lobby: Lobby):
    if lobby.mode != LobbyMode.betting or not lobby.stake_amount:
        return
    if len(lobby.players) < 2:
        return
    match = db.get_match_by_lobby(lobby.lobby_id)
    if match:
        lobby.match_id = match.get("match_id") or lobby.lobby_id
    elif not lobby.match_id:
        db.create_betting_match(
            match_id=lobby.lobby_id,
            lobby_id=lobby.lobby_id,
            stake=lobby.stake_amount,
            player1=lobby.players[0].user_id,
            player2=lobby.players[1].user_id,
        )
        lobby.match_id = lobby.lobby_id
    await send_betting_invoice(bot, lobby, lobby.players[0])
    await send_betting_invoice(bot, lobby, lobby.players[1])


# ------------------------
# ОБРАБОТЧИКИ МЕНЮ И ЛОББИ
# ------------------------

async def show_menu(bot: Bot, chat_id: int, user, message: Message = None):
    if not await ensure_subscribed(bot, chat_id, user):
        return
    text = render_main_menu_text(user)
    if message:
        try:
            await message.edit_text(text, reply_markup=kb_menu(), parse_mode=ParseMode.HTML)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb_menu(), parse_mode=ParseMode.HTML)


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot):
    db.record_user_event(message.from_user.id, "start")
    await show_menu(bot, message.chat.id, message.from_user)


@router.message(Command("menu"))
async def cmd_menu(message: Message, bot: Bot):
    await show_menu(bot, message.chat.id, message.from_user)


@router.message(Command("admin"))
async def cmd_admin(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await show_admin_panel(bot, message.chat.id, message.from_user)


@router.callback_query(F.data == "back:menu")
async def cb_back_menu(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    awaiting_code.discard(call.from_user.id)
    await show_menu(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data == CB.CHECK_SUB)
async def cb_check_sub(call: CallbackQuery, bot: Bot):
    if await is_user_subscribed(bot, call.from_user.id):
        await safe_answer(call, "Подписка подтверждена!")
        await show_menu(bot, call.message.chat.id, call.from_user, call.message)
        return
    await safe_answer(call, "Подписка не найдена.", show_alert=True)


@router.callback_query(F.data == CB.MENU_HELP)
async def cb_help(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    await call.message.edit_text(rules_text(), parse_mode=ParseMode.HTML, reply_markup=kb_menu())


@router.callback_query(F.data == CB.MENU_PROFILE)
async def cb_menu_profile(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    settings = get_cached_user_settings(call.from_user.id)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_profile_text(call.from_user),
        kb_profile(bool(settings.get("show_card_photos", 1)), bool(settings.get("allow_broadcast", 1))),
    )


@router.callback_query(F.data == CB.PROFILE_TOGGLE_PHOTO)
async def cb_profile_toggle_photo(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    settings = get_cached_user_settings(call.from_user.id)
    new_val = 0 if settings.get("show_card_photos", 1) else 1
    db.set_user_setting(call.from_user.id, "show_card_photos", new_val)
    settings = db.get_user_settings(call.from_user.id)
    update_cached_user_settings(call.from_user.id, settings)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_profile_text(call.from_user),
        kb_profile(bool(settings.get("show_card_photos", 1)), bool(settings.get("allow_broadcast", 1))),
    )


@router.callback_query(F.data == CB.PROFILE_TOGGLE_NOTIFY)
async def cb_profile_toggle_notify(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    settings = get_cached_user_settings(call.from_user.id)
    new_val = 0 if settings.get("allow_broadcast", 1) else 1
    db.set_user_setting(call.from_user.id, "allow_broadcast", new_val)
    settings = db.get_user_settings(call.from_user.id)
    update_cached_user_settings(call.from_user.id, settings)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_profile_text(call.from_user),
        kb_profile(bool(settings.get("show_card_photos", 1)), bool(settings.get("allow_broadcast", 1))),
    )


@router.callback_query(F.data == CB.MENU_OPEN)
async def cb_menu_open(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Открытая игра — выберите действие:",
        kb_open_menu(),
    )


@router.callback_query(F.data == CB.MENU_OPEN_CREATE)
async def cb_open_create(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Сколько игроков в лобби?",
        kb_open_create_max(),
    )


@router.callback_query(F.data == CB.MENU_OPEN_FRIEND)
async def cb_open_friend(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Сколько игроков в лобби с другом?",
        kb_open_friend_max(),
    )


@router.callback_query(F.data == CB.MENU_OPEN_AUTO)
async def cb_open_auto(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Идёт поиск лобби (до 30 секунд)...",
        kb_open_menu(),
    )
    deadline = now_ts() + 30
    while now_ts() < deadline:
        existing = lobbies.get_lobby_by_player(call.from_user.id)
        if existing:
            await safe_edit_text(
                bot,
                call.message.chat.id,
                call.message.message_id,
                render_lobby_text(existing),
                kb_lobby(existing, call.from_user.id),
            )
            return
        now = now_ts()
        candidates = [
            lb for lb in lobbies.lobbies.values()
            if lb.mode == LobbyMode.open
            and lb.is_listed
            and lobbies._is_joinable(lb)
            and (now - lb.last_activity_ts) <= LOBBY_IDLE_TIMEOUT
        ]
        if candidates:
            lobby = random.choice(candidates)
            player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
            if lobbies._try_join(lobby, player):
                player.ui_chat_id = call.message.chat.id
                player.ui_message_id = call.message.message_id
                await safe_edit_text(
                    bot,
                    call.message.chat.id,
                    call.message.message_id,
                    render_lobby_text(lobby),
                    kb_lobby(lobby, player.user_id),
                )
                await update_lobby_ui(bot, lobby)
                return
        await asyncio.sleep(3)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Лобби не найдено за 30 секунд. Попробуйте позже.",
        kb_open_menu(),
    )


@router.callback_query(F.data == CB.MENU_OPEN_LIST)
async def cb_open_list(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    now = now_ts()
    lst = [
        lb for lb in lobbies.lobbies.values()
        if lb.mode == LobbyMode.open
        and lb.is_listed
        and lobbies._is_joinable(lb)
        and (now - lb.last_activity_ts) <= LOBBY_IDLE_TIMEOUT
    ]
    lst.sort(key=lambda x: x.display_id)
    if not lst:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Нет доступных лобби.",
            kb_open_menu(),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Доступные лобби:",
        kb_open_list(lst),
    )


@router.callback_query(F.data.startswith(CB.OPEN_CREATE))
async def cb_open_create_max(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        max_players = int(call.data[len(CB.OPEN_CREATE):])
    except ValueError:
        return
    if max_players < 2 or max_players > 4:
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.open, max_players=max_players)
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.OPEN_FRIEND))
async def cb_open_friend_max(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        max_players = int(call.data[len(CB.OPEN_FRIEND):])
    except ValueError:
        return
    if max_players < 2 or max_players > 4:
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.closed, max_players=max_players)
    lobby.code = lobby.code or gen_code(6)
    lobby.is_listed = False
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.OPEN_JOIN))
async def cb_open_join(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    try:
        display_id = int(call.data[len(CB.OPEN_JOIN):])
    except ValueError:
        return
    lobby = lobbies.get_lobby_by_display_id(display_id)
    if not lobby or lobby.mode != LobbyMode.open or not lobbies._is_joinable(lobby):
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Лобби недоступно.",
            kb_open_menu(),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    if not lobbies._try_join(lobby, player):
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Лобби недоступно.",
            kb_open_menu(),
        )
        return
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.MENU_CLOSED)
async def cb_menu_closed(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await call.message.edit_text(render_lobby_text(existing), reply_markup=kb_lobby(existing, call.from_user.id), parse_mode=ParseMode.HTML)
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.closed)
    lobby.code = lobby.code or gen_code(6)
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await call.message.edit_text(render_lobby_text(lobby), reply_markup=kb_lobby(lobby, player.user_id), parse_mode=ParseMode.HTML)


@router.callback_query(F.data == CB.MENU_JOIN)
async def cb_menu_join(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    awaiting_code.add(call.from_user.id)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Введите код приглашения:",
        kb_back_menu(),
    )


@router.callback_query(F.data == CB.MENU_AI)
async def cb_menu_ai(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    await call.message.edit_text("🤖 Выбери сложность ИИ:", reply_markup=kb_ai_difficulty())


@router.callback_query(F.data.startswith(CB.AI_DIFF))
async def cb_ai_diff(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    diff_s = call.data[len(CB.AI_DIFF):]
    diff = AIDifficulty(diff_s)
    model = {
        AIDifficulty.easy: AI_MODEL_EASY,
        AIDifficulty.normal: AI_MODEL_NORMAL,
        AIDifficulty.hard: AI_MODEL_HARD,
    }[diff]
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.ai)
    lobby.ai_difficulty = diff
    lobby.ai_model = model
    ai_player = Player(user_id=-int(random.randint(10_000, 99_999)), name="ИИ", is_ai=True)
    ai_player.seat = 1
    ai_player.color = "black"
    lobby.players.append(ai_player)
    lobby.status = LobbyStatus.playing
    gs = engine.start_game(lobby)
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await call.message.edit_text(render_game_text(lobby, gs, player, engine), reply_markup=kb_game(lobby, gs, player), parse_mode=ParseMode.HTML)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.MENU_BETTING)
async def cb_betting_menu(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    if not is_betting_enabled():
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Ставки временно отключены.",
            kb_menu(),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "💰 Выберите ставку:",
        kb_betting_menu(),
    )


@router.callback_query(F.data.startswith(CB.BETTING_SELECT))
async def cb_betting_select(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_betting_enabled():
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Ставки временно отключены.",
            kb_menu(),
        )
        return
    amount_s = call.data[len(CB.BETTING_SELECT):]
    try:
        amount = float(amount_s)
    except ValueError:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Некорректная ставка.",
            kb_menu(),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        f"Ставка ${amount}. Выберите действие:",
        kb_betting_open_menu(amount),
    )


@router.callback_query(F.data.startswith(CB.BETTING_CREATE))
async def cb_betting_create(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        amount = float(call.data[len(CB.BETTING_CREATE):])
    except ValueError:
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.betting, max_players=2)
    lobby.stake_amount = amount
    lobby.code = gen_code(6)
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.BETTING_FRIEND))
async def cb_betting_friend(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        amount = float(call.data[len(CB.BETTING_FRIEND):])
    except ValueError:
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    lobby = lobbies.create_lobby(player, LobbyMode.betting, max_players=2)
    lobby.stake_amount = amount
    lobby.code = gen_code(6)
    lobby.is_listed = False
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.BETTING_AUTO))
async def cb_betting_auto(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        amount = float(call.data[len(CB.BETTING_AUTO):])
    except ValueError:
        return
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        f"Идёт поиск лобби на ставку ${amount} (до 30 секунд)...",
        kb_betting_open_menu(amount),
    )
    deadline = now_ts() + 30
    while now_ts() < deadline:
        existing = lobbies.get_lobby_by_player(call.from_user.id)
        if existing:
            await safe_edit_text(
                bot,
                call.message.chat.id,
                call.message.message_id,
                render_lobby_text(existing),
                kb_lobby(existing, call.from_user.id),
            )
            return
        now = now_ts()
        candidates = [
            lb for lb in lobbies.lobbies.values()
            if lb.mode == LobbyMode.betting
            and lb.stake_amount == amount
            and lb.is_listed
            and lobbies._is_joinable(lb)
            and (now - lb.last_activity_ts) <= LOBBY_IDLE_TIMEOUT
        ]
        if candidates:
            lobby = random.choice(candidates)
            player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
            if lobbies._try_join(lobby, player):
                player.ui_chat_id = call.message.chat.id
                player.ui_message_id = call.message.message_id
                await safe_edit_text(
                    bot,
                    call.message.chat.id,
                    call.message.message_id,
                    render_lobby_text(lobby),
                    kb_lobby(lobby, player.user_id),
                )
                await update_lobby_ui(bot, lobby)
                return
        await asyncio.sleep(3)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Лобби не найдено за 30 секунд. Попробуйте позже.",
        kb_betting_open_menu(amount),
    )


@router.callback_query(F.data.startswith(CB.BETTING_LIST))
async def cb_betting_list(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    try:
        amount = float(call.data[len(CB.BETTING_LIST):])
    except ValueError:
        return
    now = now_ts()
    lst = [
        lb for lb in lobbies.lobbies.values()
        if lb.mode == LobbyMode.betting
        and lb.stake_amount == amount
        and lb.is_listed
        and lobbies._is_joinable(lb)
        and (now - lb.last_activity_ts) <= LOBBY_IDLE_TIMEOUT
    ]
    lst.sort(key=lambda x: x.display_id)
    if not lst:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Нет доступных лобби для этой ставки.",
            kb_betting_open_menu(amount),
        )
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Доступные лобби:",
        kb_betting_list(amount, lst),
    )


@router.callback_query(F.data.startswith(CB.BETTING_JOIN))
async def cb_betting_join(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    existing = lobbies.get_lobby_by_player(call.from_user.id)
    if existing:
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            render_lobby_text(existing),
            kb_lobby(existing, call.from_user.id),
        )
        return
    try:
        display_id = int(call.data[len(CB.BETTING_JOIN):])
    except ValueError:
        return
    lobby = lobbies.get_lobby_by_display_id(display_id)
    if not lobby or lobby.mode != LobbyMode.betting or not lobbies._is_joinable(lobby):
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Лобби недоступно.",
            kb_menu(),
        )
        return
    player = Player(user_id=call.from_user.id, name=human_name(call.from_user))
    if not lobbies._try_join(lobby, player):
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Лобби недоступно.",
            kb_menu(),
        )
        return
    player.ui_chat_id = call.message.chat.id
    player.ui_message_id = call.message.message_id
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, player.user_id),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.MENU_ADMIN_MSG)
async def cb_admin_msg(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not await ensure_subscribed_for_call(call, bot):
        return
    if not is_admin_msg_enabled():
        await safe_edit_text(
            bot,
            call.message.chat.id,
            call.message.message_id,
            "Сообщения админу временно отключены.",
            kb_menu(),
        )
        return
    awaiting_support_message.discard(call.from_user.id)
    awaiting_support_category.pop(call.from_user.id, None)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Выберите тип проблемы:",
        kb_support_categories(),
    )


@router.callback_query(F.data == CB.ADMIN_MSG_CANCEL)
async def cb_admin_msg_cancel(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    awaiting_admin_message.discard(call.from_user.id)
    awaiting_support_message.discard(call.from_user.id)
    await show_menu(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data == CB.SUPPORT_CANCEL)
async def cb_support_cancel(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    awaiting_support_message.discard(call.from_user.id)
    awaiting_support_category.pop(call.from_user.id, None)
    await show_menu(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data.startswith(CB.SUPPORT_TYPE))
async def cb_support_type(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    key = call.data[len(CB.SUPPORT_TYPE):]
    label = next((lbl for k, lbl in SUPPORT_CATEGORIES if k == key), "Другое")
    awaiting_support_category[call.from_user.id] = label
    awaiting_support_message.add(call.from_user.id)
    text = (
        "Опишите проблему.\n"
        "Укажите, если возможно:\n"
        "• ID матча или лобби\n"
        "• Сумма и валюта\n"
        "• Время события\n"
        "• Коротко что произошло"
    )
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_support_cancel(),
    )


@router.callback_query(F.data == CB.ADMIN_REFRESH)
async def cb_admin_refresh(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    await show_admin_panel(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data == CB.ADMIN_SETTINGS)
async def cb_admin_settings(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Настройки:",
        kb_admin_settings(),
    )


@router.callback_query(F.data == CB.ADMIN_NOTIFY)
async def cb_admin_notify(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_admin_notify_text(),
        kb_admin_notify(),
    )


@router.callback_query(F.data == CB.ADMIN_NOTIFY_CREATE)
async def cb_admin_notify_create(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    awaiting_admin_broadcast.pop(call.from_user.id, None)
    awaiting_support_reply.pop(call.from_user.id, None)
    awaiting_support_message.discard(call.from_user.id)
    awaiting_admin_broadcast[call.from_user.id] = {"step": "text"}
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Введите текст уведомления:",
        kb_admin_notify_cancel(),
    )


@router.callback_query(F.data == CB.ADMIN_NOTIFY_STOP)
async def cb_admin_notify_stop(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    stopped = db.stop_all_broadcasts()
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        f"Остановлено уведомлений: {stopped}",
        kb_admin_notify(),
    )


@router.callback_query(F.data == CB.ADMIN_NOTIFY_CANCEL)
async def cb_admin_notify_cancel(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    awaiting_admin_broadcast.pop(call.from_user.id, None)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_admin_notify_text(),
        kb_admin_notify(),
    )


@router.callback_query(F.data == CB.ADMIN_SUPPORT)
async def cb_admin_support(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    tickets = db.list_support_tickets(limit=8, status="open")
    text = render_admin_support_text(tickets)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_support(tickets, "open"),
    )


@router.callback_query(F.data.in_({CB.ADMIN_SUPPORT_OPEN, CB.ADMIN_SUPPORT_ALL, CB.ADMIN_SUPPORT_CLOSED}))
async def cb_admin_support_filter(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    if call.data == CB.ADMIN_SUPPORT_CLOSED:
        status = "closed"
        active = "closed"
    elif call.data == CB.ADMIN_SUPPORT_ALL:
        status = None
        active = "all"
    else:
        status = "open"
        active = "open"
    tickets = db.list_support_tickets(limit=8, status=status)
    text = render_admin_support_text(tickets)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_support(tickets, active),
    )


@router.callback_query(F.data == CB.ADMIN_MATCHES)
async def cb_admin_matches(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    matches = db.get_recent_matches(limit=10)
    text = render_admin_matches_text(matches)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_matches(),
    )


@router.callback_query(F.data == CB.ADMIN_SYSTEM)
async def cb_admin_system(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    db_ok, db_ping_ms, db_err = db.ping_ms()
    bot_ping_ms = await measure_bot_ping(bot)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_admin_system_text(db_ok, db_ping_ms, db_err, bot_ping_ms),
        kb_admin_matches(),
    )


@router.callback_query(F.data == CB.ADMIN_CLEANUP)
async def cb_admin_cleanup(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    awaiting_admin_cleanup.pop(call.from_user.id, None)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "<b>Очистка БД</b>\n\nВыберите тип очистки:",
        kb_admin_cleanup(),
    )


@router.callback_query(F.data == CB.ADMIN_CLEANUP_FULL)
async def cb_admin_cleanup_full(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    text = (
        "<b>Полная очистка БД</b>\n\n"
        "Это удалит все данные (матчи, платежи, обращения, события и настройки пользователей).\n"
        "Подтвердите действие:"
    )
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_cleanup_confirm(),
    )


@router.callback_query(F.data == CB.ADMIN_CLEANUP_FULL_CONFIRM)
async def cb_admin_cleanup_full_confirm(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    ok = db.reset_all_data()
    SUBSCRIPTION_CACHE.clear()
    NOTICE_CACHE["ts"] = 0.0
    NOTICE_CACHE["text"] = None
    USER_SETTINGS_CACHE.clear()
    text = "База очищена и пересоздана." if ok else "Не удалось очистить БД."
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_cleanup(),
    )


@router.callback_query(F.data == CB.ADMIN_CLEANUP_TIME)
async def cb_admin_cleanup_time(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    awaiting_admin_cleanup.pop(call.from_user.id, None)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Очистить данные старше указанного времени. Выберите единицу:",
        kb_admin_cleanup_units(),
    )


@router.callback_query(F.data.startswith(CB.ADMIN_CLEANUP_UNIT))
async def cb_admin_cleanup_unit(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    unit = call.data[len(CB.ADMIN_CLEANUP_UNIT):]
    if unit not in ("sec", "min", "hour", "day", "week"):
        return
    awaiting_admin_cleanup[call.from_user.id] = {"unit": unit}
    unit_ru = {
        "sec": "секундах",
        "min": "минутах",
        "hour": "часах",
        "day": "днях",
        "week": "неделях",
    }[unit]
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        f"Введите число в {unit_ru}:",
        kb_admin_cleanup_cancel(),
    )


@router.callback_query(F.data == CB.ADMIN_CLEANUP_CANCEL)
async def cb_admin_cleanup_cancel(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    awaiting_admin_cleanup.pop(call.from_user.id, None)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "<b>Очистка БД</b>\n\nВыберите тип очистки:",
        kb_admin_cleanup(),
    )


@router.callback_query(F.data == CB.ADMIN_TOGGLE_MSG)
async def cb_admin_toggle_msg(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    db.set_bool_setting("admin_msg_enabled", not is_admin_msg_enabled())
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Настройки:",
        kb_admin_settings(),
    )


@router.callback_query(F.data == CB.ADMIN_TOGGLE_BETTING)
async def cb_admin_toggle_betting(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    db.set_bool_setting("betting_enabled", not is_betting_enabled())
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Настройки:",
        kb_admin_settings(),
    )


@router.callback_query(F.data.startswith(CB.SUPPORT_REPLY))
async def cb_support_reply(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    try:
        ticket_id = int(call.data[len(CB.SUPPORT_REPLY):])
    except ValueError:
        return
    ticket = db.get_support_ticket(ticket_id)
    if not ticket:
        await safe_answer(call, "Тикет не найден.", show_alert=True)
        return
    awaiting_support_reply[call.from_user.id] = ticket_id
    msgs = db.get_support_messages(ticket_id, limit=5)
    history_lines = []
    for m in msgs:
        who = "Админ" if m.get("is_admin") else "Пользователь"
        text = html.escape((m.get("text") or "").strip())
        if len(text) > 80:
            text = text[:77] + "..."
        history_lines.append(f"{who}: {text}")
    history = "\n".join(history_lines) if history_lines else "Истории пока нет."
    category = html.escape(ticket.get("category") or "Другое")
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        f"Тикет #{ticket_id}\nКатегория: {category}\n\nПоследние сообщения:\n{history}\n\nНапишите ответ:",
        kb_admin_reply_cancel(),
    )


@router.callback_query(F.data.startswith(CB.SUPPORT_CLOSE))
async def cb_support_close(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    if not is_admin(call.from_user.id):
        return
    try:
        ticket_id = int(call.data[len(CB.SUPPORT_CLOSE):])
    except ValueError:
        return
    ticket = db.get_support_ticket(ticket_id)
    if not ticket:
        await safe_answer(call, "Тикет не найден.", show_alert=True)
        return
    db.close_support_ticket(ticket_id)
    try:
        await bot.send_message(ticket["user_id"], f"Ваше обращение #{ticket_id} закрыто.")
    except Exception:
        pass
    tickets = db.list_support_tickets(limit=8)
    text = render_admin_support_text(tickets)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        text,
        kb_admin_support(tickets, "open"),
    )


@router.callback_query(F.data == CB.LOBBY_REFRESH)
async def cb_lobby_refresh(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return
    lobbies._touch(lobby)
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.LOBBY_COLOR))
async def cb_lobby_color(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return
    color = call.data[len(CB.LOBBY_COLOR):]
    player = lobby.get_player(call.from_user.id)
    if player:
        player.color = color
    lobbies._touch(lobby)
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.LOBBY_START)
async def cb_lobby_start(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return
    if lobby.owner_id != call.from_user.id:
        await safe_answer(call, "Только хост может начать игру.", show_alert=True)
        return
    if any((not p.is_ai) and (not p.color) for p in lobby.players):
        await safe_answer(call, "Все игроки должны выбрать цвет.", show_alert=True)
        return
    if lobby.mode == LobbyMode.betting:
        if len(lobby.players) != 2 or not all(p.has_paid for p in lobby.players):
            await safe_answer(call, "Ожидается оплата обоих игроков.", show_alert=True)
            return
    if len(lobby.players) < 2:
        await safe_answer(call, "Недостаточно игроков.", show_alert=True)
        return
    lobby.status = LobbyStatus.playing
    lobbies._touch(lobby)
    gs = engine.start_game(lobby)
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.LOBBY_SETTINGS)
async def cb_lobby_settings(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby or lobby.owner_id != call.from_user.id:
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Параметры лобби:",
        kb_lobby_settings(lobby),
    )


@router.callback_query(F.data == CB.LOBBY_LOCK_TOGGLE)
async def cb_lobby_lock_toggle(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby or lobby.owner_id != call.from_user.id:
        return
    lobby.is_locked = not lobby.is_locked
    lobbies._touch(lobby)
    lobbies._refresh_open_queue(lobby)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Параметры лобби:",
        kb_lobby_settings(lobby),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.LOBBY_SET_MAX))
async def cb_lobby_set_max(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby or lobby.owner_id != call.from_user.id:
        return
    try:
        max_players = int(call.data[len(CB.LOBBY_SET_MAX):])
    except ValueError:
        return
    if max_players < 2 or max_players > 4:
        return
    if max_players < len(lobby.players):
        await safe_answer(call, "Нельзя меньше текущего числа игроков.", show_alert=True)
        return
    lobby.max_players = max_players
    lobbies._touch(lobby)
    lobbies._refresh_open_queue(lobby)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Параметры лобби:",
        kb_lobby_settings(lobby),
    )
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.LOBBY_SETTINGS_BACK)
async def cb_lobby_settings_back(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, call.from_user.id),
    )


@router.callback_query(F.data == CB.LOBBY_LEAVE)
async def cb_lobby_leave(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Выйти из лобби?",
        kb_confirm_lobby_leave(),
    )


@router.callback_query(F.data == CB.LOBBY_LEAVE_YES)
async def cb_lobby_leave_yes(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.leave(call.from_user.id)
    if lobby:
        await update_lobby_ui(bot, lobby)
    await show_menu(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data == CB.LOBBY_LEAVE_NO)
async def cb_lobby_leave_no(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await show_menu(bot, call.message.chat.id, call.from_user, call.message)
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_lobby_text(lobby),
        kb_lobby(lobby, call.from_user.id),
    )


@router.callback_query(F.data == CB.GAME_REFRESH)
async def cb_game_refresh(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    await update_game_ui(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_LEAVE)
async def cb_game_leave(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        "Выйти из матча?",
        kb_confirm_game_leave(),
    )


@router.callback_query(F.data == CB.GAME_LEAVE_YES)
async def cb_game_leave_yes(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.leave(call.from_user.id)
    if lobby:
        gs = engine.get_game(lobby.lobby_id)
        if gs:
            await update_game_ui(bot, lobby, gs)
    await show_menu(bot, call.message.chat.id, call.from_user, call.message)


@router.callback_query(F.data == CB.GAME_LEAVE_NO)
async def cb_game_leave_no(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await show_menu(bot, call.message.chat.id, call.from_user, call.message)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await show_menu(bot, call.message.chat.id, call.from_user, call.message)
        return
    await safe_edit_text(
        bot,
        call.message.chat.id,
        call.message.message_id,
        render_game_text(lobby, gs, lobby.get_player(call.from_user.id), engine),
        kb_game(lobby, gs, lobby.get_player(call.from_user.id)),
    )


@router.callback_query(F.data.startswith(CB.GAME_SELECT))
async def cb_game_select(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    payload = call.data[len(CB.GAME_SELECT):]
    parts = payload.split("|")
    if len(parts) != 3:
        return
    kind, rank, suit_s = parts
    card = Card(rank=rank, suit=Suit(suit_s))
    if kind == "a":
        ok, err = engine.toggle_attack_select(lobby, me, card)
        if not ok:
            await safe_answer(call, err, show_alert=True)
            return
    elif kind == "t":
        ok, err = engine.toggle_throwin_select(lobby, me, card)
        if not ok:
            await safe_answer(call, err, show_alert=True)
            return
    await update_game_ui(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_CLEAR)
async def cb_game_clear(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    gs.pending_attack[me.seat] = []
    gs.pending_throwin[me.seat] = []
    await update_game_ui(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_DONE)
async def cb_game_done(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
        ok, err, cards = engine.commit_attack(lobby, me)
        if not ok:
            await safe_answer(call, err, show_alert=True)
            return
        await broadcast_table_card_photos(bot, lobby, gs, cards, caption_prefix="Атака")
        await update_game_ui(bot, lobby, gs)
        await run_ai_loop_until_human_turn(bot, lobby, gs)
        return
    if gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
        ok, err, cards = engine.commit_throwin_done(lobby)
        if not ok:
            await safe_answer(call, err, show_alert=True)
            return
        if cards:
            await broadcast_table_card_photos(bot, lobby, gs, cards, caption_prefix="Подкинули")
        await cleanup_table_photos(bot, gs)
        await update_game_ui(bot, lobby, gs)
        await run_ai_loop_until_human_turn(bot, lobby, gs)
        return


@router.callback_query(F.data.startswith(CB.GAME_DEFEND))
async def cb_game_defend(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    payload = call.data[len(CB.GAME_DEFEND):]
    parts = payload.split("|")
    if len(parts) != 3:
        return
    pair_idx_s, rank, suit_s = parts
    pair_idx = int(pair_idx_s)
    card = Card(rank=rank, suit=Suit(suit_s))
    ok, err = engine.defend(lobby, me, pair_idx, card)
    if not ok:
        await safe_answer(call, err, show_alert=True)
        return
    await broadcast_table_card_photos(bot, lobby, gs, [card], caption_prefix="Отбились")
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_TAKE)
async def cb_game_take(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    ok, err = engine.defender_take(lobby, me)
    if not ok:
        await safe_answer(call, err, show_alert=True)
        return
    await broadcast_lobby_notice(bot, lobby, f"👐 {me.name} берет карты.")
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_BITO)
async def cb_game_bito(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = lobby.get_player(call.from_user.id)
    if not me:
        return
    ok, err = engine.attacker_bito(lobby, me)
    if not ok:
        await safe_answer(call, err, show_alert=True)
        return
    await cleanup_table_photos(bot, gs)
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.NOOP)
async def cb_noop(call: CallbackQuery, bot: Bot):
    await safe_answer(call)


@router.callback_query(F.data == CB.AFK_OK)
async def cb_afk_ok(call: CallbackQuery, bot: Bot):
    await safe_answer(call)
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby or lobby.mode != LobbyMode.betting:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs or not gs.afk_prompt_active:
        return
    gs.afk_prompt_responses.add(call.from_user.id)
    try:
        msg = await bot.send_message(call.from_user.id, "✅ Подтверждено.")
        asyncio.create_task(delete_later(bot, msg.chat.id, msg.message_id, 2.0))
    except Exception:
        pass


@router.message()
async def msg_any(message: Message, bot: Bot):
    uid = message.from_user.id
    text = (message.text or "").strip()
    if text.startswith("/"):
        return
    if REQUIRED_CHANNEL and not is_admin(uid):
        if not await is_user_subscribed(bot, uid):
            await ensure_subscribed(bot, message.chat.id, message.from_user)
            return
    if uid in awaiting_code:
        awaiting_code.discard(uid)
        code = text.upper()
        player = Player(user_id=uid, name=human_name(message.from_user))
        lobby = lobbies.join_closed(player, code)
        if not lobby:
            await message.answer("Код не найден или лобби заполнено.")
            return
        player.ui_chat_id = message.chat.id
        msg = await message.answer(render_lobby_text(lobby), reply_markup=kb_lobby(lobby, uid), parse_mode=ParseMode.HTML)
        player.ui_message_id = msg.message_id
        await update_lobby_ui(bot, lobby)
        if lobby.mode == LobbyMode.betting:
            await prepare_betting_match(bot, lobby)
        return
    if uid in awaiting_support_message:
        awaiting_support_message.discard(uid)
        category = awaiting_support_category.pop(uid, "Другое")
        if not is_admin_msg_enabled():
            await message.answer("Поддержка временно отключена.")
            return
        sender = human_name(message.from_user)
        ticket_id = db.create_support_ticket(uid, sender, text, category)
        if not ticket_id:
            await message.answer("Ой, что-то пошло не так. Попробуйте позже.")
            return
        await message.answer(f"Спасибо! Ваше обращение #{ticket_id} отправлено.")
        try:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=f"Ответить #{ticket_id}", callback_data=f"{CB.SUPPORT_REPLY}{ticket_id}"),
                        InlineKeyboardButton(text=f"Закрыть #{ticket_id}", callback_data=f"{CB.SUPPORT_CLOSE}{ticket_id}"),
                    ]
                ]
            )
            for admin_id in ADMIN_USER_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"🆘 Новое обращение #{ticket_id}\nТип: {category}\nОт: {sender} ({uid})\n\n{text}",
                        reply_markup=kb,
                    )
                except Exception:
                    pass
        except Exception:
            pass
        return
    if is_admin(uid) and uid in awaiting_support_reply:
        ticket_id = awaiting_support_reply.pop(uid)
        ticket = db.get_support_ticket(ticket_id)
        if not ticket:
            await message.answer("Тикет не найден.")
            return
        db.add_support_message(ticket_id, uid, True, text)
        try:
            await bot.send_message(
                ticket["user_id"],
                f"Ответ поддержки по обращению #{ticket_id}:\n{text}",
            )
        except Exception:
            pass
        await message.answer(f"Ответ по тикету #{ticket_id} отправлен.")
        return
    if is_admin(uid) and uid in awaiting_admin_cleanup:
        state = awaiting_admin_cleanup.pop(uid, {})
        unit = state.get("unit")
        if unit not in ("sec", "min", "hour", "day", "week"):
            await message.answer("Неверная единица времени.")
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await message.answer("Введите число.")
            return
        if value <= 0:
            await message.answer("Введите число больше 0.")
            return
        mult = {"sec": 1, "min": 60, "hour": 3600, "day": 86400, "week": 604800}[unit]
        seconds = value * mult
        counts = db.cleanup_older_than(seconds)
        if not counts:
            await message.answer("Не удалось выполнить очистку.")
            return
        total = sum(counts.values())
        name_map = {
            "user_events": "события",
            "support_messages": "сообщения поддержки",
            "support_tickets": "тикеты",
            "betting_payouts": "выплаты",
            "betting_payments": "платежи",
            "betting_matches": "матчи",
            "admin_broadcasts": "уведомления",
        }
        lines = ["Очистка завершена.", f"Удалено всего: {total}"]
        for key, val in counts.items():
            if val:
                lines.append(f"• {name_map.get(key, key)}: {val}")
        await message.answer("\n".join(lines))
        return
    if is_admin(uid) and uid in awaiting_admin_broadcast:
        state = awaiting_admin_broadcast.get(uid, {})
        step = state.get("step")
        if step == "text":
            awaiting_admin_broadcast[uid]["text"] = text
            awaiting_admin_broadcast[uid]["step"] = "delay"
            await message.answer("Через сколько минут отправить? (0 = сразу)")
            return
        if step == "delay":
            try:
                delay_min = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введите число минут.")
                return
            if delay_min < 0:
                delay_min = 0
            awaiting_admin_broadcast[uid]["delay"] = delay_min
            awaiting_admin_broadcast[uid]["step"] = "duration"
            await message.answer("На сколько минут включить уведомление в меню? (0 = до ручного выключения)")
            return
        if step == "duration":
            try:
                duration_min = float(text.replace(",", "."))
            except ValueError:
                await message.answer("Введите число минут.")
                return
            if duration_min < 0:
                duration_min = 0
            data = awaiting_admin_broadcast.pop(uid, {})
            msg_text = data.get("text", "").strip()
            delay_min = float(data.get("delay", 0))
            if not msg_text:
                await message.answer("Текст уведомления пустой.")
                return
            start_at = now_ts() + delay_min * 60.0
            end_at = None if duration_min == 0 else start_at + duration_min * 60.0
            bid = db.create_broadcast(msg_text, start_at, end_at, uid)
            if not bid:
                await message.answer("Ой, что-то пошло не так. Попробуйте позже.")
                return
            await message.answer(
                f"Уведомление #{bid} создано.\n"
                f"Старт через: {delay_min:g} мин\n"
                f"Длительность: {'до выключения' if duration_min == 0 else f'{duration_min:g} мин'}"
            )
            return

# =============================================================================
# ФОТО
# =============================================================================
async def broadcast_table_card_photos(bot: Bot, lobby: Lobby, gs: GameState,
                                     cards: List[Card], caption_prefix: str):
    for pl in lobby.players:
        if pl.is_ai:
            continue
        settings = db.get_user_settings(pl.user_id)
        if not settings.get("show_card_photos", 1):
            continue
        if pl.user_id not in gs.table_photo_message_ids:
            gs.table_photo_message_ids[pl.user_id] = []
        for c in cards:
            svg = c.svg_path()
            if not svg.exists():
                continue
            try:
                png = svg_to_png_bytes(svg)
                file = BufferedInputFile(png, filename=f"{c.rank}_{c.suit.value}.png")
                msg = await bot.send_photo(
                    chat_id=pl.user_id,
                    photo=file,
                    caption=f"{caption_prefix}: {c.label_ru_long}",
                )
                gs.table_photo_message_ids[pl.user_id].append(msg.message_id)
            except Exception:
                pass

async def cleanup_table_photos(bot: Bot, gs: GameState):
    for user_id, msg_ids in list(gs.table_photo_message_ids.items()):
        for mid in msg_ids:
            await safe_delete_message(bot, user_id, mid)
    gs.table_photo_message_ids = {}


async def _finish_betting_afk(bot: Bot, lobby: Lobby, gs: GameState, winner_id: Optional[int], reason: str):
    if lobby.status == LobbyStatus.finished or gs.phase == TurnPhase.finished:
        return
    lobby.status = LobbyStatus.finished
    gs.phase = TurnPhase.finished
    gs.end_reason = reason
    if winner_id:
        gs.winners_user_ids = [winner_id]
        loser = next((p.user_id for p in lobby.players if p.user_id != winner_id), None)
        gs.loser_user_id = loser
        await update_game_ui(bot, lobby, gs)
        try:
            for p in lobby.players:
                if not p.is_ai:
                    if p.user_id == winner_id:
                        await bot.send_message(p.user_id, "Матч завершён по AFK соперника. Победа за вами.")
                    else:
                        await bot.send_message(p.user_id, "Матч завершён по AFK. Вы проиграли.")
        except Exception:
            pass
    else:
        db.finish_match_no_winner(lobby.match_id or lobby.lobby_id)
        gs.winners_user_ids = []
        gs.loser_user_id = None
        await update_game_ui(bot, lobby, gs)
        try:
            for p in lobby.players:
                if not p.is_ai:
                    await bot.send_message(p.user_id, "Матч завершён по AFK. Оба игрока потеряли ставки.")
        except Exception:
            pass


async def check_betting_afk(bot: Bot):
    now = now_ts()
    for lobby in list(lobbies.lobbies.values()):
        if lobby.mode != LobbyMode.betting or lobby.status != LobbyStatus.playing:
            continue
        gs = engine.get_game(lobby.lobby_id)
        if not gs or gs.phase == TurnPhase.finished:
            continue
        idle = now - gs.last_play_ts

        if gs.afk_prompt_active:
            if now - gs.afk_prompt_started >= AFK_PROMPT_WINDOW:
                responders = set(gs.afk_prompt_responses)
                gs.afk_prompt_active = False
                gs.afk_prompt_responses.clear()
                if len(responders) == 1:
                    await _finish_betting_afk(bot, lobby, gs, next(iter(responders)), "AFK соперника")
                elif len(responders) == 0:
                    await _finish_betting_afk(bot, lobby, gs, None, "AFK обоих игроков")
                else:
                    gs.afk_last_prompt_ts = now
            continue

        if gs.afk_prompt_count >= AFK_MAX_PROMPTS and idle >= AFK_FORFEIT_DELAY:
            await _finish_betting_afk(bot, lobby, gs, None, "AFK обоих игроков")
            continue

        if idle >= AFK_PROMPT_DELAY and gs.afk_prompt_count < AFK_MAX_PROMPTS:
            if now - gs.afk_last_prompt_ts < AFK_PROMPT_DELAY:
                continue
            gs.afk_prompt_active = True
            gs.afk_prompt_started = now
            gs.afk_prompt_count += 1
            gs.afk_last_prompt_ts = now
            gs.afk_prompt_responses = set()
            for p in lobby.players:
                if p.is_ai:
                    continue
                try:
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="✅ Я в сети", callback_data=CB.AFK_OK)]
                        ]
                    )
                    await bot.send_message(
                        p.user_id,
                        "Вы в сети? Нажмите кнопку, иначе матч завершится по AFK.",
                        reply_markup=kb,
                    )
                except Exception:
                    pass


async def process_broadcasts(bot: Bot):
    now = now_ts()
    due = db.activate_due_broadcasts(now)
    if due:
        user_ids = db.get_all_user_ids()
        for b in due:
            text = b.get("text", "").strip()
            if not text:
                continue
            for uid in user_ids:
                try:
                    settings = db.get_user_settings(uid)
                    if not settings.get("allow_broadcast", 1):
                        continue
                    await bot.send_message(uid, f"📢 {text}")
                except Exception:
                    pass
                await asyncio.sleep(0.03)
    db.expire_broadcasts(now)

# =============================================================================
# UI
# =============================================================================
async def update_lobby_ui(bot: Bot, lobby: Lobby):
    for p in lobby.players:
        if p.is_ai:
            continue
        if p.ui_chat_id and p.ui_message_id:
            new_id = await safe_edit_text(
                bot,
                p.ui_chat_id,
                p.ui_message_id,
                render_lobby_text(lobby),
                kb_lobby(lobby, p.user_id),
            )
            if new_id:
                p.ui_message_id = new_id
        else:
            try:
                msg = await bot.send_message(
                    p.user_id,
                    render_lobby_text(lobby),
                    reply_markup=kb_lobby(lobby, p.user_id),
                    parse_mode=ParseMode.HTML,
                )
                p.ui_chat_id = msg.chat.id
                p.ui_message_id = msg.message_id
            except Exception:
                pass

async def finalize_betting_payout(bot: Bot, lobby: Lobby, gs: GameState):
    if lobby.mode != LobbyMode.betting:
        return
    match = db.get_match_by_lobby(lobby.lobby_id)
    if not match:
        return
    match_id = match.get("match_id") or lobby.match_id or lobby.lobby_id
    lock = payout_locks.setdefault(match_id, asyncio.Lock())
    async with lock:
        match = db.get_match_by_id(match_id) or match
        winner_id = match.get("winner_id")
        if not winner_id and gs.winners_user_ids:
            winner_id = gs.winners_user_ids[0]
        if not winner_id:
            return
        payout = None
        if match.get("status") != "finished":
            payout = db.finish_match(match_id, winner_id)
            match = db.get_match_by_id(match_id) or match
        else:
            payout = match.get("payout_amount")
        if not payout:
            return
        if db.has_payout(match_id, winner_id):
            return
        if not cryptobot.enabled:
            return
        try:
            check = None
            last_err = None
            for attempt in range(3):
                try:
                    check = await cryptobot.create_check(
                        asset="USDT",
                        amount=payout,
                        description=f"Выигрыш в игре (лобби {lobby.lobby_id})"
                    )
                    break
                except Exception as e:
                    last_err = e
                    await asyncio.sleep(2 + attempt * 3)
            if not check:
                raise last_err or Exception("Не удалось создать чек")
            def _pick(d: dict, keys: list[str]) -> str:
                for k in keys:
                    v = d.get(k)
                    if v:
                        return str(v)
                return ""
            check_hash = _pick(check, ["checkCode", "check_code", "check_hash", "hash", "code", "checkId", "check_id"])
            check_url = _pick(check, ["botCheckUrl", "bot_check_url", "checkUrl", "check_url", "url"])
            if not check_url and check_hash:
                check_url = f"https://t.me/CryptoBot?start=check_{check_hash}"
            db.create_payout_check(match_id, winner_id, payout, check_hash, check_url)
            if check_url:
                msg = (
                    f"\U0001f3c6 <b>Победа!</b>\n\n"
                    f"Вы выиграли <b>${payout:.2f}</b>\n\n"
                    f"Чек на получение: {check_url}"
                )
            elif check_hash:
                msg = (
                    f"\U0001f3c6 <b>Победа!</b>\n\n"
                    f"Вы выиграли <b>${payout:.2f}</b>\n\n"
                    f"Чек создан, но ссылка не вернулась. Код чека: <code>{check_hash}</code>"
                )
            else:
                msg = (
                    f"\U0001f3c6 <b>Победа!</b>\n\n"
                    f"Вы выиграли <b>${payout:.2f}</b>\n\n"
                    f"Чек создан, но ссылка недоступна. Напишите администратору."
                )
            await bot.send_message(winner_id, msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            log_message(f"Ошибка создания чека: {e}")
            try:
                await bot.send_message(winner_id, "Ой, что-то пошло не так.")
            except Exception:
                pass
            try:
                for admin_id in ADMIN_USER_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            f"Ошибка создания чека по матчу {match_id} для пользователя {winner_id}: {e}",
                        )
                    except Exception:
                        pass
            except Exception:
                pass
async def update_game_ui(bot: Bot, lobby: Lobby, gs: GameState):
    engine.normalize_turn_seats_after_leave(lobby, gs)
    engine._check_endgame(lobby)
    for p in lobby.players:
        if p.is_ai:
            continue
        text = render_game_text(lobby, gs, p, engine)
        kb = kb_game(lobby, gs, p)
        if p.ui_chat_id and p.ui_message_id:
            new_id = await safe_edit_text(bot, p.ui_chat_id, p.ui_message_id, text, kb)
            if new_id:
                p.ui_message_id = new_id
        else:
            try:
                msg = await bot.send_message(
                    p.user_id,
                    text,
                    reply_markup=kb,
                    parse_mode=ParseMode.HTML,
                )
                p.ui_chat_id = msg.chat.id
                p.ui_message_id = msg.message_id
            except Exception:
                pass
    if lobby.status == LobbyStatus.finished:
        await finalize_betting_payout(bot, lobby, gs)


async def broadcast_say(bot: Bot, lobby: Lobby, from_player: Player, text: str):
    prefix = f"\U0001f4ac {from_player.name}: "
    msg_text = prefix + (text.strip()[:2000])
    for p in lobby.players:
        if p.is_ai:
            continue
        try:
            m = await bot.send_message(p.user_id, msg_text, parse_mode=ParseMode.HTML)
            asyncio.create_task(delete_later(bot, p.user_id, m.message_id, 10.0))
        except Exception:
            pass


async def broadcast_lobby_notice(bot: Bot, lobby: Lobby, text: str, delete_after: float = 5.0):
    for p in lobby.players:
        if p.is_ai:
            continue
        try:
            m = await bot.send_message(p.user_id, text)
            if delete_after and delete_after > 0:
                asyncio.create_task(delete_later(bot, p.user_id, m.message_id, delete_after))
        except Exception:
            pass

@router.message(Command("say"))
async def cmd_say(message: Message, bot: Bot):
    uid = message.from_user.id
    lobby = lobbies.get_lobby_by_player(uid)
    if not lobby:
        await safe_delete_message(bot, message.chat.id, message.message_id)
        return
    ts = now_ts()
    if ts - last_say_ts.get(uid, 0) < 2.0:
        await safe_delete_message(bot, message.chat.id, message.message_id)
        warn = await message.answer("Слишком часто. Подожди немного.")
        asyncio.create_task(delete_later(bot, warn.chat.id, warn.message_id, 3.0))
        return
    last_say_ts[uid] = ts
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    await safe_delete_message(bot, message.chat.id, message.message_id)
    if len(parts) < 2 or not parts[1].strip():
        warn = await message.answer("Просто напиши сообщение — оно попадёт в чат игры.")
        asyncio.create_task(delete_later(bot, warn.chat.id, warn.message_id, 3.0))
        return
    from_player = next((p for p in lobby.players if p.user_id == uid), None)
    if not from_player:
        return
    await broadcast_say(bot, lobby, from_player, parts[1])

def render_main_menu_text(user=None, compact: bool = False) -> str:
    name = human_name(user) if user else None
    hour = datetime.now().hour
    if 5 <= hour < 12:
        greet = f"Доброе утро, {name}!" if name else "Доброе утро!"
    elif 12 <= hour < 18:
        greet = f"Добрый день, {name}!" if name else "Добрый день!"
    elif 18 <= hour < 23:
        greet = f"Добрый вечер, {name}!" if name else "Добрый вечер!"
    else:
        greet = f"Доброй ночи, {name}!" if name else "Доброй ночи!"
    accents = [
        "Сегодня удача улыбается смелым.",
        "Одна кнопка — и стол уже накрыт.",
        "Лучшая партия начинается прямо сейчас.",
    ]
    accent = random.choice(accents)
    lines = ["\U0001f3b4 Дурак • Главное меню", " "]
    lines.append(greet)
    if not compact:
        lines.append(f" {accent} ")
    lines.append(" ")
    lines.append("Режимы")
    lines.append("• Открытая игра — быстрый поиск соперников")
    lines.append("• Закрытая игра — матч по коду")
    lines.append("• Игра против ИИ — тренировка в одиночку")
    lines.append("• Игра на ставки — играй на деньги")
    lines.append(" ")
    lines.append("Нужна помощь? Загляни в «Правила».")
    show_notice = True
    if user:
        settings = get_cached_user_settings(user.id)
        show_notice = bool(settings.get("allow_broadcast", 1))
    notice_text_raw = get_cached_notice_text() if show_notice else None
    if notice_text_raw:
        notice_text = html.escape(notice_text_raw)
        lines.append(" ")
        lines.append("📢 Уведомление")
        lines.append(notice_text)
    return "\n".join(lines)


def render_profile_text(user) -> str:
    profile = db.get_user_profile(user.id)
    settings = db.get_user_settings(user.id)
    balance = profile.get("balance", 0.0)
    total_won = profile.get("total_won", 0.0)
    total_lost = profile.get("total_lost", 0.0)
    deposited = profile.get("total_deposited", 0.0)
    withdrawn = profile.get("total_withdrawn", 0.0)
    show_photos = "Вкл" if settings.get("show_card_photos", 1) else "Выкл"
    allow_broadcast = "Вкл" if settings.get("allow_broadcast", 1) else "Выкл"
    uname = human_name(user)
    return (
        "<b>👤 Профиль игрока</b>\n"
        f"<code>UID: {user.id}</code>\n"
        f"Ник: <b>{html.escape(uname)}</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        "<b>💰 Финансы</b>\n"
        f"• Баланс: <b>${balance:.2f}</b>\n"
        f"• Выигрыш: <b>${total_won:.2f}</b>\n"
        f"• Проигрыш: <b>${total_lost:.2f}</b>\n"
        f"• Депозит: <b>${deposited:.2f}</b>\n"
        f"• Вывод: <b>${withdrawn:.2f}</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        "<b>⚙️ Настройки</b>\n"
        f"• Фото карт: <b>{show_photos}</b>\n"
        f"• Уведомления: <b>{allow_broadcast}</b>\n"
        "<i>Используй кнопки ниже, чтобы переключать настройки.</i>\n"
    )

async def safe_edit_text(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
):
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return message_id
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return message_id
        try:
            msg = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return msg.message_id
        except Exception:
            return None

def svg_to_png_bytes(svg_path: Path) -> bytes:
    png_bytes = resvg_py.svg_to_bytes(svg_path=str(svg_path))
    return bytes(png_bytes)

async def safe_delete_message(bot: Bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def delete_later(bot: Bot, chat_id: int, message_id: int, delay: float = 10.0):
    await asyncio.sleep(delay)
    await safe_delete_message(bot, chat_id, message_id)

def human_name(user) -> str:
    return (getattr(user, "full_name", None) or getattr(user, "username", None) or str(getattr(user, "id", ""))).strip()

async def _render_http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.readline()
        while True:
            line = await reader.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
        body = b"OK"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n"
            b"\r\n"
            + body
        )
        await writer.drain()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def start_render_server() -> asyncio.AbstractServer:
    port_s = os.environ.get("PORT", "10000")
    try:
        port = int(port_s)
    except ValueError:
        port = 10000
    return await asyncio.start_server(_render_http_handler, host="0.0.0.0", port=port)

async def cleanup_task(bot: Bot):
    last_log_ts = 0.0
    while not SHUTDOWN_EVENT.is_set():
        await asyncio.sleep(10)
        if now_ts() - last_log_ts > 60:
            log_message("Очистка просроченных платежей и лобби...")
            last_log_ts = now_ts()
        now = now_ts()
        for user_id, payment in list(awaiting_payment.items()):
            if now - payment.get("start_time", 0) > PAYMENT_TIMEOUT:
                lobby = lobbies.get_lobby_by_player(user_id)
                if lobby:
                    db.refund_payment(lobby.match_id or lobby.lobby_id, user_id)
                    await bot.send_message(user_id, "\u23f0 Время оплаты истекло. Ставка возвращена.")
                awaiting_payment.pop(user_id, None)
        await lobbies.cleanup_stale(bot)
        await check_betting_afk(bot)
        await process_broadcasts(bot)

async def main():
    db._init_db()
    check_db_connection()
    if not validate_config():
        return
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    server = await start_render_server()
    asyncio.create_task(cleanup_task(bot))
    log_message("Бот запущен")
    status_icon = "\u2705" if cryptobot.enabled else "\u274c"
    log_message(f"CryptoBot: {status_icon}")
    log_message(f"Admin IDs: {', '.join(str(x) for x in sorted(ADMIN_USER_IDS))}")
    try:
        loop = asyncio.get_running_loop()
        def _shutdown():
            if not SHUTDOWN_EVENT.is_set():
                SHUTDOWN_EVENT.set()
                log_message("Получен сигнал завершения. Останавливаю бота...")
        try:
            loop.add_signal_handler(signal.SIGTERM, _shutdown)
            loop.add_signal_handler(signal.SIGINT, _shutdown)
        except Exception:
            pass
        while not SHUTDOWN_EVENT.is_set():
            try:
                await dp.start_polling(bot)
                if not SHUTDOWN_EVENT.is_set():
                    log_message("Polling завершился. Перезапуск через 3 секунды.")
                    await asyncio.sleep(3)
            except Exception as e:
                log_message(f"Ошибка polling: {e}. Перезапуск через 5 секунд.")
                await asyncio.sleep(5)
    finally:
        SHUTDOWN_EVENT.set()
        server.close()
        await server.wait_closed()
        await cryptobot.close()
        await bot.session.close()
        log_message("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
