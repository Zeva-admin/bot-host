import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Dict, List, Optional

try:
    import psycopg2
    import psycopg2.extras
except Exception:  # pragma: no cover - optional locally when SQLite is used
    psycopg2 = None

from .config import DATABASE_URL, LOCAL_SQLITE_PATH, REDEEM_POINTS_PER_USD, BOT_USERNAME


class WebDatabase:
    def __init__(self, database_url: str = DATABASE_URL):
        self.database_url = database_url
        self.db_kind = "postgres" if database_url else "sqlite"
        if self.db_kind == "postgres" and psycopg2 is None:
            raise RuntimeError("psycopg2-binary is required for DATABASE_URL")
        self.init_db()

    def _adapt(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.db_kind == "postgres" else sql

    @contextmanager
    def connect(self):
        if self.db_kind == "postgres":
            conn = psycopg2.connect(self.database_url, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn = sqlite3.connect(str(LOCAL_SQLITE_PATH))
            conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, cursor, sql: str, params: tuple = ()):
        cursor.execute(self._adapt(sql), params)
        return cursor

    @staticmethod
    def row_to_dict(row) -> Optional[dict]:
        return WebDatabase.clean(dict(row)) if row else None

    @staticmethod
    def clean(value):
        if isinstance(value, dict):
            return {key: WebDatabase.clean(item) for key, item in value.items()}
        if isinstance(value, list):
            return [WebDatabase.clean(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    def ensure_core_tables(self, cur) -> None:
        if self.db_kind == "postgres":
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_balances (
                    user_id BIGINT PRIMARY KEY,
                    balance DOUBLE PRECISION DEFAULT 0.0,
                    total_deposited DOUBLE PRECISION DEFAULT 0.0,
                    total_withdrawn DOUBLE PRECISION DEFAULT 0.0,
                    total_won DOUBLE PRECISION DEFAULT 0.0,
                    total_lost DOUBLE PRECISION DEFAULT 0.0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
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
                    payout_check_hash TEXT,
                    points_awarded INTEGER DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_points (
                    user_id BIGINT PRIMARY KEY,
                    balance INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    total_redeemed INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS points_ledger (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    delta INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    match_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                    user_id BIGINT PRIMARY KEY,
                    referrer_id BIGINT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    matches_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    activated_at TIMESTAMP
                )
                """
            )
            cur.execute(
                """
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
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS support_messages (
                    id SERIAL PRIMARY KEY,
                    ticket_id INTEGER NOT NULL,
                    sender_id BIGINT NOT NULL,
                    is_admin INTEGER DEFAULT 0,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            return

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id INTEGER PRIMARY KEY,
                balance REAL DEFAULT 0.0,
                total_deposited REAL DEFAULT 0.0,
                total_withdrawn REAL DEFAULT 0.0,
                total_won REAL DEFAULT 0.0,
                total_lost REAL DEFAULT 0.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
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
                payout_check_hash TEXT,
                points_awarded INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_points (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                total_earned INTEGER DEFAULT 0,
                total_redeemed INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS points_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT NOT NULL,
                match_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                user_id INTEGER PRIMARY KEY,
                referrer_id INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                matches_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activated_at TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                text TEXT NOT NULL,
                category TEXT DEFAULT 'Другое',
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                is_admin INTEGER DEFAULT 0,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def init_db(self) -> None:
        with self.connect() as conn:
            cur = conn.cursor()
            self.ensure_core_tables(cur)
            if self.db_kind == "postgres":
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_profile_cache (
                        user_id BIGINT PRIMARY KEY,
                        display_name TEXT,
                        username TEXT,
                        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_profile_cache (
                        user_id INTEGER PRIMARY KEY,
                        display_name TEXT,
                        username TEXT,
                        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute("PRAGMA table_info(support_tickets)")
                columns = {row[1] for row in cur.fetchall()}
                if "category" not in columns:
                    cur.execute("ALTER TABLE support_tickets ADD COLUMN category TEXT DEFAULT 'Другое'")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_web_profile_username ON user_profile_cache(username)")

    def health(self) -> bool:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return bool(cur.fetchone())

    def remember_telegram_user(self, user_id: int, display_name: str, username: str) -> None:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                """
                INSERT INTO user_profile_cache (user_id, display_name, username)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    username = excluded.username,
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (user_id, display_name or None, username or None),
            )

    def get_profile(self, user_id: int) -> Dict[str, Any]:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(cur, "SELECT * FROM user_balances WHERE user_id = ?", (user_id,))
            balance = self.row_to_dict(cur.fetchone()) or {}
            self.execute(cur, "SELECT display_name, username, first_seen_at FROM user_profile_cache WHERE user_id = ?", (user_id,))
            cached = self.row_to_dict(cur.fetchone()) or {}
            if not cached:
                self.execute(
                    cur,
                    "SELECT username FROM support_tickets WHERE user_id = ? AND username IS NOT NULL ORDER BY id DESC LIMIT 1",
                    (user_id,),
                )
                ticket_user = self.row_to_dict(cur.fetchone()) or {}
                cached["username"] = ticket_user.get("username")

            self.execute(
                cur,
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN winner_id = ? THEN 1 ELSE 0 END) AS won
                FROM betting_matches
                WHERE status = 'finished' AND (player1_id = ? OR player2_id = ?)
                """,
                (user_id, user_id, user_id),
            )
            stats = self.row_to_dict(cur.fetchone()) or {}
            total_matches = int(stats.get("total") or 0)
            won_matches = int(stats.get("won") or 0)
            winrate = round((won_matches / total_matches) * 100) if total_matches else 0

            return {
                "user_id": user_id,
                "display_name": cached.get("display_name") or cached.get("username") or f"UID {user_id}",
                "username": cached.get("username"),
                "first_seen_at": cached.get("first_seen_at"),
                "balance": float(balance.get("balance") or 0.0),
                "total_deposited": float(balance.get("total_deposited") or 0.0),
                "total_withdrawn": float(balance.get("total_withdrawn") or 0.0),
                "total_won": float(balance.get("total_won") or 0.0),
                "total_lost": float(balance.get("total_lost") or 0.0),
                "matches_total": total_matches,
                "matches_won": won_matches,
                "winrate": winrate,
            }

    def get_matches(self, user_id: int, limit: int = 20, offset: int = 0) -> List[dict]:
        limit = max(1, min(int(limit), 100))
        offset = max(0, int(offset))
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                """
                SELECT match_id, stake_amount, player1_id, player2_id, winner_id, status,
                       payout_amount, commission_amount, created_at, finished_at
                FROM betting_matches
                WHERE player1_id = ? OR player2_id = ?
                ORDER BY COALESCE(finished_at, created_at) DESC
                LIMIT ? OFFSET ?
                """,
                (user_id, user_id, limit, offset),
            )
            rows = self.clean([dict(row) for row in cur.fetchall()])
        for row in rows:
            opponent_id = row["player2_id"] if int(row["player1_id"]) == user_id else row["player1_id"]
            row["opponent_id"] = int(opponent_id)
            row["result"] = "win" if row.get("winner_id") == user_id else "lose" if row.get("winner_id") else "draw"
            stake = float(row.get("stake_amount") or 0)
            payout = float(row.get("payout_amount") or 0)
            row["amount_delta"] = payout if row["result"] == "win" else -stake if row["result"] == "lose" else 0
        return rows

    def get_recent_results(self, user_id: int, limit: int = 10) -> List[dict]:
        rows = self.get_matches(user_id, limit=limit, offset=0)
        return [{"result": row["result"], "date": row.get("finished_at") or row.get("created_at")} for row in rows]

    def get_points(self, user_id: int) -> dict:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(cur, "SELECT balance, total_earned, total_redeemed FROM user_points WHERE user_id = ?", (user_id,))
            row = self.row_to_dict(cur.fetchone()) or {}
        balance = int(row.get("balance") or 0)
        return {
            "balance": balance,
            "total_earned": int(row.get("total_earned") or 0),
            "total_redeemed": int(row.get("total_redeemed") or 0),
            "usd_equivalent": balance / REDEEM_POINTS_PER_USD,
            "rate": REDEEM_POINTS_PER_USD,
        }

    def get_points_ledger(self, user_id: int, limit: int = 30) -> List[dict]:
        limit = max(1, min(int(limit), 100))
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                "SELECT id, delta, reason, match_id, created_at FROM points_ledger WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            )
            return self.clean([dict(row) for row in cur.fetchall()])

    def redeem_points(self, user_id: int, points: int) -> Optional[dict]:
        points = int(points)
        if points <= 0:
            return None
        with self.connect() as conn:
            cur = conn.cursor()
            if self.db_kind == "postgres":
                self.execute(
                    cur,
                    """
                    UPDATE user_points
                    SET balance = balance - ?,
                        total_redeemed = total_redeemed + ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND balance >= ?
                    RETURNING balance, total_earned, total_redeemed
                    """,
                    (points, points, user_id, points),
                )
                row = self.row_to_dict(cur.fetchone())
                if not row:
                    return None
            else:
                self.execute(cur, "SELECT balance, total_earned, total_redeemed FROM user_points WHERE user_id = ?", (user_id,))
                row = self.row_to_dict(cur.fetchone())
                if not row or int(row.get("balance") or 0) < points:
                    return None
                self.execute(
                    cur,
                    """
                    UPDATE user_points
                    SET balance = balance - ?,
                        total_redeemed = total_redeemed + ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                    """,
                    (points, points, user_id),
                )
                row["balance"] = int(row["balance"]) - points
                row["total_redeemed"] = int(row.get("total_redeemed") or 0) + points
            self.execute(
                cur,
                "INSERT INTO points_ledger (user_id, delta, reason, match_id) VALUES (?, ?, 'web_redeem', NULL)",
                (user_id, -points),
            )
        updated = self.get_points(user_id)
        return updated

    def get_referrals(self, user_id: int) -> dict:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(cur, "SELECT COUNT(*) AS total FROM referrals WHERE referrer_id = ?", (user_id,))
            total = int((self.row_to_dict(cur.fetchone()) or {}).get("total") or 0)
            self.execute(cur, "SELECT COUNT(*) AS active FROM referrals WHERE referrer_id = ? AND status = 'active'", (user_id,))
            active = int((self.row_to_dict(cur.fetchone()) or {}).get("active") or 0)
            self.execute(
                cur,
                "SELECT COALESCE(SUM(delta), 0) AS earned FROM points_ledger WHERE user_id = ? AND reason LIKE 'ref%'",
                (user_id,),
            )
            points_earned = int((self.row_to_dict(cur.fetchone()) or {}).get("earned") or 0)
        return {
            "total": total,
            "active": active,
            "points_earned": points_earned,
            "referral_link": f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}",
        }

    def get_referral_list(self, user_id: int) -> List[dict]:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                """
                SELECT r.user_id, r.status, r.matches_count, r.created_at, r.activated_at,
                       p.display_name, p.username
                FROM referrals r
                LEFT JOIN user_profile_cache p ON p.user_id = r.user_id
                WHERE r.referrer_id = ?
                ORDER BY r.created_at DESC
                """,
                (user_id,),
            )
            rows = self.clean([dict(row) for row in cur.fetchall()])
        for row in rows:
            raw_id = str(row.get("user_id") or "")
            row["masked_user"] = f"UID {raw_id[:3]}***{raw_id[-2:]}" if len(raw_id) > 5 else "UID ***"
            row["display_name"] = row.get("display_name") or row.get("username") or row["masked_user"]
        return rows

    def get_user_tickets(self, user_id: int) -> List[dict]:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                """
                SELECT id, user_id, username, text, category, status, created_at, closed_at
                FROM support_tickets
                WHERE user_id = ?
                ORDER BY id DESC
                """,
                (user_id,),
            )
            return self.clean([dict(row) for row in cur.fetchall()])

    def create_ticket(self, user_id: int, username: str, text: str, category: str) -> dict:
        with self.connect() as conn:
            cur = conn.cursor()
            if self.db_kind == "postgres":
                self.execute(
                    cur,
                    """
                    INSERT INTO support_tickets (user_id, username, text, category, status)
                    VALUES (?, ?, ?, ?, 'open') RETURNING id
                    """,
                    (user_id, username, text, category),
                )
                ticket_id = int(cur.fetchone()["id"])
            else:
                self.execute(
                    cur,
                    "INSERT INTO support_tickets (user_id, username, text, category, status) VALUES (?, ?, ?, ?, 'open')",
                    (user_id, username, text, category),
                )
                ticket_id = int(cur.lastrowid)
            self.execute(
                cur,
                "INSERT INTO support_messages (ticket_id, sender_id, is_admin, text) VALUES (?, ?, 0, ?)",
                (ticket_id, user_id, text),
            )
        return self.get_ticket_for_user(user_id, ticket_id)

    def get_ticket_for_user(self, user_id: int, ticket_id: int) -> Optional[dict]:
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                "SELECT id, user_id, username, text, category, status, created_at, closed_at FROM support_tickets WHERE id = ? AND user_id = ?",
                (ticket_id, user_id),
            )
            return self.row_to_dict(cur.fetchone())

    def get_ticket_messages(self, user_id: int, ticket_id: int) -> Optional[List[dict]]:
        if not self.get_ticket_for_user(user_id, ticket_id):
            return None
        with self.connect() as conn:
            cur = conn.cursor()
            self.execute(
                cur,
                """
                SELECT id, ticket_id, sender_id, is_admin, text, created_at
                FROM support_messages
                WHERE ticket_id = ?
                ORDER BY id ASC
                """,
                (ticket_id,),
            )
            return self.clean([dict(row) for row in cur.fetchall()])

    def add_ticket_message(self, user_id: int, ticket_id: int, text: str) -> Optional[dict]:
        if not self.get_ticket_for_user(user_id, ticket_id):
            return None
        with self.connect() as conn:
            cur = conn.cursor()
            if self.db_kind == "postgres":
                self.execute(
                    cur,
                    """
                    INSERT INTO support_messages (ticket_id, sender_id, is_admin, text)
                    VALUES (?, ?, 0, ?) RETURNING id, ticket_id, sender_id, is_admin, text, created_at
                    """,
                    (ticket_id, user_id, text),
                )
                return self.clean(dict(cur.fetchone()))
            self.execute(
                cur,
                "INSERT INTO support_messages (ticket_id, sender_id, is_admin, text) VALUES (?, ?, 0, ?)",
                (ticket_id, user_id, text),
            )
            message_id = int(cur.lastrowid)
            self.execute(
                cur,
                "SELECT id, ticket_id, sender_id, is_admin, text, created_at FROM support_messages WHERE id = ?",
                (message_id,),
            )
            return self.clean(dict(cur.fetchone()))


db = WebDatabase()
