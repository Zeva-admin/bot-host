
import asyncio
import json
import os
import random
import string
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime

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
    from groq import Groq  # type: ignore
except Exception:
    Groq = None


# =========================
# config (inlined)
# =========================
TOKEN = "8788323258:AAESyyBf_-S2MHuklb0bTJrgls_am0Wazm4"

GROQ_API_KEY = "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"

AI_MODEL_EASY = "meta-llama/llama-prompt-guard-2-86m"
AI_MODEL_NORMAL = "llama-3.3-70b-versatile"
AI_MODEL_HARD = "openai/gpt-oss-120b"

ADMIN_USER_IDS = (7053001262, 7719220317)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS
STATS_DB_PATH = Path("casino_stats.db")

# =========================
# Player colors
# =========================
COLORS = ["red", "blue", "green", "yellow"]
COLOR_EMOJI = {
    "red": "🟥",
    "blue": "🟦",
    "green": "🟩",
    "yellow": "🟨",
    "black": "⬛",
}
COLOR_NAME_RU = {
    "red": "красный",
    "blue": "синий",
    "green": "зелёный",
    "yellow": "жёлтый",
    "black": "чёрный",
}


# =========================
# Utilities
# =========================
def now_ts() -> float:
    return time.time()


def gen_code(k: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(k))


def init_stats_db() -> None:
    try:
        with sqlite3.connect(STATS_DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    ts REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_events_type_ts ON user_events(event_type, ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_events_user ON user_events(user_id)")
            conn.execute(
                "INSERT OR IGNORE INTO kv_settings (key, value) VALUES (?, ?)",
                ("admin_msg_enabled", "1"),
            )
            conn.commit()
    except Exception:
        pass


def record_user_event(user_id: int, event_type: str) -> None:
    if user_id <= 0:
        return
    try:
        with sqlite3.connect(STATS_DB_PATH) as conn:
            conn.execute(
                "INSERT INTO user_events (user_id, event_type, ts) VALUES (?, ?, ?)",
                (user_id, event_type, now_ts()),
            )
            conn.commit()
    except Exception:
        pass


def count_unique_users(event_type: str, since_ts: float) -> int:
    try:
        with sqlite3.connect(STATS_DB_PATH) as conn:
            cur = conn.execute(
                "SELECT COUNT(DISTINCT user_id) FROM user_events WHERE event_type = ? AND ts >= ?",
                (event_type, since_ts),
            )
            row = cur.fetchone()
            return int(row[0] or 0)
    except Exception:
        return 0


def get_setting(key: str, default: str) -> str:
    try:
        with sqlite3.connect(STATS_DB_PATH) as conn:
            cur = conn.execute("SELECT value FROM kv_settings WHERE key = ?", (key,))
            row = cur.fetchone()
            return row[0] if row else default
    except Exception:
        return default


def set_setting(key: str, value: str) -> None:
    try:
        with sqlite3.connect(STATS_DB_PATH) as conn:
            conn.execute(
                "INSERT INTO kv_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            conn.commit()
    except Exception:
        pass


def get_bool_setting(key: str, default: bool = True) -> bool:
    val = get_setting(key, "1" if default else "0")
    return val == "1"


def set_bool_setting(key: str, value: bool) -> None:
    set_setting(key, "1" if value else "0")


def is_admin_msg_enabled() -> bool:
    return get_bool_setting("admin_msg_enabled", True)


def ru_day_word(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return "день"
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return "дня"
    return "дней"


def time_greeting() -> str:
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "Доброе утро"
    if 12 <= hour < 18:
        return "Добрый день"
    if 18 <= hour < 23:
        return "Добрый вечер"
    return "Доброй ночи"


def render_main_menu_text(user=None, compact: bool = False) -> str:
    name = human_name(user) if user else None
    greet = f"{time_greeting()}, <b>{name}</b>!" if name else "Выбери режим и начни игру."
    accents = [
        "Сегодня удача улыбается смелым.",
        "Одна кнопка — и стол уже накрыт.",
        "Лучшая партия начинается прямо сейчас.",
    ]
    accent = random.choice(accents)
    lines = ["<b>🎴 Дурак</b> • Главное меню", ""]
    lines.append(greet)
    if not compact:
        lines.append(f"<i>{accent}</i>")
        lines.append("")
        lines.append("<b>Режимы</b>")
        lines.append("• Открытая игра — быстрый поиск соперников")
        lines.append("• Закрытая игра — матч по коду")
        lines.append("• Игра против ИИ — тренировка в одиночку")
        lines.append("")
        lines.append("Нужна помощь? Загляни в «Правила».")
    return "\n".join(lines)


def render_admin_settings_text() -> str:
    msg_state = "включён" if is_admin_msg_enabled() else "выключен"
    lines = [
        "<b>🛠 Админ • Настройки</b>",
        "",
        f"Приём сообщений админу: <b>{msg_state}</b>",
        "Это влияет на кнопку «Сообщение админу» в меню.",
    ]
    return "\n".join(lines)


def render_admin_text() -> str:
    now = now_ts()
    days_list = [1, 3, 7, 30]
    lines = ["<b>🛠 Админ • Обзор</b>", "", "Период — запускали / играли"]
    for d in days_list:
        since = now - d * 86400
        launched = count_unique_users("launch", since)
        played = count_unique_users("play", since)
        lines.append(f"• {d} {ru_day_word(d)} — <b>{launched}</b> / <b>{played}</b>")
    lines.append("")
    lines.append(f"<i>Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}</i>")
    return "\n".join(lines)


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
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception:
            pass


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


# =========================
# Cards / Models
# =========================
class Suit(str, Enum):
    clubs = "clubs"
    diamonds = "diamonds"
    hearts = "hearts"
    spades = "spades"

    @property
    def symbol(self) -> str:
        return {
            Suit.clubs: "♣️",
            Suit.diamonds: "♦️",
            Suit.hearts: "♥️",
            Suit.spades: "♠️",
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
            self.suit.symbol.replace("️", ""),
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
            suit_needles = [self.suit.value.lower(), self.suit.name.lower(), self.suit.symbol.replace("️", "")]
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

    def sort_hand(self, trump: Suit):
        def key(c: Card):
            is_trump = 1 if c.suit == trump else 0
            return (is_trump, c.rank_value, c.suit.value)

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
    status: LobbyStatus = LobbyStatus.waiting
    players: List[Player] = field(default_factory=list)
    created_at: float = field(default_factory=now_ts)

    ai_difficulty: Optional[AIDifficulty] = None
    ai_model: Optional[str] = None


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

    def all_table_ranks(self) -> Set[str]:
        ranks = set()
        for p in self.table:
            ranks.add(p.attack.rank)
            if p.defense:
                ranks.add(p.defense.rank)
        return ranks

    def max_attack_cards(self, defender_hand_size: int) -> int:
        # requirement: table limit should be 4, not 6
        return min(4, defender_hand_size)

    def is_all_covered(self) -> bool:
        return len(self.table) > 0 and all(p.is_covered() for p in self.table)

    def has_uncovered(self) -> bool:
        return any(p.defense is None for p in self.table)


# =========================
# Services: Cards
# =========================
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


# =========================
# Lobby Manager
# =========================
class LobbyManager:
    def __init__(self):
        self.lobbies: Dict[str, Lobby] = {}
        self.player_to_lobby: Dict[int, str] = {}
        self.open_queue: List[str] = []

    def get_lobby_by_player(self, user_id: int) -> Optional[Lobby]:
        lid = self.player_to_lobby.get(user_id)
        if not lid:
            return None
        return self.lobbies.get(lid)

    def create_lobby(self, owner: Player, mode: LobbyMode) -> Lobby:
        lobby_id = gen_code(10)
        code = gen_code(6) if mode == LobbyMode.closed else None
        lobby = Lobby(lobby_id=lobby_id, mode=mode, code=code, owner_id=owner.user_id)
        owner.seat = 0
        lobby.players.append(owner)
        self.lobbies[lobby_id] = lobby
        self.player_to_lobby[owner.user_id] = lobby_id
        if mode == LobbyMode.open:
            self.open_queue.append(lobby_id)
        return lobby

    def join_open(self, player: Player) -> Lobby:
        for lid in list(self.open_queue):
            lobby = self.lobbies.get(lid)
            if not lobby or lobby.status != LobbyStatus.waiting or lobby.mode != LobbyMode.open:
                if lid in self.open_queue:
                    self.open_queue.remove(lid)
                continue
            if len(lobby.players) < 4:
                player.seat = len(lobby.players)
                lobby.players.append(player)
                self.player_to_lobby[player.user_id] = lobby.lobby_id
                if len(lobby.players) == 4 and lobby.lobby_id in self.open_queue:
                    self.open_queue.remove(lobby.lobby_id)
                return lobby
        return self.create_lobby(player, LobbyMode.open)

    def join_closed(self, player: Player, code: str) -> Optional[Lobby]:
        for lobby in self.lobbies.values():
            if lobby.mode == LobbyMode.closed and lobby.status == LobbyStatus.waiting and lobby.code == code:
                if len(lobby.players) >= 4:
                    return None
                player.seat = len(lobby.players)
                lobby.players.append(player)
                self.player_to_lobby[player.user_id] = lobby.lobby_id
                return lobby
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

        return lobby


# =========================
# Game Engine
# =========================
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
        assert gs

        if lobby.status == LobbyStatus.finished:
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


# =========================
# Groq AI + heuristics (unchanged)
# =========================
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


# =========================
# AI loop
# =========================
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


# =========================
# Callback data
# =========================
class CB:
    MENU_OPEN = "m:open"
    MENU_CLOSED = "m:closed"
    MENU_JOIN = "m:join"
    MENU_HELP = "m:help"
    MENU_AI = "m:ai"
    MENU_ADMIN_MSG = "m:admin_msg"
    AI_DIFF = "ai:diff:"  # + easy/normal/hard

    LOBBY_REFRESH = "l:refresh"
    LOBBY_START = "l:start"
    LOBBY_LEAVE = "l:leave"
    LOBBY_COLOR = "l:color:"

    GAME_REFRESH = "g:refresh"
    GAME_LEAVE = "g:leave"
    GAME_TAKE = "g:take"
    GAME_BITO = "g:bito"
    GAME_SELECT = "g:sel:"
    GAME_DONE = "g:done"
    GAME_CLEAR = "g:clear"
    GAME_DEFEND = "g:def:"

    CONFIRM_LEAVE = "c:leave"
    CONFIRM_LEAVE_YES = "c:leave:yes"
    CONFIRM_LEAVE_NO = "c:leave:no"

    ADMIN_REFRESH = "a:refresh"
    ADMIN_MSG_CANCEL = "a:cancel"
    ADMIN_SETTINGS = "a:settings"
    ADMIN_TOGGLE_MSG = "a:toggle_msg"


# =========================
# Keyboards
# =========================
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
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Открытая игра", callback_data=CB.MENU_OPEN)],
            [InlineKeyboardButton(text="🔒 Закрытая игра", callback_data=CB.MENU_CLOSED)],
            [InlineKeyboardButton(text="🔑 Войти по коду", callback_data=CB.MENU_JOIN)],
            [InlineKeyboardButton(text="🤖 Игра против ИИ", callback_data=CB.MENU_AI)],
            [InlineKeyboardButton(text="📖 Правила", callback_data=CB.MENU_HELP)],
            [InlineKeyboardButton(text="✉️ Сообщение админу", callback_data=CB.MENU_ADMIN_MSG)],
        ]
    )


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Обновить", callback_data=CB.ADMIN_REFRESH),
                InlineKeyboardButton(text="Настройки", callback_data=CB.ADMIN_SETTINGS),
            ],
            [InlineKeyboardButton(text="Назад в меню", callback_data="back:menu")],
        ]
    )


def kb_admin_settings() -> InlineKeyboardMarkup:
    state = "Вкл" if is_admin_msg_enabled() else "Выкл"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Сообщения админу: {state}", callback_data=CB.ADMIN_TOGGLE_MSG)],
            [InlineKeyboardButton(text="Назад", callback_data=CB.ADMIN_REFRESH)],
        ]
    )


def kb_admin_msg_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CB.ADMIN_MSG_CANCEL)]]
    )


def kb_confirm_leave(in_game: bool) -> InlineKeyboardMarkup:
    back_cb = CB.GAME_REFRESH if in_game else CB.LOBBY_REFRESH
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, выйти", callback_data=CB.CONFIRM_LEAVE_YES)],
            [InlineKeyboardButton(text="❌ Нет", callback_data=CB.CONFIRM_LEAVE_NO)],
            [InlineKeyboardButton(text="Назад", callback_data=back_cb)],
        ]
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
            InlineKeyboardButton(text=txt, callback_data=(CB.LOBBY_COLOR + c) if not taken else "noop")
        )
    rows.append(color_buttons[:2])
    rows.append(color_buttons[2:])

    if me_id == lobby.owner_id and lobby.status == LobbyStatus.waiting:
        rows.insert(0, [InlineKeyboardButton(text="Начать игру", callback_data=CB.LOBBY_START)])

    rows.append(
        [
            InlineKeyboardButton(text="Обновить", callback_data=CB.LOBBY_REFRESH),
            InlineKeyboardButton(text="Выйти", callback_data=CB.CONFIRM_LEAVE),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_game(lobby: Lobby, gs: GameState, me: Player) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    if lobby.status == LobbyStatus.finished or gs.phase == TurnPhase.finished:
        rows.append(
            [
                InlineKeyboardButton(text="Обновить", callback_data=CB.GAME_REFRESH),
                InlineKeyboardButton(text="Выйти", callback_data=CB.CONFIRM_LEAVE),
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if me.seat == gs.defender_seat and gs.phase == TurnPhase.defend and not gs.took:
        uncovered = [i for i, p in enumerate(gs.table) if p.defense is None]
        target_idx = uncovered[0] if uncovered else 0
        btns = []
        for c in me.hand:
            payload = f"{target_idx}|{c.rank}|{c.suit.value}"
            btns.append(InlineKeyboardButton(text=c.label_ru, callback_data=CB.GAME_DEFEND + payload))
        for i in range(0, len(btns), 2):
            rows.append(btns[i : i + 2])
    else:
        btns = []
        if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
            selected = set(gs.pending_attack.get(me.seat, []))
            for c in me.hand:
                mark = "✅ " if c in selected else ""
                payload = f"a|{c.rank}|{c.suit.value}"
                btns.append(InlineKeyboardButton(text=mark + c.label_ru, callback_data=CB.GAME_SELECT + payload))
        elif gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
            selected = set(gs.pending_throwin.get(me.seat, []))
            for c in me.hand:
                mark = "✅ " if c in selected else ""
                payload = f"t|{c.rank}|{c.suit.value}"
                btns.append(InlineKeyboardButton(text=mark + c.label_ru, callback_data=CB.GAME_SELECT + payload))
        else:
            for c in me.hand:
                btns.append(InlineKeyboardButton(text=c.label_ru, callback_data="noop"))
        for i in range(0, len(btns), 2):
            rows.append(btns[i : i + 2])

    action_row: List[InlineKeyboardButton] = []
    if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
        action_row.append(InlineKeyboardButton(text="Кинуть выбранные", callback_data=CB.GAME_DONE))
        action_row.append(InlineKeyboardButton(text="Сбросить выбор", callback_data=CB.GAME_CLEAR))
    if me.seat == gs.defender_seat:
        action_row.append(InlineKeyboardButton(text="Взять", callback_data=CB.GAME_TAKE))
    if me.seat == gs.attacker_seat:
        action_row.append(InlineKeyboardButton(text="Бито", callback_data=CB.GAME_BITO))
    if gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
        action_row.append(InlineKeyboardButton(text="Подкинуть выбранные", callback_data=CB.GAME_DONE))
        action_row.append(InlineKeyboardButton(text="Сбросить выбор", callback_data=CB.GAME_CLEAR))
    if action_row:
        rows.append(action_row)

    rows.append(
        [
            InlineKeyboardButton(text="Обновить", callback_data=CB.GAME_REFRESH),
            InlineKeyboardButton(text="Выйти", callback_data=CB.CONFIRM_LEAVE),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# Rules text
# =========================
def rules_text() -> str:
    return (
        "<b>📜 Правила и справка</b>\n\n"
        "<b>🃏 Игра «Дурак» (подкидной, 36 карт)</b>\n"
        "• Колода: 36 карт (6–A). Козырь — нижняя карта колоды.\n"
        "• Игроки: 2–4.\n"
        "• Раздача: по 6 карт каждому.\n"
        "• Цель: избавиться от всех карт. Последний с картами — дурак.\n\n"
        "<b>🧭 Ход игры</b>\n"
        "• Первый атакующий — игрок с самым младшим козырем.\n"
        "• Атака: атакующий кладёт карту(ы) на стол.\n"
        "• Защита: защитник бьёт каждой картой или берёт все карты со стола.\n"
        "• Подкидывание: подкидывать можно только карты тех же номиналов, что уже на столе.\n"
        "• Максимум карт на столе за ход — <b>4</b> (и не больше карт у защитника).\n"
        "• «Бито»: когда все карты побиты, атакующий завершает ход.\n"
        "• Добор: после хода игроки добирают до 6, начиная с атакующего.\n\n"
        "<b>💬 Чат во время игры</b>\n"
        "• Просто пиши сообщение — его увидят все игроки.\n"
        "• Сообщения удаляются автоматически через несколько секунд, чтобы не мешать игре.\n\n"
        "<b>🛎️ Как пользоваться ботом</b>\n"
        "• Все действия с картами — кнопками.\n"
        "• Выход из лобби/игры требует подтверждения.\n"
        "• В закрытое лобби вход по коду приглашения.\n"
        "• Обратная связь — кнопка «Сообщение админу» в меню.\n"
    )


# =========================
# Rendering helpers
# =========================
def render_lobby_text(lobby: Lobby) -> str:
    lines = []
    lines.append(f"<b>♦ Лобби</b> <code>{lobby.lobby_id}</code>")
    lines.append("────────────────")
    mode_ru = (
        "открытое" if lobby.mode == LobbyMode.open else ("закрытое" if lobby.mode == LobbyMode.closed else "против ИИ")
    )
    lines.append(f"Режим: <b>{mode_ru}</b>")
    if lobby.mode == LobbyMode.ai and lobby.ai_difficulty:
        diff_ru = {"easy": "лёгкий", "normal": "нормальный", "hard": "тяжёлый"}[lobby.ai_difficulty.value]
        lines.append(f"Сложность: <b>{diff_ru}</b>")
    if lobby.mode == LobbyMode.closed and lobby.code:
        lines.append(f"Код приглашения: <code>{lobby.code}</code>")
    status_ru = "ожидание" if lobby.status == LobbyStatus.waiting else "игра"
    lines.append(f"Статус: <b>{status_ru}</b>")
    lines.append("")
    lines.append("<b>Игроки</b> (2–4)")
    for p in lobby.players:
        default_emoji = "🂡"
        default_color = "без цвета"
        c = f"{COLOR_EMOJI.get(p.color, default_emoji)} {COLOR_NAME_RU.get(p.color, default_color)}"
        owner = " (хост)" if p.user_id == lobby.owner_id else ""
        ai = " 🤖" if p.is_ai else ""
        lines.append(f"• {p.name}{ai}{owner} — {c}")
    lines.append("")
    if lobby.status == LobbyStatus.waiting:
        lines.append("<b>Подсказка</b>")
        if lobby.mode == LobbyMode.open:
            lines.append("• Идёт подбор игроков. Выбери цвет и ожидай начала.")
        elif lobby.mode == LobbyMode.closed:
            lines.append("• Поделись кодом с друзьями и выбери цвет.")
        else:
            lines.append("• Выбери цвет и нажми «Начать игру».")
    else:
        lines.append("<b>Игра в процессе</b>")
        lines.append("• Нажми «Обновить», если нужно.")
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
        col = COLOR_EMOJI.get(p.color, "🂡")
        ai = " 🤖" if p.is_ai else ""
        return f"{col} {p.name}{ai}"

    lines = []
    lines.append(f"<b>Дурак (подкидной)</b> • Лобби <code>{lobby.lobby_id}</code>")
    if lobby.mode == LobbyMode.ai and lobby.ai_difficulty:
        diff_ru = {"easy": "лёгкий", "normal": "нормальный", "hard": "тяжёлый"}[lobby.ai_difficulty.value]
        lines.append(f"Сложность: <b>{diff_ru}</b>")
    lines.append(f"Козырь: <b>{gs.trump.symbol}</b> • Козырная карта: <b>{gs.trump_card.label_ru}</b>")
    lines.append(f"В колоде осталось: <b>{len(gs.deck)}</b> • Сброс: <b>{len(gs.discard)}</b>")
    lines.append(f"Ходит: <b>{seat_name(gs.attacker_seat)}</b> • Защищается: <b>{seat_name(gs.defender_seat)}</b>")
    lines.append("")
    lines.append("<b>Игроки:</b>")
    for p in lobby.players:
        col = COLOR_EMOJI.get(p.color, "🂡")
        you = " (ты)" if p.user_id == me.user_id else ""
        ai = " 🤖" if p.is_ai else ""
        lines.append(f"• {col} {p.name}{ai}{you}: <b>{len(p.hand)}</b>")

    lines.append("")
    lines.append("<b>Стол:</b>")
    if not gs.table:
        lines.append("— пусто —")
    else:
        for i, pair in enumerate(gs.table, start=1):
            if pair.defense:
                lines.append(f"{i}. {pair.attack.compact} → {pair.defense.compact}")
            else:
                lines.append(f"{i}. {pair.attack.compact} → …")

    lines.append(_result_block_for_player(lobby, gs, me))
    return "\n".join(lines)


# =========================
# State
# =========================
lobbies = LobbyManager()
engine = GameEngine()
awaiting_code: Set[int] = set()
awaiting_admin_message: Set[int] = set()
router = Router()
ai_service = GroqDurakAI(api_key=GROQ_API_KEY)

# chat anti-spam
last_say_ts: Dict[int, float] = {}


# =========================
# Photos
# =========================
async def broadcast_table_card_photos(bot: Bot, lobby: Lobby, gs: GameState, cards: List[Card], caption_prefix: str):
    for pl in lobby.players:
        if pl.is_ai:
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


# =========================
# UI
# =========================
async def update_lobby_ui(bot: Bot, lobby: Lobby):
    for p in lobby.players:
        if p.is_ai:
            continue
        if p.ui_chat_id and p.ui_message_id:
            await safe_edit_text(bot, p.ui_chat_id, p.ui_message_id, render_lobby_text(lobby), kb_lobby(lobby, p.user_id))


async def update_game_ui(bot: Bot, lobby: Lobby, gs: GameState):
    engine.normalize_turn_seats_after_leave(lobby, gs)
    engine._check_endgame(lobby)
    for p in lobby.players:
        if p.is_ai:
            continue
        if p.ui_chat_id and p.ui_message_id:
            p.sort_hand(gs.trump)
            await safe_edit_text(
                bot,
                p.ui_chat_id,
                p.ui_message_id,
                render_game_text(lobby, gs, p, engine),
                kb_game(lobby, gs, p),
            )


# =========================
# Chat with auto-delete
# =========================
async def broadcast_say(bot: Bot, lobby: Lobby, from_player: Player, text: str):
    prefix = f"💬 <b>{from_player.name}</b>: "
    msg_text = prefix + (text.strip()[:2000])

    for p in lobby.players:
        if p.is_ai:
            continue
        try:
            m = await bot.send_message(p.user_id, msg_text, parse_mode=ParseMode.HTML)
            asyncio.create_task(delete_later(bot, p.user_id, m.message_id, 10.0))
        except Exception:
            pass


@router.message(Command("say"))
async def cmd_say(message: Message, bot: Bot):
    uid = message.from_user.id
    lobby = lobbies.get_lobby_by_player(uid)
    if not lobby:
        # delete command anyway if possible
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


# =========================
# Handlers: Menu
# =========================
@router.message(CommandStart())
async def cmd_start(message: Message):
    awaiting_code.discard(message.from_user.id)
    awaiting_admin_message.discard(message.from_user.id)
    record_user_event(message.from_user.id, "launch")
    await message.answer(
        render_main_menu_text(message.from_user),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_menu(),
    )


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    awaiting_code.discard(message.from_user.id)
    awaiting_admin_message.discard(message.from_user.id)
    await message.answer(
        render_main_menu_text(message.from_user),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_menu(),
    )


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(render_admin_text(), parse_mode=ParseMode.HTML, reply_markup=kb_admin())


@router.callback_query(F.data == CB.MENU_HELP)
async def cb_help(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text(rules_text(), parse_mode=ParseMode.HTML, reply_markup=kb_menu())


@router.callback_query(F.data == CB.MENU_ADMIN_MSG)
async def cb_menu_admin_msg(call: CallbackQuery):
    await call.answer()
    uid = call.from_user.id
    awaiting_code.discard(uid)
    if not ADMIN_USER_IDS or not is_admin_msg_enabled():
        await call.message.edit_text(
            f"<b>✉️ Сообщения админу временно отключены.</b>\n\n{render_main_menu_text(call.from_user, compact=True)}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_menu(),
        )
        return
    awaiting_admin_message.add(uid)
    await call.message.edit_text(
        "<b>✉️ Сообщение админу</b>\n\nНапиши свой текст одним сообщением. Я отправлю его лично.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_admin_msg_cancel(),
    )


@router.callback_query(F.data == CB.ADMIN_MSG_CANCEL)
async def cb_admin_msg_cancel(call: CallbackQuery):
    await call.answer()
    awaiting_admin_message.discard(call.from_user.id)
    await call.message.edit_text(
        f"<b>Отменено.</b>\n\n{render_main_menu_text(call.from_user, compact=True)}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_menu(),
    )


@router.callback_query(F.data == CB.ADMIN_REFRESH)
async def cb_admin_refresh(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return
    await call.answer()
    await call.message.edit_text(render_admin_text(), parse_mode=ParseMode.HTML, reply_markup=kb_admin())


@router.callback_query(F.data == CB.ADMIN_SETTINGS)
async def cb_admin_settings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return
    await call.answer()
    await call.message.edit_text(render_admin_settings_text(), parse_mode=ParseMode.HTML, reply_markup=kb_admin_settings())


@router.callback_query(F.data == CB.ADMIN_TOGGLE_MSG)
async def cb_admin_toggle_msg(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer()
        return
    set_bool_setting("admin_msg_enabled", not is_admin_msg_enabled())
    await call.answer("Готово")
    await call.message.edit_text(render_admin_settings_text(), parse_mode=ParseMode.HTML, reply_markup=kb_admin_settings())


@router.callback_query(F.data == CB.MENU_AI)
async def cb_menu_ai(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("🤖 Выбери сложность ИИ:", reply_markup=kb_ai_difficulty())


@router.callback_query(F.data.startswith(CB.AI_DIFF))
async def cb_ai_diff(call: CallbackQuery, bot: Bot):
    await call.answer()
    user = call.from_user

    existing = lobbies.get_lobby_by_player(user.id)
    if existing:
        await call.answer("Ты уже в лобби/игре. Выйди сначала.", show_alert=True)
        return

    diff_s = call.data.split(":")[-1]
    diff = AIDifficulty(diff_s)

    model = {
        AIDifficulty.easy: AI_MODEL_EASY,
        AIDifficulty.normal: AI_MODEL_NORMAL,
        AIDifficulty.hard: AI_MODEL_HARD,
    }[diff]

    human = Player(user_id=user.id, name=human_name(user))
    lobby = lobbies.create_lobby(human, LobbyMode.ai)
    lobby.ai_difficulty = diff
    lobby.ai_model = model

    ai_player = Player(user_id=-int(random.randint(10_000, 99_999)), name="ИИ", is_ai=True)
    ai_player.seat = 1
    ai_player.color = "black"
    lobby.players.append(ai_player)

    await call.message.edit_text(render_lobby_text(lobby), reply_markup=kb_lobby(lobby, user.id))
    human.ui_chat_id = call.message.chat.id
    human.ui_message_id = call.message.message_id
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == "back:menu")
async def cb_back_menu(call: CallbackQuery):
    await call.answer()
    awaiting_code.discard(call.from_user.id)
    awaiting_admin_message.discard(call.from_user.id)
    await call.message.edit_text(
        render_main_menu_text(call.from_user, compact=True),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_menu(),
    )


@router.callback_query(F.data == CB.MENU_OPEN)
async def cb_open(call: CallbackQuery, bot: Bot):
    await call.answer()
    user = call.from_user
    existing = lobbies.get_lobby_by_player(user.id)
    if existing:
        await call.message.edit_text(render_lobby_text(existing), reply_markup=kb_lobby(existing, user.id))
        me = next(p for p in existing.players if p.user_id == user.id)
        me.ui_chat_id = call.message.chat.id
        me.ui_message_id = call.message.message_id
        return

    player = Player(user_id=user.id, name=human_name(user))
    lobby = lobbies.join_open(player)

    await call.message.edit_text(render_lobby_text(lobby), reply_markup=kb_lobby(lobby, user.id))
    me = next(p for p in lobby.players if p.user_id == user.id)
    me.ui_chat_id = call.message.chat.id
    me.ui_message_id = call.message.message_id
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.MENU_CLOSED)
async def cb_closed_create(call: CallbackQuery, bot: Bot):
    await call.answer()
    user = call.from_user
    existing = lobbies.get_lobby_by_player(user.id)
    if existing:
        await call.message.edit_text(render_lobby_text(existing), reply_markup=kb_lobby(existing, user.id))
        me = next(p for p in existing.players if p.user_id == user.id)
        me.ui_chat_id = call.message.chat.id
        me.ui_message_id = call.message.message_id
        return

    owner = Player(user_id=user.id, name=human_name(user))
    lobby = lobbies.create_lobby(owner, LobbyMode.closed)

    await call.message.edit_text(render_lobby_text(lobby), reply_markup=kb_lobby(lobby, user.id))
    owner.ui_chat_id = call.message.chat.id
    owner.ui_message_id = call.message.message_id
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.MENU_JOIN)
async def cb_join(call: CallbackQuery):
    await call.answer()
    awaiting_admin_message.discard(call.from_user.id)
    awaiting_code.add(call.from_user.id)
    await call.message.edit_text(
        "🔑 Введи код лобби одним сообщением. Пример: <code>AB12CD</code>.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Назад в меню", callback_data="back:menu")]]
        ),
    )


@router.message(F.text)
async def msg_text(message: Message, bot: Bot):
    uid = message.from_user.id
    text = (message.text or "").strip()
    if uid in awaiting_code:
        code = text.upper()
        awaiting_code.discard(uid)

        existing = lobbies.get_lobby_by_player(uid)
        if existing:
            await message.answer("Ты уже в лобби. Выйди из него, чтобы войти в другое.", reply_markup=kb_menu())
            return

        player = Player(user_id=uid, name=human_name(message.from_user))
        lobby = lobbies.join_closed(player, code)
        if not lobby:
            await message.answer("Лобби по коду не найдено или оно заполнено.", reply_markup=kb_menu())
            return

        sent = await message.answer(
            render_lobby_text(lobby), reply_markup=kb_lobby(lobby, uid), parse_mode=ParseMode.HTML
        )
        me = next(p for p in lobby.players if p.user_id == uid)
        me.ui_chat_id = sent.chat.id
        me.ui_message_id = sent.message_id
        await update_lobby_ui(bot, lobby)
        return

    if uid in awaiting_admin_message:
        awaiting_admin_message.discard(uid)
        if not text or text.startswith("/"):
            await message.answer(
                f"<b>Отменено.</b>\n\n{render_main_menu_text(message.from_user, compact=True)}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_menu(),
            )
            return
        if not ADMIN_USER_IDS or not is_admin_msg_enabled():
            await message.answer(
                f"<b>✉️ Сообщения админу временно отключены.</b>\n\n{render_main_menu_text(message.from_user, compact=True)}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_menu(),
            )
            return
        sender = human_name(message.from_user)
        for admin_id in ADMIN_USER_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"<b>✉️ Сообщение админу</b>\nОт: {sender} (<code>{uid}</code>)\n\n{text}",
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
        await message.answer(
            f"<b>Готово!</b> Сообщение отправлено.\n\n{render_main_menu_text(message.from_user, compact=True)}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_menu(),
        )
        return

    if text.startswith("/"):
        return

    lobby = lobbies.get_lobby_by_player(uid)
    if lobby and lobby.status == LobbyStatus.playing:
        if not text:
            return
        ts = now_ts()
        if ts - last_say_ts.get(uid, 0) < 2.0:
            await safe_delete_message(bot, message.chat.id, message.message_id)
            warn = await message.answer("Слишком часто. Подожди немного.")
            asyncio.create_task(delete_later(bot, warn.chat.id, warn.message_id, 3.0))
            return
        last_say_ts[uid] = ts
        from_player = next((p for p in lobby.players if p.user_id == uid), None)
        if not from_player:
            return
        await safe_delete_message(bot, message.chat.id, message.message_id)
        await broadcast_say(bot, lobby, from_player, text)
        return


# =========================
# Lobby handlers
# =========================
@router.callback_query(F.data == CB.LOBBY_REFRESH)
async def cb_lobby_refresh(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data.startswith(CB.LOBBY_COLOR))
async def cb_lobby_color(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return

    color = call.data.split(":")[-1]
    used = {p.color for p in lobby.players if p.color and not p.is_ai}
    if color in used:
        await call.answer("Этот цвет уже занят.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)
    me.color = color
    await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.CONFIRM_LEAVE)
async def cb_confirm_leave(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    in_game = bool(lobby and lobby.status == LobbyStatus.playing)
    await call.message.edit_text(
        "Вы уверены, что хотите выйти?",
        reply_markup=kb_confirm_leave(in_game=in_game),
    )


@router.callback_query(F.data == CB.CONFIRM_LEAVE_NO)
async def cb_confirm_leave_no(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Меню:", reply_markup=kb_menu())
        return
    if lobby.status == LobbyStatus.playing:
        gs = engine.get_game(lobby.lobby_id)
        if not gs:
            await call.message.edit_text("Меню:", reply_markup=kb_menu())
            return
        await update_game_ui(bot, lobby, gs)
    else:
        await update_lobby_ui(bot, lobby)


@router.callback_query(F.data == CB.CONFIRM_LEAVE_YES)
async def cb_confirm_leave_yes(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Меню:", reply_markup=kb_menu())
        return

    gs = engine.get_game(lobby.lobby_id)

    # delete my table photos
    if gs:
        msg_ids = gs.table_photo_message_ids.get(call.from_user.id, [])
        for mid in msg_ids:
            await safe_delete_message(bot, call.from_user.id, mid)
        gs.table_photo_message_ids.pop(call.from_user.id, None)

    # AI lobby: remove immediately
    if lobby.mode == LobbyMode.ai:
        if gs:
            await cleanup_table_photos(bot, gs)
        lobbies.leave(call.from_user.id)
        await call.message.edit_text("Ты вышел(ла).", reply_markup=kb_menu())
        return

    # normal lobby
    lobbies.leave(call.from_user.id)

    # update remaining
    if gs and lobby.players:
        engine.normalize_turn_seats_after_leave(lobby, gs)
        engine._check_endgame(lobby)
        await update_game_ui(bot, lobby, gs)
    elif lobby.players and lobby.status != LobbyStatus.playing:
        await update_lobby_ui(bot, lobby)

    await call.message.edit_text("Ты вышел(ла).", reply_markup=kb_menu())


@router.callback_query(F.data == CB.LOBBY_START)
async def cb_lobby_start(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в лобби.", reply_markup=kb_menu())
        return
    if call.from_user.id != lobby.owner_id:
        await call.answer("Начать игру может только хост.", show_alert=True)
        return

    if lobby.mode != LobbyMode.ai:
        if not (2 <= len(lobby.players) <= 4):
            await call.answer("Нужно 2–4 игрока.", show_alert=True)
            return
        if any((p.color is None) for p in lobby.players):
            await call.answer("Все игроки должны выбрать цвет.", show_alert=True)
            return
    else:
        human = next((p for p in lobby.players if not p.is_ai), None)
        if not human or not human.color:
            await call.answer("Выбери цвет перед стартом.", show_alert=True)
            return

    lobby.status = LobbyStatus.playing
    gs = engine.start_game(lobby)
    for p in lobby.players:
        if not p.is_ai:
            record_user_event(p.user_id, "play")

    for p in lobby.players:
        if p.is_ai:
            continue
        if p.ui_chat_id is None:
            sent = await bot.send_message(p.user_id, "Игра начинается…")
            p.ui_chat_id = sent.chat.id
            p.ui_message_id = sent.message_id

    await cleanup_table_photos(bot, gs)
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


# =========================
# Game handlers
# =========================
@router.callback_query(F.data == CB.GAME_REFRESH)
async def cb_game_refresh(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.message.edit_text("Ты не в игре.", reply_markup=kb_menu())
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.message.edit_text("Игра не найдена.", reply_markup=kb_menu())
        return
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_CLEAR)
async def cb_game_clear(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
        gs.pending_attack[me.seat] = []
    if gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
        gs.pending_throwin[me.seat] = []
    await update_game_ui(bot, lobby, gs)


@router.callback_query(F.data.startswith(CB.GAME_SELECT))
async def cb_game_select(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.answer("Ты не в игре.", show_alert=True)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.answer("Игра не найдена.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    payload = call.data[len(CB.GAME_SELECT) :]
    parts = payload.split("|")
    if len(parts) != 3:
        return
    kind, rank, suit_s = parts
    card = Card(rank=rank, suit=Suit(suit_s))

    if kind == "a":
        ok, err = engine.toggle_attack_select(lobby, me, card)
        if not ok:
            await call.answer(err, show_alert=True)
            return
    elif kind == "t":
        ok, err = engine.toggle_throwin_select(lobby, me, card)
        if not ok:
            await call.answer(err, show_alert=True)
            return

    await update_game_ui(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_DONE)
async def cb_game_done(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.answer("Ты не в игре.", show_alert=True)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.answer("Игра не найдена.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    if gs.phase == TurnPhase.attack_select and me.seat == gs.attacker_seat:
        ok, err, cards = engine.commit_attack(lobby, me)
        if not ok:
            await call.answer(err, show_alert=True)
            return
        await broadcast_table_card_photos(bot, lobby, gs, cards, caption_prefix="Атака")
        await update_game_ui(bot, lobby, gs)
        await run_ai_loop_until_human_turn(bot, lobby, gs)
        return

    if gs.phase == TurnPhase.throwin_select and me.seat != gs.defender_seat:
        ok, err, cards = engine.commit_throwin_done(lobby)
        if not ok:
            await call.answer(err, show_alert=True)
            return
        if cards:
            await broadcast_table_card_photos(bot, lobby, gs, cards, caption_prefix="Подкинули")
        await cleanup_table_photos(bot, gs)
        await update_game_ui(bot, lobby, gs)
        await run_ai_loop_until_human_turn(bot, lobby, gs)
        return

    await call.answer("Сейчас не твоё действие.", show_alert=True)


@router.callback_query(F.data.startswith(CB.GAME_DEFEND))
async def cb_game_defend(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.answer("Ты не в игре.", show_alert=True)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.answer("Игра не найдена.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    payload = call.data[len(CB.GAME_DEFEND) :]
    parts = payload.split("|")
    if len(parts) != 3:
        return
    pair_idx_s, rank, suit_s = parts
    pair_idx = int(pair_idx_s)
    card = Card(rank=rank, suit=Suit(suit_s))

    ok, err = engine.defend(lobby, me, pair_idx, card)
    if not ok:
        await call.answer(err, show_alert=True)
        return

    await broadcast_table_card_photos(bot, lobby, gs, [card], caption_prefix="Отбились")
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_TAKE)
async def cb_game_take(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.answer("Ты не в игре.", show_alert=True)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.answer("Игра не найдена.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    ok, err = engine.defender_take(lobby, me)
    if not ok:
        await call.answer(err, show_alert=True)
        return

    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


@router.callback_query(F.data == CB.GAME_BITO)
async def cb_game_bito(call: CallbackQuery, bot: Bot):
    await call.answer()
    lobby = lobbies.get_lobby_by_player(call.from_user.id)
    if not lobby:
        await call.answer("Ты не в игре.", show_alert=True)
        return
    gs = engine.get_game(lobby.lobby_id)
    if not gs:
        await call.answer("Игра не найдена.", show_alert=True)
        return
    me = next(p for p in lobby.players if p.user_id == call.from_user.id)

    ok, err = engine.attacker_bito(lobby, me)
    if not ok:
        await call.answer(err, show_alert=True)
        return

    await cleanup_table_photos(bot, gs)
    await update_game_ui(bot, lobby, gs)
    await run_ai_loop_until_human_turn(bot, lobby, gs)


# =========================
# Render HTTP (keepalive)
# =========================
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


# =========================
# Main
# =========================
async def main():
    init_stats_db()
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    server = await start_render_server()
    try:
        await dp.start_polling(bot)
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
