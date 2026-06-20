import os
from pathlib import Path
from typing import List


BASE_DIR = Path(__file__).resolve().parents[2]


def load_env_file(path: Path = BASE_DIR / ".env") -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file()

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
JWT_SECRET = os.environ.get("JWT_SECRET", "").strip()
JWT_EXPIRES_MINUTES = int(os.environ.get("JWT_EXPIRES_MINUTES", "1440") or "1440")
MINI_APP_PUBLIC_URL = os.environ.get("MINI_APP_PUBLIC_URL", "").strip().rstrip("/")
BOT_TOKENS_RAW = os.environ.get("BOT_TOKENS", os.environ.get("BOT_TOKEN", "")).strip()
BOT_USERNAME = os.environ.get("BOT_USERNAME", "cartplaybot").strip() or "cartplaybot"
LOCAL_SQLITE_PATH = BASE_DIR / "casino_stats.db"
REDEEM_POINTS_PER_USD = 1000


def split_tokens(raw: str) -> List[str]:
    cleaned = (raw or "").replace("\n", ",").replace(";", ",")
    return [part.strip() for part in cleaned.split(",") if part.strip()]


BOT_TOKENS = split_tokens(BOT_TOKENS_RAW)
