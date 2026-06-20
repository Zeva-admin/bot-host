import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qsl

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import BOT_TOKENS, JWT_EXPIRES_MINUTES, JWT_SECRET
from .db import db


security = HTTPBearer(auto_error=False)


class TelegramAuthError(ValueError):
    pass


def _data_check_string(items: dict) -> str:
    return "\n".join(f"{key}={value}" for key, value in sorted(items.items()))


def _verify_with_token(init_data: str, bot_token: str) -> dict:
    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    supplied_hash = parsed.pop("hash", "")
    if not supplied_hash:
        raise TelegramAuthError("Missing hash")
    data_check = _data_check_string(parsed)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    expected_hash = hmac.new(secret_key, data_check.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_hash, supplied_hash):
        raise TelegramAuthError("Invalid hash")
    auth_date = int(parsed.get("auth_date", "0") or "0")
    if auth_date <= 0 or time.time() - auth_date > 86400:
        raise TelegramAuthError("Expired auth_date")
    user_raw = parsed.get("user")
    if not user_raw:
        raise TelegramAuthError("Missing user")
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise TelegramAuthError("Invalid user") from exc
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        raise TelegramAuthError("Invalid user_id")
    return user


def verify_telegram_init_data(init_data: str) -> dict:
    if not BOT_TOKENS:
        raise TelegramAuthError("Bot token is not configured")
    last_error: Optional[Exception] = None
    for token in BOT_TOKENS:
        try:
            return _verify_with_token(init_data, token)
        except TelegramAuthError as exc:
            last_error = exc
    raise TelegramAuthError(str(last_error or "Invalid initData"))


def issue_jwt(user_id: int) -> str:
    if not JWT_SECRET:
        raise RuntimeError("JWT_SECRET is not configured")
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=JWT_EXPIRES_MINUTES)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def decode_jwt(token: str) -> int:
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT is not configured")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = int(payload.get("sub") or 0)
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    if user_id <= 0:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    return user_id


def current_user_id(credentials: HTTPAuthorizationCredentials = Depends(security)) -> int:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return decode_jwt(credentials.credentials)


def persist_telegram_user(user: dict) -> None:
    user_id = int(user["id"])
    first = (user.get("first_name") or "").strip()
    last = (user.get("last_name") or "").strip()
    display_name = " ".join(part for part in (first, last) if part).strip()
    username = (user.get("username") or "").strip()
    db.remember_telegram_user(user_id, display_name, username)
