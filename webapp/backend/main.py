from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .auth import TelegramAuthError, issue_jwt, persist_telegram_user, verify_telegram_init_data
from .config import BASE_DIR, MINI_APP_PUBLIC_URL
from .db import db
from .routes_me import router as me_router
from .schemas import AuthResponse, HealthResponse, TelegramAuthRequest


FRONTEND_DIR = BASE_DIR / "webapp" / "frontend"

app = FastAPI(title="CARTPLAY Mini App", version="1.0.0")

origins = [MINI_APP_PUBLIC_URL] if MINI_APP_PUBLIC_URL else []
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(me_router)
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.post("/api/auth/telegram", response_model=AuthResponse)
def auth_telegram(payload: TelegramAuthRequest):
    try:
        user = verify_telegram_init_data(payload.initData)
        persist_telegram_user(user)
        user_id = int(user["id"])
        return {"token": issue_jwt(user_id), "user_id": user_id}
    except TelegramAuthError:
        raise HTTPException(status_code=401, detail="Invalid Telegram auth")


@app.get("/api/health", response_model=HealthResponse)
def health():
    try:
        connected = db.health()
    except Exception:
        connected = False
    return {"status": "ok" if connected else "error", "db": "connected" if connected else "error"}


@app.get("/")
def index():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/{path:path}")
def spa_fallback(path: str):
    target = FRONTEND_DIR / path
    if target.exists() and target.is_file():
        return FileResponse(target)
    return FileResponse(FRONTEND_DIR / "index.html")
