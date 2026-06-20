from typing import List, Optional

from pydantic import BaseModel, Field


class TelegramAuthRequest(BaseModel):
    initData: str = Field(min_length=1)


class AuthResponse(BaseModel):
    token: str
    user_id: int


class ProfileResponse(BaseModel):
    user_id: int
    display_name: str
    username: Optional[str] = None
    first_seen_at: Optional[str] = None
    balance: float
    total_deposited: float
    total_withdrawn: float
    total_won: float
    total_lost: float
    matches_total: int
    matches_won: int
    winrate: int


class MatchResponse(BaseModel):
    match_id: str
    stake_amount: float
    player1_id: int
    player2_id: int
    opponent_id: int
    winner_id: Optional[int] = None
    status: str
    result: str
    amount_delta: float
    payout_amount: Optional[float] = None
    commission_amount: Optional[float] = None
    created_at: Optional[str] = None
    finished_at: Optional[str] = None


class RecentResultResponse(BaseModel):
    result: str
    date: Optional[str] = None


class PointsResponse(BaseModel):
    balance: int
    total_earned: int
    total_redeemed: int
    usd_equivalent: float
    rate: int


class PointsLedgerResponse(BaseModel):
    id: int
    delta: int
    reason: str
    match_id: Optional[str] = None
    created_at: Optional[str] = None


class RedeemRequest(BaseModel):
    points: int = Field(gt=0)


class ReferralResponse(BaseModel):
    total: int
    active: int
    points_earned: int
    referral_link: str


class ReferralItemResponse(BaseModel):
    user_id: int
    masked_user: str
    display_name: str
    username: Optional[str] = None
    status: str
    matches_count: int
    created_at: Optional[str] = None
    activated_at: Optional[str] = None


class TicketCreateRequest(BaseModel):
    text: str = Field(min_length=3, max_length=4000)
    category: str = Field(default="Другое", max_length=80)


class TicketMessageCreateRequest(BaseModel):
    text: str = Field(min_length=1, max_length=4000)


class TicketResponse(BaseModel):
    id: int
    user_id: int
    username: Optional[str] = None
    text: str
    category: Optional[str] = None
    status: str
    created_at: Optional[str] = None
    closed_at: Optional[str] = None


class TicketMessageResponse(BaseModel):
    id: int
    ticket_id: int
    sender_id: int
    is_admin: int
    text: str
    created_at: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    db: str
