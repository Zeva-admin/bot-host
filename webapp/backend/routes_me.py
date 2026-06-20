from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status

from .auth import current_user_id
from .db import db
from .schemas import (
    MatchResponse,
    PointsLedgerResponse,
    PointsResponse,
    ProfileResponse,
    RecentResultResponse,
    RedeemRequest,
    ReferralItemResponse,
    ReferralResponse,
    TicketCreateRequest,
    TicketMessageCreateRequest,
    TicketMessageResponse,
    TicketResponse,
)


router = APIRouter(prefix="/api/me", tags=["me"])


@router.get("", response_model=ProfileResponse)
def me(user_id: int = Depends(current_user_id)):
    return db.get_profile(user_id)


@router.get("/matches", response_model=List[MatchResponse])
def matches(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user_id: int = Depends(current_user_id),
):
    return db.get_matches(user_id, limit=limit, offset=offset)


@router.get("/matches/recent-results", response_model=List[RecentResultResponse])
def recent_results(limit: int = Query(10, ge=1, le=30), user_id: int = Depends(current_user_id)):
    return db.get_recent_results(user_id, limit=limit)


@router.get("/points", response_model=PointsResponse)
def points(user_id: int = Depends(current_user_id)):
    return db.get_points(user_id)


@router.get("/points/ledger", response_model=List[PointsLedgerResponse])
def points_ledger(limit: int = Query(30, ge=1, le=100), user_id: int = Depends(current_user_id)):
    return db.get_points_ledger(user_id, limit=limit)


@router.post("/points/redeem", response_model=PointsResponse)
def redeem_points(payload: RedeemRequest, user_id: int = Depends(current_user_id)):
    updated = db.redeem_points(user_id, payload.points)
    if not updated:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Not enough points")
    return updated


@router.get("/referrals", response_model=ReferralResponse)
def referrals(user_id: int = Depends(current_user_id)):
    return db.get_referrals(user_id)


@router.get("/referrals/list", response_model=List[ReferralItemResponse])
def referral_list(user_id: int = Depends(current_user_id)):
    return db.get_referral_list(user_id)


@router.get("/support/tickets", response_model=List[TicketResponse])
def tickets(user_id: int = Depends(current_user_id)):
    return db.get_user_tickets(user_id)


@router.post("/support/tickets", response_model=TicketResponse)
def create_ticket(payload: TicketCreateRequest, user_id: int = Depends(current_user_id)):
    profile = db.get_profile(user_id)
    username = profile.get("username") or profile.get("display_name") or str(user_id)
    return db.create_ticket(user_id, username, payload.text.strip(), payload.category.strip() or "Другое")


@router.get("/support/tickets/{ticket_id}/messages", response_model=List[TicketMessageResponse])
def ticket_messages(ticket_id: int, user_id: int = Depends(current_user_id)):
    messages = db.get_ticket_messages(user_id, ticket_id)
    if messages is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ticket not found")
    return messages


@router.post("/support/tickets/{ticket_id}/messages", response_model=TicketMessageResponse)
def add_ticket_message(ticket_id: int, payload: TicketMessageCreateRequest, user_id: int = Depends(current_user_id)):
    message = db.add_ticket_message(user_id, ticket_id, payload.text.strip())
    if not message:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ticket not found")
    return message
