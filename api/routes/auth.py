from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from shared.db import get_sync_session
from shared.models import User
from shared.security import create_session_token, hash_password, verify_password

from ..dependencies import get_current_user
from ..schemas.auth import LoginRequest, LoginResponse, UserResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse)
def register_user(payload: LoginRequest) -> UserResponse:
    with get_sync_session() as session:
        existing = session.query(User).filter_by(email=payload.email).one_or_none()
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")
        user = User(id=uuid.uuid4(), email=payload.email, password_hash=hash_password(payload.password))
        session.add(user)
        session.commit()
        session.refresh(user)
        return UserResponse(id=str(user.id), email=user.email)


@router.post("/login", response_model=LoginResponse)
def login_user(payload: LoginRequest) -> LoginResponse:
    with get_sync_session() as session:
        user = session.query(User).filter_by(email=payload.email).one_or_none()
        if not user or not verify_password(payload.password, user.password_hash):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
        token = create_session_token(session, user)
        return LoginResponse(access_token=token.token)


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)) -> UserResponse:
    return UserResponse(id=str(current_user.id), email=current_user.email)
