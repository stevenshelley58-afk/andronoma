"""Simple token-based authentication utilities."""
from __future__ import annotations

import uuid
from typing import Optional

from passlib.context import CryptContext
from sqlalchemy.orm import Session

from .models import SessionToken, User

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)


def create_session_token(session: Session, user: User) -> SessionToken:
    token = SessionToken(id=uuid.uuid4(), user=user, token=str(uuid.uuid4()))
    session.add(token)
    session.commit()
    session.refresh(token)
    return token


def get_user_by_token(session: Session, token: str) -> Optional[User]:
    return (
        session.query(User)
        .join(SessionToken, SessionToken.user_id == User.id)
        .filter(SessionToken.token == token)
        .one_or_none()
    )
