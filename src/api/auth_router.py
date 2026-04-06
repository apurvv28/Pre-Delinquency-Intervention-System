import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])

COOKIE_NAME = "pie_session"
DEFAULT_FRONTEND_URL = "http://localhost:5173"
DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60


def _signing_secret() -> str:
    return os.getenv("AUTH_SESSION_SECRET", "dev-only-pie-session-secret")


def _session_ttl() -> int:
    try:
        return max(300, int(os.getenv("AUTH_SESSION_TTL_SECONDS", str(DEFAULT_SESSION_TTL_SECONDS))))
    except ValueError:
        return DEFAULT_SESSION_TTL_SECONDS


def _cookie_secure() -> bool:
    return os.getenv("AUTH_COOKIE_SECURE", "false").strip().lower() == "true"


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("utf-8")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def _create_signed_token(payload: dict) -> str:
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_part = _b64url_encode(payload_bytes)
    signature = hmac.new(_signing_secret().encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).digest()
    signature_part = _b64url_encode(signature)
    return f"{payload_part}.{signature_part}"


def _verify_signed_token(token: str) -> dict | None:
    try:
        payload_part, signature_part = token.split(".", 1)
    except ValueError:
        return None

    expected_signature = hmac.new(
        _signing_secret().encode("utf-8"),
        payload_part.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    actual_signature = _b64url_decode(signature_part)

    if not hmac.compare_digest(expected_signature, actual_signature):
        return None

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None

    exp = payload.get("exp")
    if not isinstance(exp, int) or exp <= int(time.time()):
        return None

    return payload


def _default_admin_profile() -> dict:
    return {
        "id": os.getenv("ADMIN_USER_ID", "admin-local-001"),
        "name": os.getenv("ADMIN_NAME", "PIE Bank Admin"),
        "email": os.getenv("ADMIN_EMAIL", "admin@piebank.internal"),
        "role": os.getenv("ADMIN_ROLE", "RISK_ADMIN"),
        "branch": os.getenv("ADMIN_BRANCH", "HQ"),
        "avatarUrl": os.getenv("ADMIN_AVATAR_URL", "https://api.dicebear.com/9.x/thumbs/svg?seed=PIE-Admin"),
    }


@router.get("/sign-in/google")
async def sign_in_google() -> Response:
    # Local/dev auth shim: creates an admin session and redirects to the frontend.
    now = int(time.time())
    ttl = _session_ttl()
    expires_at = now + ttl
    user = _default_admin_profile()

    token = _create_signed_token({
        "sub": user["id"],
        "exp": expires_at,
        "user": user,
    })

    frontend_url = os.getenv("FRONTEND_URL", DEFAULT_FRONTEND_URL)
    response = RedirectResponse(url=f"{frontend_url}/", status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        max_age=ttl,
        path="/",
    )
    return response


@router.get("/session")
async def get_session(request: Request) -> dict:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    payload = _verify_signed_token(token)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session")

    exp = payload["exp"]
    expires_at = datetime.fromtimestamp(exp, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "token": token,
        "expiresAt": expires_at,
        "user": payload["user"],
    }


@router.post("/sign-out")
async def sign_out(response: Response) -> dict:
    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"status": "ok"}
