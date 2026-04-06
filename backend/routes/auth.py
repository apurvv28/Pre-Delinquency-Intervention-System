import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])

COOKIE_NAME = "pie_session"
OAUTH_STATE_COOKIE_NAME = "pie_oauth_state"
DEFAULT_FRONTEND_URL = "http://localhost:5173"
DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"


def _signing_secret() -> str:
    return os.getenv("AUTH_SESSION_SECRET", "dev-only-pie-session-secret")


def _session_ttl() -> int:
    try:
        return max(300, int(os.getenv("AUTH_SESSION_TTL_SECONDS", str(DEFAULT_SESSION_TTL_SECONDS))))
    except ValueError:
        return DEFAULT_SESSION_TTL_SECONDS


def _cookie_secure() -> bool:
    return os.getenv("AUTH_COOKIE_SECURE", "false").strip().lower() == "true"


def _frontend_url() -> str:
    return os.getenv("FRONTEND_URL", DEFAULT_FRONTEND_URL)


def _backend_url() -> str:
    return os.getenv("BACKEND_URL", "http://localhost:8000")


def _google_client_id() -> str:
    return os.getenv("GOOGLE_CLIENT_ID", "").strip()


def _google_client_secret() -> str:
    return os.getenv("GOOGLE_CLIENT_SECRET", "").strip()


def _google_redirect_uri() -> str:
    return os.getenv("GOOGLE_REDIRECT_URI", f"{_backend_url()}/api/auth/callback/google").strip()


def _google_configured() -> bool:
    return bool(_google_client_id() and _google_client_secret())


def _allowed_admin_emails() -> set[str]:
    raw = os.getenv("ADMIN_ALLOWED_EMAILS", "")
    return {email.strip().lower() for email in raw.split(",") if email.strip()}


def _allowed_admin_domains() -> set[str]:
    raw = os.getenv("ADMIN_ALLOWED_DOMAINS", "")
    return {domain.strip().lower() for domain in raw.split(",") if domain.strip()}


def _is_admin_email_allowed(email: str) -> bool:
    email_lc = email.strip().lower()
    allowed_emails = _allowed_admin_emails()
    if email_lc in allowed_emails:
        return True

    domain = email_lc.split("@")[-1] if "@" in email_lc else ""
    allowed_domains = _allowed_admin_domains()
    if allowed_domains and domain in allowed_domains:
        return True

    # If no allowlist/domain list configured, allow by default.
    return not allowed_emails and not allowed_domains


def _redirect_to_frontend_login_error(code: str) -> RedirectResponse:
    return RedirectResponse(url=f"{_frontend_url()}/login?authError={code}", status_code=status.HTTP_302_FOUND)


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


@router.get("/sign-in/google")
async def sign_in_google() -> Response:
    if not _google_configured():
        return _redirect_to_frontend_login_error("google_oauth_not_configured")

    state = secrets.token_urlsafe(24)
    params = {
        "client_id": _google_client_id(),
        "redirect_uri": _google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    auth_url = f"{GOOGLE_AUTH_URL}?{urlencode(params)}"
    response = RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key=OAUTH_STATE_COOKIE_NAME,
        value=state,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        max_age=600,
        path="/",
    )
    return response


@router.get("/callback/google")
async def google_callback(request: Request, code: str | None = None, state: str | None = None, error: str | None = None) -> Response:
    if error:
        return _redirect_to_frontend_login_error("oauth_denied")
    if not code or not state:
        return _redirect_to_frontend_login_error("oauth_missing_code")

    state_cookie = request.cookies.get(OAUTH_STATE_COOKIE_NAME)
    if not state_cookie or state_cookie != state:
        return _redirect_to_frontend_login_error("oauth_invalid_state")

    token_data = {
        "code": code,
        "client_id": _google_client_id(),
        "client_secret": _google_client_secret(),
        "redirect_uri": _google_redirect_uri(),
        "grant_type": "authorization_code",
    }

    try:
        async with httpx.AsyncClient() as client:
            token_response = await client.post(GOOGLE_TOKEN_URL, data=token_data, timeout=15.0)
            token_response.raise_for_status()
            token_payload = token_response.json()
            access_token = token_payload.get("access_token")
            expires_in = int(token_payload.get("expires_in", _session_ttl()))
            if not access_token:
                return _redirect_to_frontend_login_error("oauth_missing_access_token")

            userinfo_response = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=15.0,
            )
            userinfo_response.raise_for_status()
            userinfo = userinfo_response.json()
    except Exception:
        return _redirect_to_frontend_login_error("oauth_exchange_failed")

    email = str(userinfo.get("email", "")).strip().lower()
    if not email:
        return _redirect_to_frontend_login_error("oauth_missing_email")
    if not _is_admin_email_allowed(email):
        return _redirect_to_frontend_login_error("unauthorized_admin")

    now = int(time.time())
    ttl = max(300, min(_session_ttl(), expires_in))
    expires_at = now + ttl
    user = {
        "id": str(userinfo.get("sub", "admin-google")),
        "name": str(userinfo.get("name", email.split("@")[0])),
        "email": email,
        "role": os.getenv("ADMIN_ROLE", "RISK_ADMIN"),
        "branch": os.getenv("ADMIN_BRANCH", "HQ"),
        "avatarUrl": str(userinfo.get("picture", "https://api.dicebear.com/9.x/thumbs/svg?seed=PIE-Admin")),
    }

    token = _create_signed_token(
        {
            "sub": user["id"],
            "exp": expires_at,
            "user": user,
        }
    )

    response = RedirectResponse(url=f"{_frontend_url()}/", status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        max_age=ttl,
        path="/",
    )
    response.delete_cookie(key=OAUTH_STATE_COOKIE_NAME, path="/")
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