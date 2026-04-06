"""
Timezone utility for IST (Indian Standard Time) across the application.
All timestamps use IST for consistency with the application's Indian deployment.
"""

from datetime import datetime, timezone, timedelta
import pytz
import os

# IST timezone definition
IST = pytz.timezone('Asia/Kolkata')

# UTC timezone for reference
UTC = pytz.UTC


def get_ist_now() -> datetime:
    """
    Returns current datetime in IST timezone.
    This replaces all datetime.now(timezone.utc) calls.
    """
    return datetime.now(IST)


def get_ist_timezone():
    """Returns the IST timezone object."""
    return IST


def get_app_timezone():
    """
    Returns the application's configured timezone.
    Can be overridden via APP_TIMEZONE env var.
    """
    tz_name = os.getenv('APP_TIMEZONE', 'Asia/Kolkata')
    try:
        return pytz.timezone(tz_name)
    except Exception:
        return IST


def utc_to_ist(dt: datetime) -> datetime:
    """
    Converts a UTC datetime to IST.
    If datetime is naive (no timezone info), assumes it's UTC.
    """
    if dt is None:
        return None
    
    # If naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    
    # Convert to IST
    return dt.astimezone(IST)


def ist_to_utc(dt: datetime) -> datetime:
    """
    Converts an IST datetime to UTC.
    If datetime is naive, assumes it's IST.
    """
    if dt is None:
        return None
    
    # If naive, assume IST
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    
    # Convert to UTC
    return dt.astimezone(UTC)


def format_ist(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Formats a datetime to IST string representation.
    Default format: "2024-01-01 12:30:45"
    """
    if dt is None:
        return None
    
    ist_dt = utc_to_ist(dt) if dt.tzinfo else IST.localize(dt)
    return ist_dt.strftime(fmt)


def parse_ist_string(date_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> datetime:
    """
    Parses a string as IST datetime.
    Returns a timezone-aware IST datetime object.
    """
    naive_dt = datetime.strptime(date_str, fmt)
    return IST.localize(naive_dt)
