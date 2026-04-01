from datetime import datetime, timezone, timedelta
from typing import Union, Optional


def to_utc_datetime(dt: Union[str, datetime, None], end_of_day: bool = False) -> Optional[datetime]:
    """Convert string or naive datetime to UTC-aware datetime.

    Args:
        dt: ISO-format string, datetime object, or None.

    Returns:
        UTC-aware datetime, or None if input is None.
    """
    if dt is None or dt == "":
        return None

    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    if end_of_day:
        # Shift to start of the next day so filtering can use '< to_date'
        dt = dt + timedelta(days=1)

    return dt
