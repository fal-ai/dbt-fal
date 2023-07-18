from __future__ import annotations

from fal_serverless.auth import USER
from structlog.typing import EventDict, WrappedLogger


def add_user_id(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> EventDict:
    """The structlog processor that sends the logged user id on every log"""
    user_id: str | None = None
    try:
        user_id = USER.info.get("sub")
    except:
        # logs are fail-safe, so any exception is safe to ignore
        # this is expected to happen only when user is logged out
        # or there's no internet connection
        pass
    event_dict["user.id"] = user_id
    return event_dict
