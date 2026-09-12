"""Runtime configuration loaded from environment variables."""

import os


def telegram_token() -> str:
    token = os.environ.get("TRADING_TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("TRADING_TELEGRAM_TOKEN is not set")
    return token


def telegram_chat_id() -> int:
    value = os.environ.get("TRADING_TELEGRAM_CHAT_ID")
    if not value:
        raise RuntimeError("TRADING_TELEGRAM_CHAT_ID is not set")
    return int(value)
