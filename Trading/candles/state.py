from datetime import datetime, timedelta

import pandas as pd

from .confirmations import LevelConfirmation, MorrisConfirmation


class StateManager:
    TIMEFRAME_SECONDS = {
        "1min": 60,
        "15min": 15 * 60,
        "1h": 60 * 60,
        "1D": 24 * 60 * 60,
    }

    def __init__(self):
        self.pending = {}
        self.criteria = {
            MorrisConfirmation(weight=1): "Подтверждение по Моррису",
            LevelConfirmation(weight=2): "Касание уровня S/R",
        }

    async def process_tick(self, ticker, timeframe, df, notifier):
        key = f"{ticker}_{timeframe}"
        if key not in self.pending:
            return

        state = self.pending[key]
        for confirmation, label in self.criteria.items():
            if label in state["confirmation"]:
                continue
            if await confirmation.check(df, state):
                state["score"] += confirmation.weight
                state["confirmation"].append(label)
                photo_path = confirmation.draw(df, state, ticker, timeframe)
                await notifier.send_confirmation_update(
                    ticker,
                    state["pattern"],
                    timeframe,
                    state["score"],
                    state["confirmation"],
                    photo_path,
                )

        expires_at = state["expires_at"]
        now = datetime.now(expires_at.tzinfo) if expires_at.tzinfo else datetime.now()
        if now >= expires_at:
            del self.pending[key]
            return

        last_close = df["close"].iloc[-1]
        invalidated = (
            state["type"] and last_close < state["close"] * 0.98
        ) or (
            not state["type"] and last_close > state["close"] * 1.02
        )
        if invalidated:
            print(f"Паттерн {state['pattern']} по {ticker} отменен рынком.")
            del self.pending[key]

    def add_pattern(
        self, ticker, timeframe, name, pattern_type: bool, data_close, pattern_time
    ):
        if timeframe not in self.TIMEFRAME_SECONDS:
            raise ValueError(f"Неизвестный таймфрейм: {timeframe}")

        candle_time = pd.to_datetime(pattern_time).to_pydatetime()
        self.pending[f"{ticker}_{timeframe}"] = {
            "pattern": name,
            "type": pattern_type,
            "close": data_close,
            "expires_at": candle_time
            + timedelta(seconds=2 * self.TIMEFRAME_SECONDS[timeframe]),
            "score": 0,
            "confirmation": [],
        }
