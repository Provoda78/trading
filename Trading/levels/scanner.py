"""Orchestration for the level scanner."""

import asyncio

from Trading.common.market_data import get_candles
from .detector import LevelDetector
from .state import LevelStateManager


async def scan_levels(tickers, timeframe, notifier, state_manager):
    detector = LevelDetector(10, 0.003)

    for ticker in tickers:
        df = await get_candles(ticker, interval=timeframe, limit=100)
        if df.empty:
            continue
        await state_manager.process(ticker, timeframe, df, detector, notifier)

    await asyncio.sleep(0.5)


async def scan_all_timeframes(tickers, notifier, state_manager):
    tasks = [
        scan_levels(tickers, timeframe, notifier, state_manager)
        for timeframe in notifier.timeframe_chats
    ]
    await asyncio.gather(*tasks)
