import asyncio
import json
import os
from datetime import datetime


if __package__ in (None, ""):
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from Trading.candles.notifier import TelegramNotifier
    from Trading.candles.patterns import (
        Bearish_engulfing,
        Bearish_harami,
        Bullish_engulfing,
        Bullish_harami,
        Dark_cloud_cover,
        Evening_star,
        Hammer,
        Morning_star,
        Three_white_soldiers,
    )
    from Trading.candles.scanner import scan_all_timeframes
    from Trading.candles.state import StateManager
else:
    from .notifier import TelegramNotifier
    from .patterns import (
        Bearish_engulfing,
        Bearish_harami,
        Bullish_engulfing,
        Bullish_harami,
        Dark_cloud_cover,
        Evening_star,
        Hammer,
        Morning_star,
        Three_white_soldiers,
    )
    from .scanner import scan_all_timeframes
    from .state import StateManager


DEFAULT_TICKERS = ["SBER", "GAZP", "LKOH", "NVTK", "MGNT", "ROSN", "T", "IMOEX"]


def _patterns():
    return {
        Hammer(): True,
        Bullish_engulfing(): True,
        Bullish_harami(): True,
        Morning_star(): True,
        Dark_cloud_cover(): False,
        Three_white_soldiers(): True,
        Bearish_engulfing(): False,
        Bearish_harami(): False,
        Evening_star(): False,
    }


def _chat_map():
    return json.loads(os.environ.get("TRADING_TIMEFRAME_CHATS", "{}"))


async def main():
    token = "8715766790:AAFQd7LOY2qOqvxgTaMKz7rJbEuM9t5VrZc"
    main_chat_id = -1003851510557
    tickers = ["SBER", "GAZP", "LKOH", "NVTK", "MGNT", "ROSN", "T", "IMOEX"]
    tickers = tickers if tickers else DEFAULT_TICKERS

    notifier = TelegramNotifier(token, _chat_map(), main_chat_id)
    state_manager = StateManager()

    try:
        while True:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Начинаю плановое сканирование...")

            await scan_all_timeframes(tickers, _patterns(), notifier, state_manager)

            wait_time = 900
            print(f"Сканирование окончено. Сон {wait_time // 60} мин...")
            await asyncio.sleep(wait_time)

    except asyncio.CancelledError:
        print("Задача сканирования отменена.")
    except KeyboardInterrupt:
        print("Ручная остановка.")
    finally:
        await notifier.bot.session.close()
        print("Сессия бота закрыта. Бот полностью остановлен.")