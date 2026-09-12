"""Entry point for the level application."""

import asyncio
import json
import os
from datetime import datetime


from .notifier import LevelsTelegrammNotifier
from .scanner import scan_all_timeframes
from .state import LevelStateManager


DEFAULT_TICKERS = ["SBER", "GAZP", "LKOH", "NVTK", "MGNT", "ROSN", "T", "IMOEX"]


def _load_chat_map():
    raw = os.environ.get("TRADING_TIMEFRAME_CHATS", "{}")
    return json.loads(raw)


async def main():
    token = os.environ["TRADING_TELEGRAM_TOKEN"]
    chat_map = _load_chat_map()
    main_chat_id = int(os.environ["TRADING_MAIN_CHAT_ID"])
    tickers = os.environ.get("TRADING_TICKERS")
    tickers = tickers.split(",") if tickers else DEFAULT_TICKERS

    notifier = LevelsTelegrammNotifier(token, chat_map, main_chat_id)
    state_manager = LevelStateManager(sensitivity=0.003)

    try:
        while True:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"Начало сканирования уровней: [{current_time}]...")

            await scan_all_timeframes(tickers, notifier, state_manager)

            wait_time = 300
            print(f"Сканирование окончено. Сон {wait_time // 60} мин...")
            await asyncio.sleep(wait_time)

    except asyncio.CancelledError:
        print("Задача сканирования отменена.")
    except KeyboardInterrupt:
        print("Ручная остановка.")
    finally:
        await notifier.bot.session.close()
        print("Сессия бота закрыта. Бот полностью остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
