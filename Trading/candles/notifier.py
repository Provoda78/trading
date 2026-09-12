from datetime import datetime

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import FSInputFile


class TelegramNotifier:
    def __init__(self, token: str, timeframe_chats: dict, main_chat_id: int):
        session = AiohttpSession(proxy="http://127.0.0.1:12334")
        self.bot = Bot(token=token, session=session)
        self.timeframe_chats = timeframe_chats
        self.main_chat_id = main_chat_id

    async def send_signal(self, pattern_name, ticker, photo_path, timeframe):
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return

        tv_url = f"https://tradingview.com/symbols/RUS-{ticker}/"
        caption = (
            f"🚨 <b>{pattern_name}</b> [{timeframe}]\n\n"
            f"Акция: <b>{ticker}</b>\n"
            f"🕒 ТФ: {timeframe}\n"
            f"📅 {datetime.now().strftime('%d.%m %H:%M')}\n\n"
            f"🔗 <a href='{tv_url}'>Открыть график на TradingView</a>"
        )

        try:
            await self.bot.send_photo(
                chat_id=self.main_chat_id,
                message_thread_id=chat_id,
                photo=FSInputFile(photo_path),
                caption=caption,
                parse_mode="HTML",
            )
            print(
                f"Уведомление по {ticker} отправлено в Telegram. "
                f"id группы {chat_id}"
            )
        except Exception as error:
            print(f"Ошибка отправки в Telegram: {error}")

    async def send_confirmation_update(
        self, ticker, pattern, timeframe, score, confirmations, photo_path
    ):
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return

        tv_url = f"https://tradingview.com/symbols/RUS-{ticker}/"
        confirmation_list = "\n".join(f"✅ {item}" for item in confirmations)
        caption = (
            "⚡️ <b>ПОДТВЕРЖДЕНИЕ СИГНАЛА</b>\n\n"
            f"Акция: <b>{ticker}</b>\n"
            f"Паттерн: {pattern}\n"
            f"Таймфрейм: {timeframe}\n\n"
            f"📊 <b>Баллы: {score}</b>\n"
            "———————————————\n"
            f"{confirmation_list}\n"
            "———————————————\n\n"
            f"🔗 <a href='{tv_url}'>График TradingView</a>"
        )

        try:
            await self.bot.send_photo(
                chat_id=self.main_chat_id,
                message_thread_id=chat_id,
                photo=FSInputFile(photo_path),
                caption=caption,
                parse_mode="HTML",
            )
            print(f"Подтверждение по {ticker} отправлено в тему {chat_id}")
        except Exception as error:
            print(f"Ошибка отправки подтверждения: {error}")
