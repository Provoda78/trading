from datetime import datetime

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import FSInputFile


class LevelsTelegrammNotifier:
    def __init__(self, token: str, timeframe_chats: dict, main_chat_id: int):
        session = AiohttpSession(proxy="http://127.0.0.1:12334")
        self.bot = Bot(token=token, session=session)
        self.timeframe_chats = timeframe_chats
        self.main_chat_id = main_chat_id

    async def send_signal_levels(
        self,
        ticker: str,
        photo_path: str,
        timeframe: str,
        supports: dict,
        resistances: dict,
    ):
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return

        support_scores = [str(value[0]) for value in supports.values()]
        support_prices = [str(price) for price in supports.keys()]
        resistance_scores = [str(value[0]) for value in resistances.values()]
        resistance_prices = [str(price) for price in resistances.keys()]

        score = ""
        if support_scores:
            score += (
                f"Цены: {support_prices} 🟢 Поддержка: "
                f"{', '.join(support_scores)} б.\n"
            )
        if resistance_scores:
            score += (
                f"Цены: {resistance_prices} 🔴 Сопротивление: "
                f"{', '.join(resistance_scores)} б.\n"
            )

        tv_url = f"https://tradingview.com/symbols/RUS-{ticker}/"
        caption = (
            f"🚨 <b>Обнаружены уровни!</b>\n\n"
            f"Акция: <b>{ticker}</b>\n"
            f"{score}\n"
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
            print(f"Уведомление по {ticker} отправлено в Telegram.")
        except Exception as error:
            print(f"Ошибка отправки в Telegram: {error}")

    async def send_breakout(
        self,
        ticker: str,
        timeframe: str,
        level_type: str,
        level_price: float,
        close_price: float,
        photo_path: str,
    ):
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return

        direction = "поддержки" if level_type == "support" else "сопротивления"
        tv_url = f"https://tradingview.com/symbols/RUS-{ticker}/"
        caption = (
            f"💥 <b>ПРОБОЙ {direction.upper()}</b>\n\n"
            f"Акция: <b>{ticker}</b>\n"
            f"Таймфрейм: {timeframe}\n"
            f"Уровень: {level_price:.4f}\n"
            f"Закрытие: {close_price:.4f}\n\n"
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
            print(f"Пробой уровня по {ticker} отправлен в Telegram.")
        except Exception as error:
            print(f"Ошибка отправки пробоя: {error}")
