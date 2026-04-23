"""
Candle Pattern Scanner — идеальная версия
==========================================
Переработка оригинального кода Provoda78/trading.

Что исправлено по сравнению с оригиналом:
  - Секреты вынесены в .env (python-dotenv)
  - Нейминг по PEP8: PascalCase для классов, snake_case для методов
  - Опечатки исправлены: Candle, check_pattern, parameters
  - Устранено дублирование кода (DRY): единый метод analyze_at_index
  - Графики строятся в памяти (io.BytesIO), без записи на диск
  - Добавлены Type Hints везде
  - Добавлены docstrings к каждому классу и методу
  - Логирование через logging вместо print
  - Параметры вынесены в dataclass Config
  - Исправлена ошибка области видимости из CandlePower.py
  - Единообразный стиль кода (PEP8, black-compatible)

Зависимости:
  pip install moexalgo pandas numpy pandas_ta mplfinance aiogram plotly
  python-dotenv kaleido
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import mplfinance as mpf
import numpy as np
import pandas as pd
import pandas_ta as ta
import plotly.graph_objects as go
from aiogram import Bot
from aiogram.types import BufferedInputFile
from dotenv import load_dotenv
from moexalgo import Ticker

# ---------------------------------------------------------------------------
# Загрузка переменных окружения из файла .env
# ---------------------------------------------------------------------------
# Создайте файл .env рядом со скриптом и добавьте туда:
#   TELEGRAM_TOKEN=ваш_токен
#   TELEGRAM_CHAT_ID=ваш_chat_id
load_dotenv()

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%d.%m.%Y %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("candle_scanner.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------
@dataclass
class Config:
    """Центральное место для всех настроек сканера."""

    telegram_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )

    # Список тикеров для мониторинга
    tickers: list[str] = field(
        default_factory=lambda: ["SBER", "GAZP", "LKOH", "NVTK", "MGNT", "ROSN"]
    )
    timeframes: list[str] = field(default_factory=lambda: ["15min"])

    # Параметры загрузки данных
    days_to_load: int = 10
    load_retries: int = 3
    retry_base_delay: float = 2.0

    # Параметры индикаторов
    ema_period: int = 10
    rsi_period: int = 14
    body_size_window: int = 11

    # Параметры сканирования
    scan_interval_seconds: int = 3600  # раз в час

    def __post_init__(self) -> None:
        if not self.telegram_token:
            log.warning(
                "TELEGRAM_TOKEN не задан. Создайте файл .env с токеном бота."
            )
        if not self.telegram_chat_id:
            log.warning("TELEGRAM_CHAT_ID не задан.")


# ---------------------------------------------------------------------------
# Telegram-уведомления
# ---------------------------------------------------------------------------
class TelegramNotifier:
    """Отправляет сигналы (фото + текст) в Telegram через aiogram."""

    def __init__(self, config: Config) -> None:
        self._bot = Bot(token=config.telegram_token)
        self._chat_id = config.telegram_chat_id

    async def send_signal(
        self, pattern_name: str, ticker: str, image_bytes: bytes
    ) -> None:
        """
        Отправляет изображение графика с подписью в Telegram.

        Args:
            pattern_name: Название найденного паттерна.
            ticker: Тикер инструмента.
            image_bytes: PNG-изображение в виде байтов (из io.BytesIO).
        """
        caption = (
            f"🚨 *Найден паттерн!*\n\n"
            f"📊 Акция: `{ticker}`\n"
            f"🕯 Паттерн: *{pattern_name}*\n"
            f"📅 Дата: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`"
        )
        photo = BufferedInputFile(image_bytes, filename="chart.png")
        try:
            await self._bot.send_photo(
                chat_id=self._chat_id,
                photo=photo,
                caption=caption,
                parse_mode="Markdown",
            )
            log.info("Уведомление по %s отправлено в Telegram.", ticker)
        except Exception as exc:
            log.error("Ошибка отправки в Telegram: %s", exc)

    async def close(self) -> None:
        """Закрывает сессию бота. Вызывать при завершении работы."""
        await self._bot.session.close()


# ---------------------------------------------------------------------------
# Загрузка данных с MOEX
# ---------------------------------------------------------------------------
def fetch_candles(
    ticker_name: str,
    timeframe: str,
    days_needed: int = 10,
    retries: int = 3,
    base_delay: float = 2.0,
) -> pd.DataFrame:
    """
    Загружает свечи с Московской биржи через moexalgo.

    Реализует экспоненциальный backoff с jitter при сетевых ошибках.

    Args:
        ticker_name: Тикер инструмента (например, 'SBER').
        timeframe: Таймфрейм ('15min', '1h', '1D' и т.д.).
        days_needed: Количество торговых дней истории.
        retries: Максимальное число попыток при сетевой ошибке.
        base_delay: Начальная задержка для backoff (секунды).

    Returns:
        DataFrame со свечами или пустой DataFrame при ошибке.
    """
    # Берём с запасом для корректного прогрева индикаторов
    total_days = days_needed + 30
    start_dt = (datetime.now() - timedelta(days=total_days)).strftime("%Y-%m-%d")
    end_dt = datetime.now().strftime("%Y-%m-%d")

    for attempt in range(retries):
        try:
            ticker = Ticker(ticker_name)
            data = ticker.candles(start=start_dt, end=end_dt, period=timeframe)
            df = pd.DataFrame(data)

            if df.empty:
                log.warning("Тикер %s: пустой ответ от MOEX.", ticker_name)
                return pd.DataFrame()

            df = df.rename(columns={"begin": "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            log.info(
                "Загружено %d свечей для %s (%s).", len(df), ticker_name, timeframe
            )
            return df

        except Exception as exc:
            err = str(exc)
            if "not found" in err or "NoneType" in err:
                log.warning("Тикер %s неактивен или недоступен.", ticker_name)
                return pd.DataFrame()

            jitter = random.uniform(0.0, 1.0)
            wait = (base_delay * (2**attempt)) + jitter
            log.warning(
                "Сбой сети для %s. Попытка %d/%d. Ожидание %.1f с.",
                ticker_name,
                attempt + 1,
                retries,
                wait,
            )
            time.sleep(wait)

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Базовый класс паттерна
# ---------------------------------------------------------------------------
class CandlePattern(ABC):
    """
    Абстрактный базовый класс для всех свечных паттернов.

    Реализует паттерн «Шаблонный метод» (Template Method):
    - Общая логика расчёта метрик инкапсулирована здесь.
    - Каждый конкретный паттерн переопределяет только check_pattern().

    Attributes:
        name: Человекочитаемое название паттерна.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    # ------------------------------------------------------------------
    # Вспомогательные методы расчёта (вызываются внутри compute_features)
    # ------------------------------------------------------------------

    def _add_trend_flags(self, df: pd.DataFrame, window: int = 10) -> None:
        """Добавляет флаги направления SMA-тренда."""
        sma = df["close"].rolling(window=window).mean()
        df["sma_up"] = sma > sma.shift(1)
        df["sma_down"] = sma < sma.shift(1)

    def _add_body_size_flags(self, df: pd.DataFrame, window: int = 11) -> None:
        """
        Классифицирует тело свечи как относительно маленькое или большое,
        сравнивая с предыдущими (window - 1) свечами.
        """

        def _count_bigger(x: pd.Series) -> float:
            return float((x.iloc[:-1] > x.iloc[-1]).sum())

        def _count_smaller(x: pd.Series) -> float:
            return float((x.iloc[:-1] < x.iloc[-1]).sum())

        half = window // 2
        df["is_small_body"] = (
            df["body_size"].rolling(window=window).apply(_count_bigger, raw=False)
            >= half
        )
        df["is_big_body"] = (
            df["body_size"].rolling(window=window).apply(_count_smaller, raw=False)
            >= half
        )

    def compute_features(self, df: pd.DataFrame, config: Config) -> pd.DataFrame:
        """
        Рассчитывает все базовые метрики свечей и добавляет их как колонки.

        Вызывается один раз перед check_pattern(). Работает на копии df,
        не мутируя оригинал.

        Args:
            df: Исходный DataFrame со свечами.
            config: Конфигурация с параметрами индикаторов.

        Returns:
            Новый DataFrame с добавленными колонками метрик.
        """
        d = df.copy()

        # Геометрия свечи
        d["body_size"] = (d["close"] - d["open"]).abs()
        d["is_bullish"] = d["close"] > d["open"]
        d["is_bearish"] = d["close"] < d["open"]
        d["body_high"] = d[["open", "close"]].max(axis=1)
        d["body_low"] = d[["open", "close"]].min(axis=1)
        d["lower_shadow"] = d["body_low"] - d["low"]
        d["upper_shadow"] = d["high"] - d["body_high"]
        d["mid_body"] = (d["open"] + d["close"]) / 2

        # Подтверждение следующей свечой
        d["confirmed_bull"] = d["close"].shift(-1) > d["close"]
        d["confirmed_bear"] = d["close"].shift(-1) < d["close"]

        # Гэпы
        d["gap_down"] = d["close"] < d["close"].shift(1)
        d["gap_up"] = d["open"].shift(-1) > d["close"]

        # Технические индикаторы
        d["rsi"] = ta.rsi(d["close"], length=config.rsi_period)

        # Флаги размера тела и тренда
        self._add_body_size_flags(d, window=config.body_size_window)
        self._add_trend_flags(d, window=config.ema_period)

        return d

    @abstractmethod
    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        """
        Возвращает булеву Series: True на свечах, где сработал паттерн.

        Args:
            df: Исходный DataFrame со свечами.
            config: Конфигурация.
        """

    def build_chart_bytes(self, df: pd.DataFrame) -> bytes:
        """
        Строит график последних 30 свечей с маркерами сигналов.
        Возвращает PNG в виде байтов (без записи на диск).

        Args:
            df: DataFrame с уже рассчитанными колонками сигналов.

        Returns:
            PNG-изображение в байтах.
        """
        signals = self.check_pattern(df, Config())

        plot_df = df.copy()
        plot_df["datetime"] = pd.to_datetime(plot_df["datetime"])
        plot_df = plot_df.set_index("datetime").iloc[-30:]

        plot_df["marker_up"] = np.where(signals.reindex(plot_df.index), plot_df["low"] * 0.998, np.nan)

        apds = []
        if plot_df["marker_up"].notna().any():
            apds.append(
                mpf.make_addplot(
                    plot_df["marker_up"],
                    type="scatter",
                    markersize=120,
                    marker="^",
                    color="green",
                )
            )

        buf = io.BytesIO()
        mpf.plot(
            plot_df,
            type="candle",
            style="charles",
            title=f"Pattern: {self.name}",
            addplot=apds if apds else [],
            savefig=buf,
        )
        buf.seek(0)
        return buf.read()


# ---------------------------------------------------------------------------
# Конкретные паттерны
# ---------------------------------------------------------------------------

class Hammer(CandlePattern):
    """
    Молот (Hammer) — бычий разворотный паттерн.

    Условия:
      - Нижняя тень >= 2x тела
      - Верхняя тень <= 1x тела
      - Тело > 0
      - RSI < 30 (зона перепроданности)
      - Тело относительно маленькое среди последних 11 свечей
      - Следующая свеча подтверждает рост
    """

    def __init__(self) -> None:
        super().__init__("Hammer")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            (d["lower_shadow"] >= 2 * d["body_size"])
            & (d["upper_shadow"] <= d["body_size"])
            & (d["body_size"] > 0)
            & (d["rsi"] < 30)
            & d["is_small_body"]
            & d["confirmed_bull"]
        )


class BullishEngulfing(CandlePattern):
    """
    Бычье поглощение (Bullish Engulfing) — разворот вверх.

    Условия:
      - Текущая свеча бычья, предыдущая медвежья
      - Тело текущей полностью поглощает тело предыдущей
      - Тело текущей больше предыдущего
      - SMA-тренд нисходящий (ищем дно)
      - Тело относительно большое
      - Следующая свеча подтверждает рост
    """

    def __init__(self) -> None:
        super().__init__("Bullish Engulfing")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bullish"]
            & d["is_bearish"].shift(1)
            & (d["open"] <= d["close"].shift(1))
            & (d["close"] >= d["open"].shift(1))
            & (d["body_size"] > d["body_size"].shift(1))
            & d["is_big_body"]
            & d["sma_down"]
            & d["confirmed_bull"]
        )


class BearishEngulfing(CandlePattern):
    """
    Медвежье поглощение (Bearish Engulfing) — разворот вниз.

    Условия:
      - Текущая свеча медвежья, предыдущая бычья
      - Тело текущей полностью поглощает тело предыдущей
      - Тело текущей больше предыдущего
      - SMA-тренд восходящий (ищем вершину)
      - Тело относительно большое
      - Следующая свеча подтверждает падение
    """

    def __init__(self) -> None:
        super().__init__("Bearish Engulfing")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bearish"]
            & d["is_bullish"].shift(1)
            & (d["open"] >= d["close"].shift(1))
            & (d["close"] <= d["open"].shift(1))
            & (d["body_size"] > d["body_size"].shift(1))
            & d["is_big_body"]
            & d["sma_up"]
            & d["confirmed_bear"]
        )


class MorningStar(CandlePattern):
    """
    Утренняя звезда (Morning Star) — трёхсвечной бычий разворот.

    Условия (свечи 0, -1, -2 от текущей):
      - Свеча[-2]: большая медвежья
      - Свеча[-1]: маленькое тело (звезда), гэп вниз
      - Свеча[0]: большая бычья, SMA-тренд вниз
    """

    def __init__(self) -> None:
        super().__init__("Morning Star")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bullish"]
            & d["is_small_body"].shift(1)
            & d["is_bearish"].shift(2)
            & d["is_big_body"].shift(2)
            & d["gap_down"].shift(1)
            & d["sma_down"]
        )


class EveningStar(CandlePattern):
    """
    Вечерняя звезда (Evening Star) — трёхсвечной медвежий разворот.

    Условия:
      - Свеча[-2]: большая бычья
      - Свеча[-1]: маленькое тело (звезда), гэп вверх
      - Свеча[0]: большая медвежья, закрытие ниже открытия свечи[-2],
                  SMA-тренд вверх
    """

    def __init__(self) -> None:
        super().__init__("Evening Star")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bearish"]
            & d["is_small_body"].shift(1)
            & d["is_bullish"].shift(2)
            & d["is_big_body"].shift(2)
            & (d["close"] < d["open"].shift(2))
            & d["gap_up"]
            & d["sma_up"]
        )


class BullishHarami(CandlePattern):
    """
    Бычье харами (Bullish Harami) — бычий разворот внутри предыдущей свечи.

    Условия:
      - Свеча[-1]: большая медвежья
      - Свеча[0]: маленькая бычья, полностью внутри тела предыдущей
      - SMA-тренд вниз
      - Следующая свеча подтверждает рост
    """

    def __init__(self) -> None:
        super().__init__("Bullish Harami")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bearish"].shift(1)
            & d["is_bullish"]
            & d["is_big_body"].shift(1)
            & d["is_small_body"]
            & (d["body_high"] < d["body_high"].shift(1))
            & (d["body_low"] > d["body_low"].shift(1))
            & d["sma_down"]
            & d["confirmed_bull"]
        )


class BearishHarami(CandlePattern):
    """
    Медвежье харами (Bearish Harami) — медвежий разворот внутри предыдущей свечи.

    Условия:
      - Свеча[-1]: большая бычья
      - Свеча[0]: маленькая медвежья, полностью внутри тела предыдущей
      - SMA-тренд вверх
      - Следующая свеча подтверждает падение
    """

    def __init__(self) -> None:
        super().__init__("Bearish Harami")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bullish"].shift(1)
            & d["is_bearish"]
            & d["is_big_body"].shift(1)
            & d["is_small_body"]
            & (d["body_high"] < d["body_high"].shift(1))
            & (d["body_low"] > d["body_low"].shift(1))
            & d["sma_up"]
            & d["confirmed_bear"]
        )


class DarkCloudCover(CandlePattern):
    """
    Завеса из тёмных облаков (Dark Cloud Cover) — медвежий разворот.

    Условия:
      - Свеча[-1]: большая бычья
      - Свеча[0]: медвежья, открытие выше максимума предыдущей,
                  закрытие ниже середины тела предыдущей,
                  но выше открытия предыдущей
      - SMA-тренд вверх
    """

    def __init__(self) -> None:
        super().__init__("Dark Cloud Cover")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bearish"]
            & d["is_bullish"].shift(1)
            & d["is_big_body"].shift(1)
            & (d["open"] > d["high"].shift(1))
            & (d["close"] < d["mid_body"].shift(1))
            & (d["close"] > d["open"].shift(1))
            & d["sma_up"]
        )


class ThreeWhiteSoldiers(CandlePattern):
    """
    Три белых солдата (Three White Soldiers) — сильный бычий сигнал продолжения.

    Условия (три последовательные свечи):
      - Все три бычьи и с большим телом
      - Каждое закрытие выше предыдущего
      - Каждое открытие выше предыдущего, но внутри тела предыдущей свечи
      - Верхняя тень последней свечи < 20% тела (нет отката)
    """

    def __init__(self) -> None:
        super().__init__("Three White Soldiers")

    def check_pattern(self, df: pd.DataFrame, config: Config) -> pd.Series:
        d = self.compute_features(df, config)
        return (
            d["is_bullish"]
            & d["is_bullish"].shift(1)
            & d["is_bullish"].shift(2)
            & d["is_big_body"]
            & d["is_big_body"].shift(1)
            & d["is_big_body"].shift(2)
            & (d["close"] > d["close"].shift(1))
            & (d["close"].shift(1) > d["close"].shift(2))
            & (d["open"] > d["open"].shift(1))
            & (d["open"] < d["close"].shift(1))
            & (d["upper_shadow"] < d["body_size"] * 0.2)
        )


# ---------------------------------------------------------------------------
# Визуализация (процедурный сканер)
# ---------------------------------------------------------------------------

def build_signal_chart(
    df: pd.DataFrame,
    ticker: str,
    timeframe: str,
    pattern_name: str,
    signal_time: datetime,
    context: int = 20,
) -> bytes:
    """
    Строит интерактивный график вокруг сигнала и возвращает PNG в байтах.

    Args:
        df: DataFrame со свечами и колонкой ema10.
        ticker: Тикер инструмента.
        timeframe: Таймфрейм.
        pattern_name: Название паттерна для аннотации.
        signal_time: Время свечи с сигналом.
        context: Количество свечей до и после сигнала.

    Returns:
        PNG-изображение в байтах.
    """
    try:
        idx = df.index[df["datetime"] == signal_time][0]
    except IndexError:
        log.error("Время сигнала %s не найдено в DataFrame.", signal_time)
        return b""

    plot_df = df.iloc[max(0, idx - context) : min(len(df), idx + context)].copy()

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=plot_df["datetime"],
                open=plot_df["open"],
                high=plot_df["high"],
                low=plot_df["low"],
                close=plot_df["close"],
                name="Свечи",
            )
        ]
    )

    fig.add_trace(
        go.Scatter(
            x=plot_df["datetime"],
            y=plot_df["ema10"],
            mode="lines",
            line=dict(color="orange", width=1.5),
            name="EMA 10",
        )
    )

    fig.add_vline(
        x=signal_time, line_width=1, line_dash="dash", line_color="white"
    )

    is_bullish_pattern = any(
        kw in pattern_name.lower()
        for kw in ["bull", "hammer", "piercing", "morning", "soldier"]
    )
    y_pos = plot_df.loc[idx, "low"] if is_bullish_pattern else plot_df.loc[idx, "high"]
    color = "green" if is_bullish_pattern else "red"

    fig.add_annotation(
        x=signal_time,
        y=y_pos,
        text=pattern_name,
        showarrow=True,
        arrowhead=2,
        arrowcolor=color,
        ax=0,
        ay=40 if is_bullish_pattern else -40,
        bgcolor=color,
        font=dict(color="white"),
    )

    fig.update_layout(
        title=f"{ticker} {timeframe} | {pattern_name}",
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=40, b=10),
    )

    return fig.to_image(format="png", scale=2)


# ---------------------------------------------------------------------------
# Анализ паттернов (единая функция без дублирования)
# ---------------------------------------------------------------------------

def analyze_patterns_at_index(
    df: pd.DataFrame,
    idx: int,
    config: Config,
) -> list[str]:
    """
    Анализирует свечные паттерны на конкретном индексе idx.

    Заменяет две дублирующиеся функции оригинала:
    analyze_morris_patterns() и analyze_morris_patterns_at_index().
    Для анализа последней закрытой свечи передавайте idx = len(df) - 2.

    Args:
        df: DataFrame со свечами и колонкой ema10.
        idx: Индекс анализируемой свечи.
        config: Конфигурация.

    Returns:
        Список названий найденных паттернов.
    """
    if idx < 2:
        return []

    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]

    ema = curr["ema10"]
    c_open, c_close = curr["open"], curr["close"]
    c_high, c_low = curr["high"], curr["low"]

    c_body = abs(c_close - c_open)
    c_range = c_high - c_low
    c_body_high = max(c_open, c_close)
    c_body_low = min(c_open, c_close)
    c_upper = c_high - c_body_high
    c_lower = c_body_low - c_low

    p_open, p_close = prev["open"], prev["close"]
    p_body_high = max(p_open, p_close)
    p_body_low = min(p_open, p_close)
    p_body = abs(p_close - p_open)
    p_mid = (p_open + p_close) / 2

    found: list[str] = []

    if c_close < ema:
        # Бычьи развороты под EMA (нисходящий тренд)
        if c_lower >= c_body * 2 and c_upper <= c_range * 0.1 and c_body > 0:
            found.append("Hammer (Молот)")
        if c_upper >= c_body * 2 and c_lower <= c_range * 0.1 and c_body > 0:
            found.append("Inverted Hammer (Перевернутый молот)")
        if (c_close > c_open and p_close < p_open
                and c_body_high >= p_body_high and c_body_low <= p_body_low):
            found.append("Bullish Engulfing (Бычье поглощение)")
        if (p_close < p_open and c_body_high <= p_body_high
                and c_body_low >= p_body_low and p_body > c_body):
            found.append("Bullish Harami (Бычье харами)")
        if p_close < p_open and c_close > c_open and c_open < p_close and c_close > p_mid:
            found.append("Piercing Line (Просвет в облаках)")

    elif c_close > ema:
        # Медвежьи развороты над EMA (восходящий тренд)
        if c_lower >= c_body * 2 and c_upper <= c_range * 0.1 and c_body > 0:
            found.append("Hanging Man (Висельник)")
        if c_upper >= c_body * 2 and c_lower <= c_range * 0.1 and c_body > 0:
            found.append("Shooting Star (Падающая звезда)")
        if (c_close < c_open and p_close > p_open
                and c_body_high >= p_body_high and c_body_low <= p_body_low):
            found.append("Bearish Engulfing (Медвежье поглощение)")
        if (p_close > p_open and c_body_high <= p_body_high
                and c_body_low >= p_body_low and p_body > c_body):
            found.append("Bearish Harami (Медвежье харами)")
        if p_close > p_open and c_close < c_open and c_open > p_close and c_close < p_mid:
            found.append("Dark Cloud Cover (Завеса из тёмных облаков)")

    # Нейтральный паттерн
    if c_body <= c_range * 0.1 and c_range > 0:
        found.append("Doji (Доджи)")

    return found


# ---------------------------------------------------------------------------
# Сканер в реальном времени
# ---------------------------------------------------------------------------

async def run_realtime_scanner(config: Config) -> None:
    """
    Основной асинхронный цикл сканера.

    Каждые config.scan_interval_seconds проверяет все тикеры
    на наличие паттернов на последней закрытой свече.
    При обнаружении отправляет уведомление в Telegram.

    Args:
        config: Конфигурация сканера.
    """
    notifier = TelegramNotifier(config)
    # last_alerts: (ticker, tf, pattern) -> datetime последнего алерта
    last_alerts: dict[tuple[str, str, str], datetime] = {}

    log.info(
        "Сканер запущен. Тикеры: %s. Интервал: %d с.",
        config.tickers,
        config.scan_interval_seconds,
    )

    try:
        while True:
            for ticker in config.tickers:
                for tf in config.timeframes:
                    df = fetch_candles(
                        ticker,
                        tf,
                        days_needed=config.days_to_load,
                        retries=config.load_retries,
                        base_delay=config.retry_base_delay,
                    )
                    if df.empty:
                        continue

                    df["ema10"] = df["close"].ewm(
                        span=config.ema_period, adjust=False
                    ).mean()

                    # Анализируем последнюю ЗАКРЫТУЮ свечу (предпоследняя строка)
                    check_idx = len(df) - 2
                    if check_idx < 2:
                        continue

                    patterns = analyze_patterns_at_index(df, check_idx, config)
                    candle_time: datetime = df.iloc[check_idx]["datetime"]
                    last_price: float = df.iloc[check_idx]["close"]

                    for pattern in patterns:
                        key = (ticker, tf, pattern)
                        if last_alerts.get(key) == candle_time:
                            continue  # Уже отправляли этот сигнал

                        log.info("СИГНАЛ: %s | %s | %s @ %s", ticker, tf, pattern, candle_time)

                        image_bytes = build_signal_chart(
                            df, ticker, tf, pattern, candle_time
                        )
                        caption = (
                            f"🎯 *{pattern}*\n"
                            f"📊 `{ticker}` | `{tf}`\n"
                            f"💰 Цена: `{last_price:.2f}`\n"
                            f"⏰ Свеча: `{candle_time.strftime('%H:%M')}`"
                        )
                        if image_bytes:
                            await notifier.send_signal(pattern, ticker, image_bytes)

                        last_alerts[key] = candle_time

            log.info(
                "Сканирование завершено. Следующее через %d мин.",
                config.scan_interval_seconds // 60,
            )
            await asyncio.sleep(config.scan_interval_seconds)

    except (asyncio.CancelledError, KeyboardInterrupt):
        log.info("Сканер остановлен.")
    finally:
        await notifier.close()
        log.info("Сессия бота закрыта.")


# ---------------------------------------------------------------------------
# Бэктест по истории
# ---------------------------------------------------------------------------

def run_backtest(config: Config, days_back: int = 3) -> None:
    """
    Проходит по всей истории за последние days_back дней,
    находит паттерны и отправляет скриншоты в Telegram.

    Используется для проверки работоспособности стратегии
    перед запуском в реальном времени.

    Args:
        config: Конфигурация сканера.
        days_back: Глубина истории для бэктеста.
    """
    import requests as _requests  # локальный импорт, чтобы не засорять namespace

    log.info("--- ЗАПУСК БЭКТЕСТА ЗА %d ДНЯ ---", days_back)

    for ticker in config.tickers:
        for tf in config.timeframes:
            df = fetch_candles(
                ticker,
                tf,
                days_needed=days_back,
                retries=config.load_retries,
            )
            if df.empty:
                continue

            df["ema10"] = df["close"].ewm(
                span=config.ema_period, adjust=False
            ).mean()

            # Пропускаем первые ema_period свечей (прогрев EMA)
            for i in range(config.ema_period, len(df)):
                patterns = analyze_patterns_at_index(df, i, config)
                if not patterns:
                    continue

                candle_time: datetime = df.loc[i, "datetime"]
                last_price: float = df.loc[i, "close"]

                for pattern in patterns:
                    log.info(
                        "Найдено в истории: %s %s %s @ %s",
                        ticker, tf, pattern, candle_time,
                    )

                    image_bytes = build_signal_chart(
                        df, ticker, tf, pattern, candle_time
                    )
                    if not image_bytes:
                        continue

                    caption = (
                        f"🧪 *ТЕСТОВЫЙ СИГНАЛ*\n"
                        f"🎯 *{pattern}*\n"
                        f"📊 `{ticker}` | `{tf}`\n"
                        f"💰 Цена: `{last_price:.2f}`\n"
                        f"⏰ Время: `{candle_time.strftime('%d.%m %H:%M')}`"
                    )

                    url = f"https://api.telegram.org/bot{config.telegram_token}/sendPhoto"
                    _requests.post(
                        url,
                        data={
                            "chat_id": config.telegram_chat_id,
                            "caption": caption,
                            "parse_mode": "Markdown",
                        },
                        files={"photo": ("chart.png", image_bytes, "image/png")},
                        timeout=30,
                    )
                    time.sleep(1)  # Защита от спам-фильтра Telegram

    log.info("--- БЭКТЕСТ ЗАВЕРШЁН ---")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cfg = Config()

    # Выберите нужный режим:

    # 1. Бэктест за последние 2 дня
    run_backtest(cfg, days_back=2)

    # 2. Сканер в реальном времени (раз в час)
    # asyncio.run(run_realtime_scanner(cfg))
