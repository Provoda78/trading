from abc import ABC, abstractmethod

import mplfinance as mpf
import numpy as np
import pandas as pd

from Trading.levels.detector import LevelDetector


class Confirmation(ABC):
    def __init__(self, weight):
        self.weight = weight

    @abstractmethod
    async def check(self, df, state) -> bool:
        pass

    @abstractmethod
    def draw(self, df, state, ticker, timeframe):
        pass


class MorrisConfirmation(Confirmation):
    """Confirmation by closing above or below the pattern candle."""

    async def check(self, df, state) -> bool:
        last_close = df["close"].iloc[-1]
        pattern_close = state["close"]
        return last_close > pattern_close if state["type"] else last_close < pattern_close

    def draw(self, df, state, ticker, timeframe):
        plot_df = df.iloc[-10:].copy()
        plot_df["datetime"] = pd.to_datetime(plot_df["datetime"])
        plot_df.set_index("datetime", inplace=True)
        plot_df["marker_up"] = np.nan
        plot_df.loc[plot_df.index[-1], "marker_up"] = state["close"] * 0.998

        addplot = mpf.make_addplot(
            plot_df["marker_up"],
            type="scatter",
            markersize=120,
            marker="^",
            color="blue",
        )
        file_name = f"Trading/Graf/Conf_{ticker}_{timeframe}_{state['pattern']}_chart.png"
        mpf.plot(
            plot_df,
            type="candle",
            style="charles",
            title=f"Confirmation of pattern: {state['pattern']}",
            addplot=addplot,
            savefig=file_name,
        )
        return file_name


class LevelConfirmation(Confirmation):
    """Confirmation by proximity to a support or resistance level."""

    def __init__(self, weight, detector=None):
        super().__init__(weight)
        self.detector = detector or LevelDetector(10, 0.003)

    async def check(self, df, state) -> bool:
        supports, resistances = self.detector.get_levels(df)
        needed = supports if state["type"] else resistances
        current_price = state["close"]

        for level_price, data in needed.items():
            margin = level_price * self.detector.sensitivity
            if level_price - margin <= current_price <= level_price + margin:
                self.weight = data[0]
                return True
        return False

    def draw(self, df, state, ticker, timeframe):
        levels = self.detector.get_levels(df)
        return self.detector.draw_levels(
            df,
            ticker,
            timeframe,
            levels,
            pattern_name=state["pattern"],
        )
