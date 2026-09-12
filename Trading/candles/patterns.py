from abc import ABC, abstractmethod

import mplfinance as mpf
import numpy as np
import pandas as pd
import pandas_ta as ta


class candel(ABC):
    name: str

    def __init__(self, name):
        self.name = name

    def trend(self, df, window=10):
        df["sma_trend"] = df["close"].rolling(window=window).mean()
        df["sma_up"] = df["sma_trend"] > df["sma_trend"].shift(1)
        df["sma_down"] = df["sma_trend"] < df["sma_trend"].shift(1)
        df.drop(columns=["sma_trend"], inplace=True)
        return df

    def size_bodies(self, df: pd.DataFrame):
        def count_bigger(values):
            current_body = values.iloc[-1]
            return (values.iloc[:-1] > current_body).sum()

        def count_smaller(values):
            current_body = values.iloc[-1]
            return (values.iloc[:-1] < current_body).sum()

        df["is_small_body"] = (
            df["body_size"].rolling(window=11).apply(count_bigger, raw=False) >= 6
        )
        df["is_big_body"] = (
            df["body_size"].rolling(window=11).apply(count_smaller, raw=False) >= 6
        )

    def parametrs(self, df: pd.DataFrame):
        df["body_size"] = abs(df["close"] - df["open"])
        df["is_bullish"] = df["close"] > df["open"]
        df["is_bearish"] = df["close"] < df["open"]
        df["lower_shadow"] = df[["open", "close"]].min(axis=1) - df["low"]
        df["upper_shadow"] = df["high"] - df[["open", "close"]].max(axis=1)
        df["mid_body"] = (df["open"] + df["close"]) / 2
        df["gap_down"] = df["close"] < df["close"].shift(1)
        df["gap_up"] = df["open"].shift(-1) > df["close"]
        df["high_body"] = df[["open", "close"]].max(axis=1)
        df["low_body"] = df[["open", "close"]].min(axis=1)
        df["rsi"] = ta.rsi(df["close"], length=14)
        self.size_bodies(df)
        return self.trend(df)

    @abstractmethod
    def check_pattorn(self, df):
        pass

    def draw(self, df: pd.DataFrame):
        signals = self.check_pattorn(df)
        plot_df = df.copy()
        plot_df["datetime"] = pd.to_datetime(plot_df["datetime"])
        plot_df.set_index("datetime", inplace=True)
        plot_df["marker_up"] = np.nan
        plot_df["marker_down"] = np.nan
        plot_df.loc[signals.values, "marker_up"] = plot_df["low"] * 0.998

        view_plot = plot_df.iloc[-30:]
        addplots = []
        if view_plot["marker_up"].notna().any():
            addplots.append(
                mpf.make_addplot(
                    view_plot["marker_up"],
                    type="scatter",
                    markersize=120,
                    marker="^",
                    color="green",
                )
            )

        file_name = f"Trading/Graf/{self.name}_chart.png"
        mpf.plot(
            view_plot,
            type="candle",
            style="charles",
            title=f"Pattern: {self.name}",
            addplot=addplots,
            savefig=file_name,
        )
        return file_name


class Hammer(candel):
    def __init__(self):
        super().__init__("Hammer")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            (data["lower_shadow"] >= 2 * data["body_size"])
            & (data["upper_shadow"] <= data["body_size"])
            & (data["body_size"] > 0)
            & data["is_small_body"]
            & (data["rsi"] < 30)
        )


class Bullish_engulfing(candel):
    def __init__(self):
        super().__init__("bullish_engulfing")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bullish"]
            & data["is_bearish"].shift(1)
            & (data["open"] <= data["close"].shift(1))
            & (data["close"] >= data["open"].shift(1))
            & (data["body_size"] > data["body_size"].shift(1))
            & data["is_big_body"]
            & data["sma_down"]
        )


class Bearish_engulfing(candel):
    def __init__(self):
        super().__init__("bearish_engulfing")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bearish"]
            & data["is_bullish"].shift(1)
            & (data["open"] >= data["close"].shift(1))
            & (data["close"] <= data["open"].shift(1))
            & (data["body_size"] > data["body_size"].shift(1))
            & data["is_big_body"]
            & data["sma_up"]
        )


class Morning_star(candel):
    def __init__(self):
        super().__init__("Morning_star")

    def check_pattorn(self, df):
        data = self.parametrs(df.copy())
        return (
            data["is_bullish"]
            & data["is_small_body"].shift(1)
            & data["is_bearish"].shift(2)
            & data["is_big_body"].shift(2)
            & data["gap_down"].shift(1)
            & data["sma_down"]
        )


class Evening_star(candel):
    def __init__(self):
        super().__init__("Evening_Star")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bearish"]
            & data["is_small_body"].shift(1)
            & data["is_bullish"].shift(2)
            & data["is_big_body"].shift(2)
            & (data["close"] < data["open"].shift(2))
            & data["sma_up"]
            & data["gap_up"]
        )


class Bullish_harami(candel):
    def __init__(self):
        super().__init__("Bullish_Harami")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bearish"].shift(1)
            & data["is_bullish"]
            & data["is_big_body"].shift(1)
            & data["is_small_body"]
            & (data["high_body"] < data["high_body"].shift(1))
            & (data["low_body"] > data["low_body"].shift(1))
            & data["sma_down"]
        )


class Bearish_harami(candel):
    def __init__(self):
        super().__init__("Bearish_Harami")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bullish"].shift(1)
            & data["is_bearish"]
            & data["is_big_body"].shift(1)
            & data["is_small_body"]
            & (data["high_body"] < data["high_body"].shift(1))
            & (data["low_body"] > data["low_body"].shift(1))
            & data["sma_up"]
        )


class Dark_cloud_cover(candel):
    def __init__(self):
        super().__init__("Dark_Cloud_Cover")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bearish"]
            & data["is_bullish"].shift(1)
            & data["is_big_body"].shift(1)
            & (data["open"] > data["high"].shift(1))
            & (data["close"] < data["mid_body"].shift(1))
            & (data["close"] > data["open"].shift(1))
            & data["sma_up"]
        )


class Three_white_soldiers(candel):
    def __init__(self):
        super().__init__("Three_White_Soldiers")

    def check_pattorn(self, df: pd.DataFrame):
        data = self.parametrs(df.copy())
        return (
            data["is_bullish"]
            & data["is_bullish"].shift(1)
            & data["is_bullish"].shift(2)
            & data["is_big_body"]
            & data["is_big_body"].shift(1)
            & data["is_big_body"].shift(2)
            & (data["close"] > data["close"].shift(1))
            & (data["close"].shift(1) > data["close"].shift(2))
            & (data["open"] > data["open"].shift(1))
            & (data["open"] < data["close"].shift(1))
            & (data["upper_shadow"] < data["body_size"] * 0.2)
        )


__all__ = [
    "candel",
    "Hammer",
    "Bullish_engulfing",
    "Bearish_engulfing",
    "Morning_star",
    "Evening_star",
    "Bullish_harami",
    "Bearish_harami",
    "Dark_cloud_cover",
    "Three_white_soldiers",
]
