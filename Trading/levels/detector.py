import mplfinance as mpf
import pandas as pd


class LevelDetector:
    def __init__(self, window: int, sensitivity: float):
        self.window = window
        self.sensitivity = sensitivity

    def get_levels(self, df: pd.DataFrame) -> tuple[dict, dict]:
        df["is_min"] = (
            df["low"]
            == df["low"].rolling(window=self.window * 2 + 1, center=True).min()
        )
        df["is_max"] = (
            df["high"]
            == df["high"].rolling(window=self.window * 2 + 1, center=True).max()
        )

        supports = df[df["is_min"]]["low"].reset_index().values.tolist()
        resistances = df[df["is_max"]]["high"].reset_index().values.tolist()

        score_supports = self.cluster_levels(supports)
        score_resistances = self.cluster_levels(resistances)
        return self.find_mirro(score_supports, score_resistances)

    def cluster_levels(self, points: list) -> dict:
        levels = {}

        for index, point in points:
            found_nearby = False
            for level_price in list(levels.items()):
                if abs(point - level_price[0]) / level_price[0] <= self.sensitivity:
                    levels[level_price[0]][0] += 1
                    levels[level_price[0]][1] = index
                    found_nearby = True
                    break

            if not found_nearby:
                levels[point] = [1, index]

        return {price: [data[0] // 2, data[1]] for price, data in levels.items() if data[0] >= 2}

    def find_mirro(self, support_levels: dict, resistance_levels: dict) -> tuple[dict, dict]:
        support_prices = list(support_levels.keys())
        resistance_prices = list(resistance_levels.keys())
        supports = support_levels.copy()
        resistances = resistance_levels.copy()

        for support_price in support_prices:
            for resistance_price in resistance_prices:
                if abs(support_price - resistance_price) / support_price <= self.sensitivity:
                    total_score = (
                        supports[support_price][0]
                        + resistances[resistance_price][0]
                        + 2
                    )

                    if supports[support_price][1] > resistances[resistance_price][1]:
                        supports[support_price][0] = total_score
                        resistances.pop(resistance_price, None)
                    else:
                        resistances[resistance_price][0] = total_score
                        supports.pop(support_price, None)
                    break

        return supports, resistances

    def prepare_hlines(self, final_levels: tuple[dict, dict]) -> dict:
        supports, resistances = final_levels
        hlines_prices = []
        hlines_colors = []
        hlines_widths = []
        hlines_styles = []

        for level_dict, color in ((supports, "green"), (resistances, "red")):
            for price, data in level_dict.items():
                score = data[0]
                margin = price * self.sensitivity

                hlines_prices.extend((price + margin, price, price - margin))
                hlines_colors.extend((color, color, color))
                hlines_widths.extend((min(score, 4), min(score, 4), min(score, 4)))
                hlines_styles.extend((":", "-", ":"))

        return {
            "hlines": hlines_prices,
            "colors": hlines_colors,
            "linewidths": hlines_widths,
            "linestyle": hlines_styles,
        }

    def draw_levels(
        self,
        df: pd.DataFrame,
        ticker: str,
        tf: str,
        final_levels: tuple[dict, dict],
        pattern_name="Signal",
    ) -> str:
        plot_df = df.iloc[-100:].copy()
        plot_df["datetime"] = pd.to_datetime(plot_df["datetime"])
        plot_df.set_index("datetime", inplace=True)

        file_name = f"Trading/Graf/{ticker}_levels.png"
        mpf.plot(
            plot_df,
            type="candle",
            style="charles",
            title=f"{ticker} {tf} | Levels Analysis",
            hlines=self.prepare_hlines(final_levels),
            savefig=file_name,
            tight_layout=True,
        )
        return file_name
