class LevelStateManager:
    def __init__(self, sensitivity: float, missing_limit: int = 3):
        self.sensitivity = sensitivity
        self.missing_limit = missing_limit
        self.levels = {}

    def _find_match(self, stored_levels, level_type, price, used_indexes):
        for index, level in enumerate(stored_levels):
            if index in used_indexes or level["type"] != level_type:
                continue
            if abs(price - level["price"]) / level["price"] <= self.sensitivity:
                return index, level
        return None, None

    @staticmethod
    def _is_broken(level_type, close_price, level_price, sensitivity):
        if level_type == "support":
            return close_price < level_price * (1 - sensitivity)
        return close_price > level_price * (1 + sensitivity)

    async def process(self, ticker, timeframe, df, detector, notifier):
        supports, resistances = detector.get_levels(df)
        current_levels = [
            {"type": "support", "price": price, "score": data[0]}
            for price, data in supports.items()
        ] + [
            {"type": "resistance", "price": price, "score": data[0]}
            for price, data in resistances.items()
        ]

        key = f"{ticker}_{timeframe}"
        stored_levels = self.levels.setdefault(key, [])
        used_indexes = set()
        new_levels = []
        breakouts = []
        close_price = float(df["close"].iloc[-1])

        for current in current_levels:
            index, stored = self._find_match(
                stored_levels, current["type"], current["price"], used_indexes
            )

            if stored is None:
                stored = {
                    **current,
                    "missing_scans": 0,
                    "broken": False,
                }
                stored_levels.append(stored)
                new_levels.append(current)
                used_indexes.add(len(stored_levels) - 1)
                continue

            used_indexes.add(index)
            stored.update(current, missing_scans=0)
            is_broken = self._is_broken(
                stored["type"], close_price, stored["price"], self.sensitivity
            )
            if is_broken and not stored["broken"]:
                breakouts.append(stored.copy())
            stored["broken"] = is_broken

        for index, stored in enumerate(stored_levels):
            if index not in used_indexes:
                stored["missing_scans"] += 1

        self.levels[key] = [
            level
            for level in stored_levels
            if level["missing_scans"] < self.missing_limit
        ]

        if new_levels:
            file = detector.draw_levels(
                df, ticker, timeframe, (supports, resistances)
            )
            new_supports = {
                item["price"]: [item["score"], 0]
                for item in new_levels
                if item["type"] == "support"
            }
            new_resistances = {
                item["price"]: [item["score"], 0]
                for item in new_levels
                if item["type"] == "resistance"
            }
            await notifier.send_signal_levels(
                ticker,
                file,
                timeframe,
                new_supports,
                new_resistances,
            )

        for breakout in breakouts:
            file = detector.draw_levels(
                df, ticker, timeframe, (supports, resistances)
            )
            await notifier.send_breakout(
                ticker,
                timeframe,
                breakout["type"],
                breakout["price"],
                close_price,
                file,
            )
