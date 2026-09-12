import asyncio

from Trading.common.market_data import get_candles


async def scan(tickers, patterns_dict, notifier, state_manager, timeframe="1h"):
    print(f"🚀 Запуск сканера подтвержденных сигналов... [{timeframe}]")

    for ticker in tickers:
        df = await get_candles(ticker, interval=timeframe, limit=80)
        if df.empty:
            continue

        await state_manager.process_tick(ticker, timeframe, df, notifier)

        for pattern, pattern_type in patterns_dict.items():
            signals = pattern.check_pattorn(df)
            if not signals.iloc[-2]:
                continue

            ticker_key = f"{ticker}_{timeframe}"
            if (
                ticker_key in state_manager.pending
                and state_manager.pending[ticker_key]["pattern"] == pattern.name
            ):
                continue

            print(f"🚨 СИГНАЛ! {pattern.name} по {ticker}")
            chart_path = pattern.draw(df)
            await notifier.send_signal(
                f"{pattern.name} (Не подтвержден)",
                ticker,
                chart_path,
                timeframe,
            )
            state_manager.add_pattern(
                ticker,
                timeframe,
                pattern.name,
                pattern_type,
                df.iloc[-1]["close"],
                pattern_time=df.iloc[-1]["datetime"],
            )

        await asyncio.sleep(0.5)

    print(f"{timeframe} проверен!")


async def scan_all_timeframes(tickers, patterns_dict, notifier, state_manager):
    tasks = [
        scan(tickers, patterns_dict, notifier, state_manager, timeframe)
        for timeframe in notifier.timeframe_chats
    ]
    await asyncio.gather(*tasks)
    print("И всё...")

