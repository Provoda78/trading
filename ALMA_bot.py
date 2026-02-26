import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from moexalgo import Ticker
from datetime import datetime, timedelta

# --- БЛОК 1: ЗАГРУЗКА ДАННЫХ ---
def get_moex_data(ticker_symbol, days=5, tf='15min'):
    print(f"Загрузка данных для {ticker_symbol}...")
    t = Ticker(ticker_symbol)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    candles = t.candles(start=start_date, end=end_date, period=tf)
    df = pd.DataFrame(candles)
    
    if df.empty:
        print("Данные не получены.")
        return None
        
    df['begin'] = pd.to_datetime(df['begin'])
    df.rename(columns={'begin': 'datetime'}, inplace=True)
    return df

# --- БЛОК 2: ЛОГИКА ALMA И WAVETREND ---
def calculate_alma(series, window=9, offset=0.85, sigma=6):
    m = offset * (window - 1)
    s = window / sigma
    weights = np.exp(-((np.arange(window) - m) ** 2) / (2 * s ** 2))
    weights /= weights.sum()
    return series.rolling(window).apply(lambda x: (x * weights).sum(), raw=True)

def apply_strategy(df):
    # Параметры
    n1, n2 = 14, 21
    
    # Typical Price
    ap = (df['high'] + df['low'] + df['close']) / 3
    
    # WaveTrend расчет
    esa = ap.ewm(span=n1, adjust=False).mean()
    d = abs(ap - esa).ewm(span=n1, adjust=False).mean()
    ci = (ap - esa) / (0.015 * d)
    
    df['wt1'] = ci.ewm(span=n2, adjust=False).mean()
    # Твоя модернизация: ALMA вместо SMA для wt2
    df['wt2'] = calculate_alma(df['wt1'], window=4, offset=0.85, sigma=6)
    
    # Логика сигналов
    df['buy_sig'] = (df['wt1'] > df['wt2']) & (df['wt1'].shift(1) <= df['wt2'].shift(1))
    df['sell_sig'] = (df['wt1'] < df['wt2']) & (df['wt1'].shift(1) >= df['wt2'].shift(1))
    
    return df

# --- БЛОК 3: ВИЗУАЛИЗАЦИЯ ---
def plot_alma_strategy(df, ticker):
    # Создаем 2 подграфика: 1 для свечей, 2 для WaveTrend
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05, row_heights=[0.7, 0.3])

    # Основной график: Свечи
    fig.add_trace(go.Candlestick(
        x=df['datetime'], open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='Свечи'
    ), row=1, col=1)

    # Сигналы BUY на основном графике
    buys = df[df['buy_sig']]
    fig.add_trace(go.Scatter(
        x=buys['datetime'], y=buys['low'] * 0.999,
        mode='markers', marker=dict(symbol='triangle-up', size=15, color='green'),
        name='BUY'
    ), row=1, col=1)

    # Сигналы SELL на основном графике
    sells = df[df['sell_sig']]
    fig.add_trace(go.Scatter(
        x=sells['datetime'], y=sells['high'] * 1.001,
        mode='markers', marker=dict(symbol='triangle-down', size=15, color='red'),
        name='SELL'
    ), row=1, col=1)

    # Подграфик: WaveTrend
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['wt1'], line=dict(color='blue', width=1), name='WT1'), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['wt2'], line=dict(color='orange', width=1, dash='dot'), name='WT2 (ALMA)'), row=2, col=1)
    
    # Линии перекупленности/перепроданности
    for level, color in [(60, 'red'), (-60, 'green'), (0, 'gray')]:
        fig.add_hline(y=level, line_dash="dash", line_color=color, row=2, col=1)

    fig.update_layout(
        title=f'ALMA + WaveTrend Стратегия | {ticker}',
        xaxis_rangeslider_visible=False,
        height=900,
        template='plotly_dark'
    )
    fig.show()

# --- ЗАПУСК ---
if __name__ == "__main__":
    # Актуальный тикер нефти Brent (фьючерс) на MOEX, например BRH6 (март 2026)
    TICKER_NAME = 'BRH6' 
    
    data = get_moex_data(TICKER_NAME, days=5, tf='15min')
    
    if data is not None:
        data = apply_strategy(data)
        plot_alma_strategy(data, TICKER_NAME)