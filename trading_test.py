import talib
from moexalgo import Ticker
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# 1. Загрузка данных (без изменений)
def get_moex_data(ticker_symbol):
    t = Ticker(ticker_symbol)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=2)
    
    # Получаем свечи (15 минут)
    candles = t.candles(date=start_date, till_date=end_date, period='15m')
    df = pd.DataFrame(candles)
    df['begin'] = pd.to_datetime(df['begin'])
    df = df[['begin', 'open', 'high', 'low', 'close', 'volume']]
    df.rename(columns={'begin': 'datetime'}, inplace=True)
    return df

# 2. Детекция паттернов через TA-Lib
def apply_talib_patterns(df):
    # TA-Lib принимает numpy массивы (float)
    o, h, l, c = df['open'].values, df['high'].values, df['low'].values, df['close'].values

    # Добавим популярные паттерны
    df['pattern_hammer'] = talib.CDLHAMMER(o, h, l, c)          # Молот
    df['pattern_engulfing'] = talib.CDLENGULFING(o, h, l, c)    # Поглощение
    df['pattern_doji'] = talib.CDLDOJI(o, h, l, c)              # Доджи
    df['pattern_morning_star'] = talib.CDLMORNINGSTAR(o, h, l, c) # Утренняя звезда
    
    return df

# 3. Визуализация
def plot_talib_results(df, ticker):
    fig = go.Figure(data=[go.Candlestick(
        x=df['datetime'], open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='Candlesticks'
    )])

    # Словарь для меток: (Название колонки, цвет, символ, позиция)
    patterns_to_draw = [
        ('pattern_hammer', 'blue', 'circle', 'low'),
        ('pattern_engulfing', 'green', 'triangle-up', 'low'),
        ('pattern_morning_star', 'gold', 'star', 'low'),
        ('pattern_doji', 'gray', 'cross', 'high')
    ]

    for col, color, symbol, pos in patterns_to_draw:
        # Бычьи сигналы (100)
        bullish = df[df[col] > 0]
        if not bullish.empty:
            fig.add_trace(go.Scatter(
                x=bullish['datetime'], 
                y=bullish['low'] * 0.999 if pos == 'low' else bullish['high'] * 1.001,
                mode='markers', name=f'Bull {col}',
                marker=dict(symbol=symbol, size=10, color=color)
            ))
        
        # Медвежьи сигналы (-100)
        bearish = df[df[col] < 0]
        if not bearish.empty:
            fig.add_trace(go.Scatter(
                x=bearish['datetime'], 
                y=bearish['low'] * 0.999 if pos == 'low' else bearish['high'] * 1.001,
                mode='markers', name=f'Bear {col}',
                marker=dict(symbol=symbol, size=10, color='red')
            ))

    fig.update_layout(title=f'TA-Lib Pattern Detector: {ticker}', xaxis_rangeslider_visible=False)
    fig.show()

# --- ТЕСТ ---
ticker = 'SiH6' # Убедись, что тикер актуален на сегодня!
df = get_moex_data(ticker)
df = apply_talib_patterns(df)
plot_talib_results(df, ticker)