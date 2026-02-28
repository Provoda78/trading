import pandas as pd
import numpy as np
import plotly.graph_objects as go
from moexalgo import Ticker
from datetime import datetime, timedelta

# --- 1. ЗАГРУЗКА ДАННЫХ ---
def get_historical_data(ticker_symbol, days=7, tf='15min'):
    print(f"Загрузка истории для {ticker_symbol} за {days} дней...")
    t = Ticker(ticker_symbol)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    candles = t.candles(start=start_date, end=end_date, period=tf)
    df = pd.DataFrame(candles)
    
    if df.empty:
        print("Нет данных!")
        return None
        
    df['begin'] = pd.to_datetime(df['begin'])
    df.rename(columns={'begin': 'datetime'}, inplace=True)
    return df

# --- 2. ВЕКТОРНЫЙ АНАЛИЗАТОР (ПО МОРРИСУ + EMA) ---
def detect_all_patterns_vectorized(df):
    # Расчет EMA 10
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    
    # Тренд
    downtrend = df['close'] < df['ema10']
    uptrend = df['close'] > df['ema10']
    
    # Параметры текущей свечи
    df['body_top'] = df[['open', 'close']].max(axis=1)
    df['body_bottom'] = df[['open', 'close']].min(axis=1)
    df['body_size'] = abs(df['close'] - df['open'])
    df['range'] = df['high'] - df['low']
    df['l_shadow'] = df['body_bottom'] - df['low']
    df['u_shadow'] = df['high'] - df['body_top']
    
    # Параметры предыдущей свечи (сдвиг)
    p_open = df['open'].shift(1)
    p_close = df['close'].shift(1)
    p_body_top = df['body_top'].shift(1)
    p_body_bottom = df['body_bottom'].shift(1)
    p_body_size = df['body_size'].shift(1)
    p_midpoint = (p_open + p_close) / 2

    # Создаем пустые колонки для сигналов
    patterns = ['doji', 'hammer', 'hanging_man', 'inv_hammer', 'shooting_star', 
                'bull_engulfing', 'bear_engulfing', 'bull_harami', 'bear_harami', 
                'piercing_line', 'dark_cloud']
    for p in patterns:
        df[p] = False

    # 1. Доджи (нейтральный)
    df['doji'] = (df['body_size'] <= (df['range'] * 0.1)) & (df['range'] > 0)

    # 2. Молоты и Звезды (по форме)
    is_hammer_shape = (df['l_shadow'] >= df['body_size'] * 2) & (df['u_shadow'] <= df['body_size'] * 0.2) & (df['body_size'] > 0)
    is_inv_hammer_shape = (df['u_shadow'] >= df['body_size'] * 2) & (df['l_shadow'] <= df['body_size'] * 0.2) & (df['body_size'] > 0)

    df.loc[is_hammer_shape & downtrend, 'hammer'] = True
    df.loc[is_hammer_shape & uptrend, 'hanging_man'] = True
    df.loc[is_inv_hammer_shape & downtrend, 'inv_hammer'] = True
    df.loc[is_inv_hammer_shape & uptrend, 'shooting_star'] = True

    # 3. Поглощение
    is_bull_engulfing = (df['close'] > df['open']) & (p_close < p_open) & (df['body_top'] >= p_body_top) & (df['body_bottom'] <= p_body_bottom) & (df['body_size'] > p_body_size)
    is_bear_engulfing = (df['close'] < df['open']) & (p_close > p_open) & (df['body_top'] >= p_body_top) & (df['body_bottom'] <= p_body_bottom) & (df['body_size'] > p_body_size)
    
    df.loc[is_bull_engulfing & downtrend, 'bull_engulfing'] = True
    df.loc[is_bear_engulfing & uptrend, 'bear_engulfing'] = True

    # 4. Харами
    is_bull_harami = (df['close'] > df['open']) & (p_close < p_open) & (df['body_top'] <= p_body_top) & (df['body_bottom'] >= p_body_bottom) & (df['body_size'] < p_body_size)
    is_bear_harami = (df['close'] < df['open']) & (p_close > p_open) & (df['body_top'] <= p_body_top) & (df['body_bottom'] >= p_body_bottom) & (df['body_size'] < p_body_size)
    
    df.loc[is_bull_harami & downtrend, 'bull_harami'] = True
    df.loc[is_bear_harami & uptrend, 'bear_harami'] = True

    # 5. Облака и Просвет
    is_dark_cloud = (p_close > p_open) & (df['close'] < df['open']) & (df['open'] >= p_close) & (df['close'] < p_midpoint) & (df['close'] >= p_open)
    is_piercing = (p_close < p_open) & (df['close'] > df['open']) & (df['open'] <= p_close) & (df['close'] > p_midpoint) & (df['close'] <= p_open)

    df.loc[is_dark_cloud & uptrend, 'dark_cloud'] = True
    df.loc[is_piercing & downtrend, 'piercing_line'] = True

    return df

# --- 3. ИНТЕРАКТИВНЫЙ ГРАФИК ---
def plot_test_chart(df, ticker):
    fig = go.Figure()

    # Свечи
    fig.add_trace(go.Candlestick(
        x=df['datetime'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], name='Свечи'
    ))

    # Линия EMA 10
    fig.add_trace(go.Scatter(
        x=df['datetime'], y=df['ema10'], mode='lines', name='EMA 10', line=dict(color='orange', width=2)
    ))

    # Настройки маркеров: (колонка, цвет, символ, позиция, название)
    markers_config = [
        ('doji', 'gray', 'cross', 'high', 'Doji'),
        ('hammer', 'blue', 'triangle-up', 'low', 'Hammer'),
        ('hanging_man', 'red', 'triangle-down', 'high', 'Hanging Man'),
        ('inv_hammer', 'lightblue', 'triangle-up', 'low', 'Inv Hammer'),
        ('shooting_star', 'pink', 'triangle-down', 'high', 'Shooting Star'),
        ('bull_engulfing', 'green', 'square', 'low', 'Bull Engulfing'),
        ('bear_engulfing', 'red', 'square', 'high', 'Bear Engulfing'),
        ('bull_harami', 'lightgreen', 'circle', 'low', 'Bull Harami'),
        ('bear_harami', 'orange', 'circle', 'high', 'Bear Harami'),
        ('piercing_line', 'cyan', 'diamond', 'low', 'Piercing Line'),
        ('dark_cloud', 'purple', 'diamond', 'high', 'Dark Cloud')
    ]

    for col, color, symbol, pos, name in markers_config:
        mask = df[df[col] == True]
        if not mask.empty:
            # Смещаем маркеры чуть выше/ниже свечи для красоты
            y_vals = mask['low'] * 0.998 if pos == 'low' else mask['high'] * 1.002
            fig.add_trace(go.Scatter(
                x=mask['datetime'], y=y_vals, mode='markers',
                marker=dict(symbol=symbol, size=12, color=color, line=dict(width=1, color='black')),
                name=name
            ))

    fig.update_layout(
        title=f'Локальный Тест: {ticker} | EMA 10 Фильтр',
        yaxis_title='Цена', xaxis_rangeslider_visible=False, height=800, template='plotly_dark'
    )
    fig.show()

# --- ЗАПУСК ТЕСТА ---
if __name__ == "__main__":
    TICKER = 'SBER' # Укажите актуальный тикер
    DAYS_TO_LOAD = 10 # Загрузим неделю данных
    
    data = get_historical_data(TICKER, days=DAYS_TO_LOAD)
    if data is not None:
        data = detect_all_patterns_vectorized(data)
        plot_test_chart(data, TICKER)