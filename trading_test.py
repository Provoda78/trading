import talib
import pandas as pd
import plotly.graph_objects as go
from moexalgo import Ticker
from datetime import datetime, timedelta

# --- БЛОК 1: ЗАГРУЗКА ДАННЫХ ---
def get_moex_data(ticker_symbol, days=7, tf='10min'):
    print(f"Загрузка данных для {ticker_symbol}...")
    t = Ticker(ticker_symbol)
    
    # Исправленные аргументы: start и end
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Получаем свечи (в moexalgo аргументы start и end)
    candles = t.candles(start=start_date, end=end_date, period=tf)
    
    df = pd.DataFrame(candles)
    if df.empty:
        print("Данные не получены. Проверьте тикер или работу биржи.")
        return None
        
    df['begin'] = pd.to_datetime(df['begin'])
    df = df[['begin', 'open', 'high', 'low', 'close', 'volume']]
    df.rename(columns={'begin': 'datetime'}, inplace=True)
    return df

# --- БЛОК 2: АНАЛИЗ PATTERNS (TA-Lib) ---
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

def detect_manual_patterns(df):
    # --- БАЗОВЫЕ РАСЧЕТЫ ---
    df['body_top'] = df[['open', 'close']].max(axis=1)
    df['body_bottom'] = df[['open', 'close']].min(axis=1)
    df['body_size'] = abs(df['close'] - df['open'])
    df['range'] = df['high'] - df['low']
    df['midpoint'] = (df['open'] + df['close']) / 2
    
    # Тени
    df['lower_shadow'] = df['body_bottom'] - df['low']
    df['upper_shadow'] = df['high'] - df['body_top']

    # Сдвиги для предыдущей свечи
    prev_body_top = df['body_top'].shift(1)
    prev_body_bottom = df['body_bottom'].shift(1)
    prev_body_size = df['body_size'].shift(1)
    prev_close = df['close'].shift(1)
    prev_open = df['open'].shift(1)
    prev_midpoint = df['midpoint'].shift(1)

    # --- 1. ДОДЖИ (DOJI) ---
    df['m_doji'] = df['body_size'] <= (df['range'] * 0.1)

    # --- 2. ПОГЛОЩЕНИЕ (ENGULFING) ---
    bullish_engulfing = ((df['close'] > df['open']) & (prev_close < prev_open) & 
                         (df['body_top'] >= prev_body_top) & (df['body_bottom'] <= prev_body_bottom) & (df['body_size'] > prev_body_size))
    bearish_engulfing = ((df['close'] < df['open']) & (prev_close > prev_open) & 
                         (df['body_top'] >= prev_body_top) & (df['body_bottom'] <= prev_body_bottom) & (df['body_size'] > prev_body_size))

    # --- 3. ХАРАМИ (HARAMI) ---
    bullish_harami = ((df['close'] > df['open']) & (prev_close < prev_open) &
                      (df['body_top'] <= prev_body_top) & (df['body_bottom'] >= prev_body_bottom) & (df['body_size'] < prev_body_size))
    bearish_harami = ((df['close'] < df['open']) & (prev_close > prev_open) &
                      (df['body_top'] <= prev_body_top) & (df['body_bottom'] >= prev_body_bottom) & (df['body_size'] < prev_body_size))

    # --- 4. МОЛОТЫ (HAMMERS) ---
    # Обычный молот: нижняя тень > 2 тел, верхняя < 0.2 тела
    df['signal_hammer'] = ((df['lower_shadow'] >= df['body_size'] * 2) & 
                           (df['upper_shadow'] <= df['body_size'] * 0.2) & 
                           (df['body_size'] > 0))
    
    # Перевернутый молот: верхняя тень > 2 тел, нижняя < 0.2 тела
    df['signal_inv_hammer'] = ((df['upper_shadow'] >= df['body_size'] * 2) & 
                               (df['lower_shadow'] <= df['body_size'] * 0.2) & 
                               (df['body_size'] > 0))

    # --- 5. ТЕМНЫЕ ОБЛАКА И ПРОСВЕТ В ОБЛАКАХ ---
    # Темные облака (Медвежий разворот)
    dark_cloud = (
        (prev_close > prev_open) &  # Предыдущая - зеленая (бычья)
        (df['close'] < df['open']) &  # Текущая - красная (медвежья)
        (df['open'] >= prev_close) &  # Открылась не ниже закрытия прошлой
        (df['close'] < prev_midpoint) & # Закрылась НИЖЕ середины прошлой
        (df['close'] >= prev_open)      # Но не перекрыла её полностью (иначе это Поглощение)
    )

    # Просвет в облаках (Бычий разворот - пара для Темных облаков)
    piercing_line = (
        (prev_close < prev_open) &  # Предыдущая - красная
        (df['close'] > df['open']) &  # Текущая - зеленая
        (df['open'] <= prev_close) &  # Открылась не выше закрытия прошлой
        (df['close'] > prev_midpoint) & # Закрылась ВЫШЕ середины прошлой
        (df['close'] <= prev_open)      # Не перекрыла полностью
    )

    # --- ЗАПИСЬ СИГНАЛОВ ---
    df['signal_engulfing'] = np.where(bullish_engulfing, 100, np.where(bearish_engulfing, -100, 0))
    df['signal_harami'] = np.where(bullish_harami, 100, np.where(bearish_harami, -100, 0))
    df['signal_clouds'] = np.where(piercing_line, 100, np.where(dark_cloud, -100, 0)) # 100=Просвет, -100=Темные облака

    return df


def apply_patterns(df):
    # Принудительно конвертируем в float64 (double)
    # Это решает проблему "input array type is not double"
    o = df['open'].astype(float).values
    h = df['high'].astype(float).values
    l = df['low'].astype(float).values
    c = df['close'].astype(float).values
    
    # Теперь TA-Lib примет эти данные без ошибок
    df['hammer'] = talib.CDLHAMMER(o, h, l, c)
    df['engulfing'] = talib.CDLENGULFING(o, h, l, c)
    df['shooting_star'] = talib.CDLSHOOTINGSTAR(o, h, l, c)
    df['morning_star'] = talib.CDLMORNINGSTAR(o, h, l, c)
    
    return df

# --- БЛОК 3: ВИЗУАЛИЗАЦИЯ (Адаптировано под ручные паттерны Морриса) ---
def plot_results(df, ticker):
    fig = go.Figure(data=[go.Candlestick(
        x=df['datetime'], open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='Свечи'
    )])

    # 1. Доджи
    dojis = df[df['m_doji']]
    if not dojis.empty:
        fig.add_trace(go.Scatter(x=dojis['datetime'], y=dojis['high']*1.001, mode='markers',
                      marker=dict(symbol='cross', size=8, color='gray'), name='Doji'))

    # 2. Поглощение (Большие треугольники)
    for val, color, symbol, pos in [(100, 'green', 'triangle-up', 'low'), (-100, 'red', 'triangle-down', 'high')]:
        mask = df[df['signal_engulfing'] == val]
        if not mask.empty:
            y_val = mask[pos]*0.999 if pos == 'low' else mask[pos]*1.001
            fig.add_trace(go.Scatter(x=mask['datetime'], y=y_val, mode='markers',
                          marker=dict(symbol=symbol, size=14, color=color), name=f'Engulfing {val}'))

    # 3. Харами (Кружки)
    for val, color, pos in [(100, 'lightgreen', 'low'), (-100, 'orange', 'high')]:
        mask = df[df['signal_harami'] == val]
        if not mask.empty:
            y_val = mask[pos]*0.999 if pos == 'low' else mask[pos]*1.001
            fig.add_trace(go.Scatter(x=mask['datetime'], y=y_val, mode='markers',
                          marker=dict(symbol='circle', size=10, color=color), name=f'Harami {val}'))

    # 4. Молоты (Квадраты снизу/сверху)
    hammers = df[df['signal_hammer']]
    if not hammers.empty:
        fig.add_trace(go.Scatter(x=hammers['datetime'], y=hammers['low']*0.998, mode='markers',
                      marker=dict(symbol='square', size=10, color='blue'), name='Hammer'))
        
    inv_hammers = df[df['signal_inv_hammer']]
    if not inv_hammers.empty:
        # Перевернутый молот тоже бычий сигнал (часто), рисуем под свечой
        fig.add_trace(go.Scatter(x=inv_hammers['datetime'], y=inv_hammers['low']*0.998, mode='markers',
                      marker=dict(symbol='square', size=10, color='lightblue'), name='Inv Hammer'))

    # 5. Темные облака / Просвет в облаках (Ромбы)
    for val, color, pos, name in [(100, 'cyan', 'low', 'Piercing Line'), (-100, 'purple', 'high', 'Dark Cloud')]:
        mask = df[df['signal_clouds'] == val]
        if not mask.empty:
            y_val = mask[pos]*0.998 if pos == 'low' else mask[pos]*1.002
            fig.add_trace(go.Scatter(x=mask['datetime'], y=y_val, mode='markers',
                          marker=dict(symbol='diamond', size=12, color=color), name=name))

    fig.update_layout(title=f'Моррис: Ручные паттерны (15m) | {ticker}', xaxis_rangeslider_visible=False, height=800)
    fig.show()

# --- ЗАПУСК ---
if __name__ == "__main__":
    TICKER_NAME = 'SiH6' # Убедись, что тикер актуальный!
    
    # 1. Получаем данные
    data = get_moex_data(TICKER_NAME)
    
    if data is not None:
        # 2. Ищем паттерны вручную по Моррису
        data = detect_manual_patterns(data)
        
        # 3. Рисуем график
        plot_results(data, TICKER_NAME)