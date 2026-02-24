import talib
import pandas as pd
import plotly.graph_objects as go
from moexalgo import Ticker
from datetime import datetime, timedelta

# --- БЛОК 1: ЗАГРУЗКА ДАННЫХ ---
def get_moex_data(ticker_symbol, days=2, tf='15min'):
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
def detect_custom_hammer(df, shadow_ratio=2.0, upper_limit=0.1):
    """
    Детекция Молота по канонам Морриса.
    shadow_ratio: во сколько раз нижняя тень больше тела (минимум).
    upper_limit: какой процент от тела может составлять верхняя тень (максимум).
    """
    # 1. Считаем базовые параметры свечи
    body = abs(df['close'] - df['open'])
    lower_shadow = df[['open', 'close']].min(axis=1) - df['low']
    upper_shadow = df['high'] - df[['open', 'close']].max(axis=1)
    
    # 2. Условие: Нижняя тень в 2-3 раза больше тела
    # Добавляем небольшое смещение (1e-5), чтобы избежать деления на ноль у доджи
    cond1 = lower_shadow >= (body * shadow_ratio)
    
    # 3. Условие: Верхней тени практически нет
    cond2 = upper_shadow <= (body * upper_limit)
    
    # 4. Условие: Тело не должно быть нулевым (опционально, но для Молота важно)
    cond3 = body > 0

    # Объединяем всё в одну колонку (100 для совместимости с твоим кодом графиков)
    df['custom_hammer'] = 0
    df.loc[cond1 & cond2 & cond3, 'custom_hammer'] = 100
    
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

# --- БЛОК 3: ВИЗУАЛИЗАЦИЯ ---
def plot_results(df, ticker):
    fig = go.Figure(data=[go.Candlestick(
        x=df['datetime'], open=df['open'], high=df['high'],
        low=df['low'], close=df['close'], name='Свечи'
    )])

    # Словарь настроек: паттерн -> (цвет, символ, позиция)
    marks = {
        'hammer': ('blue', 'circle', 'low'),
        'engulfing': ('green', 'triangle-up', 'low'),
        'shooting_star': ('orange', 'triangle-down', 'high'),
        'morning_star': ('gold', 'star', 'low')
    }

    for p, (color, symbol, pos) in marks.items():
        # Бычьи (100)
        up = df[df[p] > 0]
        if not up.empty:
            fig.add_trace(go.Scatter(x=up['datetime'], y=up[pos]*0.999, mode='markers',
                          marker=dict(symbol=symbol, size=12, color=color), name=f'Bull {p}'))
        # Медвежьи (-100)
        down = df[df[p] < 0]
        if not down.empty:
            fig.add_trace(go.Scatter(x=down['datetime'], y=down[pos]*1.001, mode='markers',
                          marker=dict(symbol=symbol, size=12, color='red'), name=f'Bear {p}'))

    fig.update_layout(title=f'Тест паттернов {ticker} (15m)', xaxis_rangeslider_visible=False)
    fig.show()

# --- ЗАПУСК ---
if __name__ == "__main__":
    # Используй актуальный тикер (например, SiH6 для марта 2026)
    TICKER_NAME = 'SiH6' 
    data = get_moex_data(TICKER_NAME)
    
    if data is not None:
        data = apply_patterns(data)
        plot_results(data, TICKER_NAME)