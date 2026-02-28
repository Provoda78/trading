import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from moexalgo import Ticker
from datetime import datetime, timedelta
import requests
import time

# --- НАСТРОЙКИ ---
TOKEN = "8765910215:AAEPaq25irVjG4aEmN0Kl77XoI4IYJ50hcs"
CHAT_ID = "5595690153"

def create_screenshot(df, ticker, tf, pattern_name, signal_time):
    """
    Создает скриншот: 12 свечей ДО паттерна и ВСЕ свечи ПОСЛЕ.
    """
    try:
        idx = df.index[df['datetime'] == signal_time].tolist()[0]
    except IndexError:
        return None

    # Берем 12 свечей ДО и все доступные свечи ПОСЛЕ
    start_idx = max(0, idx - 20)
    end = min(len(df), idx + 20)
    plot_df = df.iloc[start_idx:end].copy() 
    
    fig = go.Figure(data=[go.Candlestick(
        x=plot_df['datetime'],
        open=plot_df['open'], high=plot_df['high'],
        low=plot_df['low'], close=plot_df['close'],
        name='Свечи'
    )])

    fig.add_trace(go.Scatter(
        x=plot_df['datetime'], y=plot_df['ema10'],
        mode='lines', line=dict(color='orange', width=1.5), name='EMA 10'
    ))

    fig.add_vline(x=signal_time, line_width=1, line_dash="dash", line_color="white")

    is_bullish = any(x in pattern_name.lower() for x in ['bull', 'hammer', 'piercing', 'inv_hammer'])
    y_pos = plot_df.loc[idx, 'low'] if is_bullish else plot_df.loc[idx, 'high']
    ay = 40 if is_bullish else -40
    color = "green" if is_bullish else "red"

    fig.add_annotation(
        x=signal_time, y=y_pos, text=pattern_name,
        showarrow=True, arrowhead=2, arrowcolor=color,
        ax=0, ay=ay, bgcolor=color, font=dict(color="white")
    )

    fig.update_layout(
        title=f"{ticker} {tf} | Разрез паттерна",
        template="plotly_dark", xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=40, b=10)
    )
    
    file_path = f"alert_{ticker}_{tf}.png"
    fig.write_image(file_path, scale=2)
    return file_path

def analyze_morris_patterns(df):
    if len(df) < 20: 
        return [], None, None
    
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    
    curr = df.iloc[-2].copy()
    prev = df.iloc[-3].copy()
    
    c_close, ema, c_time = curr['close'], curr['ema10'], curr['datetime']
    c_open, c_high, c_low = curr['open'], curr['high'], curr['low']
    
    c_body_size = abs(c_close - c_open)
    c_range = c_high - c_low
    c_body_top, c_body_bottom = max(c_open, c_close), min(c_open, c_close)
    c_upper_shadow = c_high - c_body_top
    c_lower_shadow = c_body_bottom - c_low
    
    p_open, p_close = prev['open'], prev['close']
    p_body_top, p_body_bottom = max(p_open, p_close), min(p_open, p_close)
    p_body_size = abs(p_close - p_open)
    p_midpoint = (p_open + p_close) / 2

    signals = []

    # --- 1. ТРЕНД ВНИЗ (Ищем бычьи развороты под EMA) ---
    if c_close < ema:
        if c_lower_shadow >= (c_body_size * 2) and c_upper_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Hammer (Молот)")
            
        if c_upper_shadow >= (c_body_size * 2) and c_lower_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Inverted Hammer (Перевернутый молот)")
            
        if c_close > c_open and p_close < p_open and c_body_top >= p_body_top and c_body_bottom <= p_body_bottom:
            signals.append("Bullish Engulfing (Бычье поглощение)")
            
        if p_close < p_open and c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and p_body_size > c_body_size:
            signals.append("Bullish Harami (Бычье Харами)")
            
        if p_close < p_open and c_close > c_open and c_open < p_close and c_close > p_midpoint:
            signals.append("Piercing Line (Просвет в облаках)")

    # --- 2. ТРЕНД ВВЕРХ (Ищем медвежьи развороты над EMA) ---
    elif c_close > ema:
        if c_lower_shadow >= (c_body_size * 2) and c_upper_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Hanging Man (Висельник)")
            
        if c_upper_shadow >= (c_body_size * 2) and c_lower_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Shooting Star (Падающая звезда)")
            
        if c_close < c_open and p_close > p_open and c_body_top >= p_body_top and c_body_bottom <= p_body_bottom:
            signals.append("Bearish Engulfing (Медвежье поглощение)")
            
        if p_close > p_open and c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and p_body_size > c_body_size:
            signals.append("Bearish Harami (Медвежье Харами)")
            
        if p_close > p_open and c_close < c_open and c_open > p_close and c_close < p_midpoint:
            signals.append("Dark Cloud Cover (Завеса из темных облаков)")

    # --- 3. НЕЙТРАЛЬНЫЕ ---
    if c_body_size <= (c_range * 0.1) and c_range > 0:
        signals.append("Doji (Доджи)")

    return signals, c_time, c_close

def analyze_morris_patterns_at_index(df, idx):
    """
    Анализирует паттерны на конкретном индексе 'idx'. 
    Логика Морриса сохранена.
    """
    if idx < 2: return []
    
    curr = df.iloc[idx].copy()
    prev = df.iloc[idx-1].copy()
    
    c_close, ema = curr['close'], curr['ema10']
    c_open, c_high, c_low = curr['open'], curr['high'], curr['low']
    c_body_size = abs(c_close - c_open)
    c_range = c_high - c_low
    c_body_top, c_body_bottom = max(c_open, c_close), min(c_open, c_close)
    c_upper_shadow = c_high - c_body_top
    c_lower_shadow = c_body_bottom - c_low
    
    p_open, p_close = prev['open'], prev['close']
    p_body_top, p_body_bottom = max(p_open, p_close), min(p_open, p_close)
    p_body_size = abs(p_close - p_open)
    p_midpoint = (p_open + p_close) / 2

    signals = []

    if c_close < ema: # Трендовый фильтр (Вниз)
        if c_lower_shadow >= (c_body_size * 2) and c_upper_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Hammer (Молот)")
        if c_upper_shadow >= (c_body_size * 2) and c_lower_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Inverted Hammer (Перевернутый молот)")
        if c_close > c_open and p_close < p_open and c_body_top >= p_body_top and c_body_bottom <= p_body_bottom:
            signals.append("Bullish Engulfing (Бычье поглощение)")
        if p_close < p_open and c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and p_body_size > c_body_size:
            signals.append("Bullish Harami (Бычье Харами)")
        if p_close < p_open and c_close > c_open and c_open < p_close and c_close > p_midpoint:
            signals.append("Piercing Line (Просвет в облаках)")

    elif c_close > ema: # Трендовый фильтр (Вверх)
        if c_lower_shadow >= (c_body_size * 2) and c_upper_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Hanging Man (Висельник)")
        if c_upper_shadow >= (c_body_size * 2) and c_lower_shadow <= (c_range * 0.1) and c_body_size > 0:
            signals.append("Shooting Star (Падающая звезда)")
        if c_close < c_open and p_close > p_open and c_body_top >= p_body_top and c_body_bottom <= p_body_bottom:
            signals.append("Bearish Engulfing (Медвежье поглощение)")
        if p_close > p_open and c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and p_body_size > c_body_size:
            signals.append("Bearish Harami (Медвежье Харами)")
        if p_close > p_open and c_close < c_open and c_open > p_close and c_close < p_midpoint:
            signals.append("Dark Cloud Cover (Завеса из темных облаков)")

    if c_body_size <= (c_range * 0.1) and c_range > 0:
        signals.append("Doji (Доджи)")

    return signals

# --- БЕЗОПАСНАЯ ЗАГРУЗКА ДАННЫХ (RETRY МЕХАНИЗМ) ---
def get_safe_candles(ticker_name, tf, days_back=4, retries=3):
    start_dt = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_dt = datetime.now().strftime('%Y-%m-%d')
    
    for attempt in range(retries):
        try:
            t = Ticker(ticker_name)
            data = t.candles(start=start_dt, end=end_dt, period=tf)
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            err_msg = str(e)
            if "isoformat" in err_msg or "NoneType" in err_msg:
                print(f"[!] Тикер {ticker_name} неактивен или данные недоступны.")
                return pd.DataFrame()
            elif "SSL" in err_msg or "EOF" in err_msg or "Connection" in err_msg:
                print(f"[*] Сбой сети при загрузке {ticker_name}. Попытка {attempt + 1}/{retries}...")
                time.sleep(2)
            else:
                print(f"[*] Неизвестная ошибка загрузки {ticker_name}: {err_msg}")
                return pd.DataFrame()
                
    return pd.DataFrame() 

def run_scanner():
    TICKERS = ['SBER'] 
    TIMEFRAMES = ['15min']
    last_alerts = {}

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Сканер запущен. Мониторинг: {TICKERS}")

    while True:
        for ticker in TICKERS:
            for tf in TIMEFRAMES:
                df = get_safe_candles(ticker, tf)
                
                if df.empty or 'begin' not in df.columns: 
                    continue
                
                try:
                    df['begin'] = pd.to_datetime(df['begin'])
                    df.rename(columns={'begin': 'datetime'}, inplace=True)
                    
                    found_patterns, candle_time, last_price = analyze_morris_patterns(df)
                    
                    if not found_patterns or candle_time is None:
                        continue

                    for pattern in found_patterns:
                        alert_key = (ticker, tf, pattern)
                        if last_alerts.get(alert_key) != candle_time:
                            
                            img_path = create_screenshot(df, ticker, tf, pattern, candle_time)
                            
                            if img_path:
                                text = (f"🎯 *{pattern}*\n"
                                        f"📊 `{ticker}` | `{tf}`\n"
                                        f"💰 Цена: `{last_price}`\n"
                                        f"⏰ Свеча: {candle_time.strftime('%H:%M')}")
                                
                                url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
                                
                                # Отправляем фото
                                with open(img_path, 'rb') as f:
                                    requests.post(url, data={'chat_id': CHAT_ID, 'caption': text, 'parse_mode': 'Markdown'}, files={'photo': f})
                                
                                # Удаляем файл после отправки, чтобы не засорять диск
                                try:
                                    os.remove(img_path)
                                except OSError as e:
                                    print(f"Ошибка при удалении {img_path}: {e}")
                                
                                last_alerts[alert_key] = candle_time
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] Отправлен сигнал: {ticker} {tf} {pattern}")

                except Exception as e:
                    print(f"Ошибка логики анализа {ticker} {tf}: {e}")
        
        # Ждем перед следующим циклом
        time.sleep(60)


def test_run(days_back=3):
    """
    Проходит по всей истории за последние дни и шлет скрины найденных паттернов.
    """
    TICKERS = ['SBER'] # Тестовый список
    TIMEFRAMES = ['15min']
    
    print(f"--- ЗАПУСК ТЕСТА ЗА {days_back} ДНЯ ---")

    for ticker in TICKERS:
        for tf in TIMEFRAMES:
            df = get_safe_candles(ticker, tf, days_back=days_back)
            if df.empty: continue
            
            df['begin'] = pd.to_datetime(df['begin'])
            df.rename(columns={'begin': 'datetime'}, inplace=True)
            df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()

            # Идем по истории (пропуская первые 10 свечей для EMA)
            for i in range(10, len(df)):
                found_patterns = analyze_morris_patterns_at_index(df, i)
                
                if found_patterns:
                    candle_time = df.loc[i, 'datetime']
                    last_price = df.loc[i, 'close']
                    
                    for pattern in found_patterns:
                        print(f"Найдено в истории: {ticker} {tf} {pattern} в {candle_time}")
                        
                        img_path = create_screenshot(df, ticker, tf, pattern, candle_time)
                        
                        if img_path:
                            text = (f"🧪 *ТЕСТОВЫЙ СИГНАЛ*\n"
                                    f"🎯 *{pattern}*\n"
                                    f"📊 `{ticker}` | `{tf}`\n"
                                    f"💰 Цена: `{last_price}`\n"
                                    f"⏰ Время: {candle_time.strftime('%d.%m %H:%M')}")
                            
                            url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
                            with open(img_path, 'rb') as f:
                                requests.post(url, data={'chat_id': CHAT_ID, 'caption': text, 'parse_mode': 'Markdown'}, files={'photo': f})
                            
                            if os.path.exists(img_path): os.remove(img_path)
                            time.sleep(1) # Защита от спам-фильтра Telegram

    print("--- ТЕСТ ЗАВЕРШЕН ---")

if __name__ == "__main__":
    test_run(days_back=2)