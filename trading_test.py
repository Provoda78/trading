import os
import time
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from moexalgo import Ticker
from datetime import datetime, timedelta

# --- КОНФИГУРАЦИЯ ---
TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# Список инструментов и таймфреймов
TICKERS = ['SiH6', 'BRH6', 'NGG6', 'MXH6'] 
TIMEFRAMES = ['15min', '10min', '5min'] 
CHECK_INTERVAL = 60 # Проверка раз в минуту

# Память бота: {(ticker, tf, pattern): last_candle_time}
last_alerts = {}

# --- БЛОК 1: ТЕЛЕГРАМ И ГРАФИКА ---

def send_telegram(text, image_path=None):
    url = f"https://api.telegram.org/bot{TOKEN}/"
    try:
        if image_path:
            with open(image_path, 'rb') as f:
                requests.post(url + "sendPhoto", data={'chat_id': CHAT_ID, 'caption': text}, files={'photo': f})
        else:
            requests.post(url + "sendMessage", data={'chat_id': CHAT_ID, 'text': text})
    except Exception as e:
        print(f"Ошибка отправки в TG: {e}")

def create_screenshot(df, ticker, tf, pattern):
    # Берем последние 40 свечей для наглядности
    plot_df = df.tail(40)
    fig = go.Figure(data=[go.Candlestick(
        x=plot_df['datetime'], open=plot_df['open'], high=plot_df['high'],
        low=plot_df['low'], close=plot_df['close'], name=ticker
    )])
    
    fig.update_layout(
        title=f"СИГНАЛ: {pattern} | {ticker} [{tf}]",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )
    
    file_path = f"snap_{ticker}_{tf}.png"
    fig.write_image(file_path, engine="kaleido")
    return file_path

# --- БЛОК 2: ДВИЖОК МОРРИСА (ПОЛНАЯ ЛОГИКА) ---

def analyze_morris_patterns(df):
    """
    Возвращает список сигналов для ПОСЛЕДНЕЙ ЗАКРЫТОЙ свечи.
    Используется строго та же логика, что мы писали ранее.
    """
    if len(df) < 3: return []
    
    # Мы анализируем свечу index[-2], так как index[-1] еще "дышит"
    curr = df.iloc[-2].copy()
    prev = df.iloc[-3].copy()
    
    # Вспомогательные расчеты для текущей
    c_open, c_close, c_high, c_low = curr['open'], curr['close'], curr['high'], curr['low']
    c_body_top, c_body_bottom = max(c_open, c_close), min(c_open, c_close)
    c_body_size = abs(c_close - c_open)
    c_range = c_high - c_low
    c_midpoint = (c_open + c_close) / 2
    
    # Вспомогательные расчеты для предыдущей
    p_open, p_close = prev['open'], prev['close']
    p_body_top, p_body_bottom = max(p_open, p_close), min(p_open, p_close)
    p_body_size = abs(p_close - p_open)
    p_midpoint = (p_open + p_close) / 2

    signals = []

    # 1. ДОДЖИ
    if c_body_size <= (c_range * 0.1) and c_range > 0:
        signals.append("Doji (Неопределенность)")

    # 2. ПОГЛОЩЕНИЕ (ENGULFING)
    if c_close > c_open and p_close < p_open: # Бычье
        if c_body_top >= p_body_top and c_body_bottom <= p_body_bottom and c_body_size > p_body_size:
            signals.append("Bullish Engulfing (Поглощение)")
    elif c_close < c_open and p_close > p_open: # Медвежье
        if c_body_top >= p_body_top and c_body_bottom <= p_body_bottom and c_body_size > p_body_size:
            signals.append("Bearish Engulfing (Поглощение)")

    # 3. ХАРАМИ (HARAMI)
    if c_close > c_open and p_close < p_open: # Бычье
        if c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and c_body_size < p_body_size:
            signals.append("Bullish Harami (Внутренний день)")
    elif c_close < c_open and p_close > p_open: # Медвежье
        if c_body_top <= p_body_top and c_body_bottom >= p_body_bottom and c_body_size < p_body_size:
            signals.append("Bearish Harami (Внутренний день)")

    # 4. МОЛОТЫ
    l_shadow = c_body_bottom - c_low
    u_shadow = c_high - c_body_top
    if l_shadow >= (c_body_size * 2) and u_shadow <= (c_body_size * 0.2) and c_body_size > 0:
        signals.append("Hammer (Молот)")
    if u_shadow >= (c_body_size * 2) and l_shadow <= (c_body_size * 0.2) and c_body_size > 0:
        signals.append("Inverted Hammer (Перевернутый молот)")

    # 5. ТЕМНЫЕ ОБЛАКА И ПРОСВЕТ В ОБЛАКАХ
    # Темные облака (Bearish)
    if p_close > p_open and c_close < c_open:
        if c_open >= p_close and c_close < p_midpoint and c_close >= p_open:
            signals.append("Dark Cloud Cover (Темные облака)")
    # Просвет в облаках (Bullish)
    if p_close < p_open and c_close > c_open:
        if c_open <= p_close and c_close > p_midpoint and c_close <= p_open:
            signals.append("Piercing Line (Просвет в облаках)")

    return signals, curr['datetime'], c_close

# --- БЛОК 3: ГЛАВНЫЙ СКАНЕР ---

def run_scanner():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Стенд запущен. Мониторинг: {TICKERS}")
    
    while True:
        # Проверяем, открыта ли биржа (будни, с 10:00 до 23:50)
        now = datetime.now()
        if now.weekday() >= 5: # Суббота, Воскресенье
            time.sleep(3600)
            continue

        for ticker in TICKERS:
            for tf in TIMEFRAMES:
                try:
                    t = Ticker(ticker)
                    # Берем данные с запасом (1 день)
                    data = t.candles(start=now-timedelta(days=1), end=now, period=tf)
                    df = pd.DataFrame(data)
                    
                    if df.empty: continue
                    
                    df['begin'] = pd.to_datetime(df['begin'])
                    df.rename(columns={'begin': 'datetime'}, inplace=True)
                    
                    # Анализ
                    found_patterns, candle_time, last_price = analyze_morris_patterns(df)
                    
                    for pattern in found_patterns:
                        alert_key = (ticker, tf, pattern)
                        
                        # Если для этой свечи мы еще не кидали этот паттерн
                        if last_alerts.get(alert_key) != candle_time:
                            print(f"!!! СИГНАЛ: {ticker} {tf} - {pattern}")
                            
                            # Делаем скрин и шлем в ТГ
                            img_path = create_screenshot(df, ticker, tf, pattern)
                            text = (f"🎯 **{pattern}**\n\n"
                                    f"📊 Инструмент: `{ticker}`\n"
                                    f"⏳ Таймфрейм: `{tf}`\n"
                                    f"💰 Цена закрытия: `{last_price}`\n"
                                    f"⏰ Время свечи: {candle_time.strftime('%H:%M')}")
                            
                            send_telegram(text, img_path)
                            
                            # Запоминаем, чтобы не дублировать
                            last_alerts[alert_key] = candle_time
                            
                except Exception as e:
                    print(f"Ошибка {ticker} {tf}: {e}")
                    
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run_scanner()