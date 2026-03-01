import pandas as pd
import time
from datetime import datetime, timedelta
from moexalgo import Ticker
import random
import pandas_ta as ta
import mplfinance as mpf
import numpy as np

def get_candles(ticker_name, tf, days_needed=10, retries=3):
      
    technical_need = 30
    total_days = days_needed + technical_need
    start_dt = (datetime.now() - timedelta(days=total_days)).strftime('%Y-%m-%d')
    end_dt = datetime.now().strftime('%Y-%m-%d')
    
    base_delay = 2 #начальная задержка
    
    for attempt in range(retries):  
              
        try:
            tc = Ticker(ticker_name)
            data = tc.candles(start=start_dt, end=end_dt, period=tf)

            df = pd.DataFrame(data)

            if df.empty:
                print(f"[!] Данные для {ticker_name} отсутствуют (пустой ответ).")
                return pd.DataFrame()
            
            df = df.rename(columns={'begin': 'datetime'})
                
            return df
        
        except Exception as e:
            
#            print(f"ERROR: {e}")
            
            if "not found" in str(e) or "NoneType" in str(e):
                print(f"[!] Тикер {ticker_name} неактивен или данные недоступны.")
                return pd.DataFrame()

            jitter = random.uniform(0, 1) 
            wait_time = (base_delay * (2 ** attempt)) + jitter
            print(f"[*] Сбой сети при загрузке {ticker_name}. Попытка {attempt + 1}/{retries}...")
            time.sleep(wait_time)
                
    return pd.DataFrame()

def detect_patterns(df):
    
    signals = {
        "bullish_engulfing": [],
        "bearish_engulfing": [],
        "hammer": []
    }
    
    if df.empty:
        return signals
    
    df_copy = df.copy()
    
    #Размеры свечей
    df['body_size'] = abs(df['close'] - df['open'])
    df['is_bullish'] = df['close'] > df['open']
    df['is_bearish'] = df['close'] < df['open']
    
    # Тени свечей
    df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
    df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
    
    #Подтверждение
    df['confirmed_bull'] = (df['close'].shift(-1) > df['close'])
    df['confirmed_bear'] = (df['close'].shift(-1) < df['close'])
    
    df['rsi'] = ta.rsi(df['close'], length=14)
    df['sma_200'] = ta.sma(df['close'], length=200)
    
    #Бычьего поглощения (Bullish Engulfing)
    df['bullish_engulfing'] = (
        (df['is_bullish']) & 
        (df['is_bearish'].shift(1)) & 
        (df['open'] <= df['close'].shift(1)) & 
        (df['close'] >= df['open'].shift(1)) &
        (df['body_size'] > df['body_size'].shift(1)) &
        (df['confirmed_bull'])
    )
    
    signals['bullish_engulfing'] = df[df['bullish_engulfing'] == True]['datetime'].tolist()
    
    #Медвежьего поглощения (Bearish Engulfing)
    df['bearish_engulfing'] = (
        (df['is_bearish']) & 
        (df['is_bullish'].shift(1)) & 
        (df['open'] >= df['close'].shift(1)) & 
        (df['close'] <= df['open'].shift(1)) &
        (df['body_size'] > df['body_size'].shift(1)) &
        (df['confirmed_bear'])
    )
    
    signals["bearish_engulfing"] = df[df['bearish_engulfing'] == True]['datetime'].tolist()
    
    #Молот (Hammer)
    df['hammer'] = (
        (df['lower_shadow'] >= 2 * df['body_size']) & 
        (df['upper_shadow'] <= df['body_size'] * 1) & 
        (df['body_size'] > 0) &
        (df['confirmed_bull']) &
        (df['rsi'] > 35)                
    )
    
    signals['hammer'] = df[df['hammer'] == True]['datetime'].tolist()
    #Чистка
    #df_copy.drop(columns=["bearish_engulfing", 'bullish_engulfing', 'hammer', 'rsi', 'sma_200'], inplace=True)
    
    return signals


def save_pattern_plot(plot_df, ticker_name):
    #Подготовка данных для mplfinance
    plot_df = plot_df.copy()
    plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
    plot_df.set_index('datetime', inplace=True)
    
    plot_df['marker'] = np.nan
    plot_df['marker_down'] = np.nan

    condition = (plot_df['close'] > 0) # Здесь подставь свое условие паттерна
    
    bullish_mask = (plot_df['hammer'] == True) | (plot_df['bullish_engulfing'] == True)
    plot_df.loc[bullish_mask, 'marker_up'] = plot_df['low'] * 0.998

    bearish_mask = (plot_df["bearish_engulfing"] == True)
    plot_df.loc[bearish_mask, 'marker_down'] = plot_df['high'] * 1.002

    # Дополнительный график (маркеры)
    apds = []
    if plot_df['marker_up'].notna().any():
        apds.append(mpf.make_addplot(plot_df['marker_up'], type='scatter', markersize=100, marker='^', color='green'))
    if plot_df['marker_down'].notna().any():
        apds.append(mpf.make_addplot(plot_df['marker_down'], type='scatter', markersize=100, marker='v', color='red'))

    # Сохраняем в файл
    file_path = f"{ticker_name}.png"
    
    mpf.plot(plot_df, 
             type='candle', 
             style='charles', 
             title=f"Pattern Detection: {ticker_name}",
             ylabel='Price (RUB)',
             addplot=apds,
             savefig=file_path) #PNG
    
    print(f"График сохранен: {file_path}")
    print(plot_df)
    return file_path

if __name__ == "__main__":

    df = get_candles("SBER", tf="1D", days_needed=50)

    found = detect_patterns(df)
    if found:
        print("Найдены паттерны Поглощения:")
        print(found)
    else:
        print("На данном отрезке паттернов Поглощения не обнаружено.")

    save_pattern_plot(df, "SBER")

