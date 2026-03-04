from abc import ABC, abstractmethod
import pandas as pd
import time
from datetime import datetime, timedelta
from moexalgo import Ticker
import random
import pandas_ta as ta
import mplfinance as mpf
import numpy as np

class candel(ABC):
    name: str
    def __init__(self, name):
        self.name = name
        
    def size_bodies(self, df: pd.DataFrame):
    
        def count_bigger(x):
            current_body = x.iloc[-1]
            previous_bodies = x.iloc[:-1]
            bigger_count = (previous_bodies > current_body).sum()
            return bigger_count
    
        def count_smaller(x):
            current_body = x.iloc[-1]
            previous_bodies = x.iloc[:-1]
            bigger_count = (previous_bodies < current_body).sum()
            return bigger_count
        
        df['is_small_body'] = df['body_size'].rolling(window=11).apply(count_bigger, raw=False) >= 6
        df['is_big_body'] = df['body_size'].rolling(window=11).apply(count_smaller, raw=False) >= 6
    
    def parametrs(self, df: pd.DataFrame):
        #Размеры свечей
        df['body_size'] = abs(df['close'] - df['open'])
        df['is_bullish'] = df['close'] > df['open']
        df['is_bearish'] = df['close'] < df['open']
    
        #Тени свечей
        df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
        df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
    
        #Подтверждение
        df['confirmed_bull'] = (df['close'].shift(-1) > df['close'])
        df['confirmed_bear'] = (df['close'].shift(-1) < df['close'])
    
        df['gap_down'] = (df['close'] < df['close'].shift(1))
        #df_copy['gap_up'] = [df_copy['open'].shift(-1) > df_copy['close']]
        
        df['rsi'] = ta.rsi(df['close'], length=14)
    
        self.size_bodies(df)
        #self.trend(df)
        
        return df
     
    @abstractmethod  
    def check_pattorn(self, df):
        pass
    
    def draw(self, df: pd.DataFrame):
        plot_df = df.copy()
        plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
        plot_df.set_index('datetime', inplace=True)
    
        plot_df['marker_up'] = np.nan
        plot_df['marker_down'] = np.nan
        
        plot_df.loc[self.check_pattorn(plot_df).values, 'marker_up'] = plot_df['low'] * 0.988
        
        apds = []
        if plot_df['marker_up'].notna().any():
            apds.append(mpf.make_addplot(plot_df['marker_up'], type='scatter', 
                                         markersize=120, marker='^', color='green'))
        
        file_name = f"Trading/Graf/{self.name}_chart.png"
        mpf.plot(plot_df, type='candle', style='charles',
                 title=f"Pattern: {self.name}",
                 addplot=apds, savefig=file_name)
        
        print(f"✅ График для {self.name} сохранен в {file_name}")
    
class Hammer(candel):
    def __init__(self):
        super().__init__('Hammer')
        
    def check_pattorn(self, df: pd.DataFrame):
        df_copy = super().parametrs(df.copy())
        
        return ((df_copy['lower_shadow'] >= 2 * df_copy['body_size']) & 
        (df_copy['upper_shadow'] <= df_copy['body_size'] * 1) & 
        (df_copy['body_size'] > 0) &
        (df_copy['confirmed_bull']) &
        (df_copy['is_small_body']) &
        (df_copy['rsi']))

class Bullish_engulfing(candel):
    def __init__(self):
        super().__init__('bullish_engulfing')
        
    def check_pattorn(self, df: pd.DataFrame):
        df_copy = super().parametrs(df.copy())
        
        return ((df_copy['is_bullish']) & 
        (df_copy['is_bearish'].shift(1)) & 
        (df_copy['open'] <= df_copy['close'].shift(1)) & 
        (df_copy['close'] >= df_copy['open'].shift(1)) &
        (df_copy['body_size'] > df_copy['body_size'].shift(1)) &
        (df_copy['confirmed_bull']) &
        (df_copy['is_big_body']) 
        #(df_copy['sma_down'])    
        )
        
class Bearish_engulfing(candel):
    def __init__(self):
        super().__init__('bearish_engulfing')
        
    def check_pattorn(self, df: pd.DataFrame):
        df_copy = super().parametrs(df.copy())
        
        return ((df_copy['is_bearish']) & 
        (df_copy['is_bullish'].shift(1)) & 
        (df_copy['open'] >= df_copy['close'].shift(1)) & 
        (df_copy['close'] <= df_copy['open'].shift(1)) &
        (df_copy['body_size'] > df_copy['body_size'].shift(1)) &
        (df_copy['confirmed_bear']) &
        (df_copy['is_big_body']))
        
class Morning_star(candel):
    def __init__(self):
        super().__init__('Morning_star')
        
    def check_pattorn(self, df):
        df_copy = super().parametrs(df.copy())
        
        return ((df_copy['is_bullish'])&
        (df_copy['is_small_body'].shift(1))&
        (df_copy['is_bearish'].shift(2))&
        (df_copy['is_big_body'].shift(2))&
        (df_copy['gap_down'].shift(1))
        #(df_copy['sma_down'])
        )
        
#Получаем данные
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
            
            #print(f"ERROR: {e}")
            
            if "not found" in str(e) or "NoneType" in str(e):
                print(f"[!] Тикер {ticker_name} неактивен или данные недоступны.")
                return pd.DataFrame()

            jitter = random.uniform(0, 1) 
            wait_time = (base_delay * (2 ** attempt)) + jitter
            print(f"[*] Сбой сети при загрузке {ticker_name}. Попытка {attempt + 1}/{retries}...")
            time.sleep(wait_time)
                
    return pd.DataFrame()

Hammer_ = Hammer()
Bull = Bullish_engulfing()
Star = Morning_star()
df = get_candles("SBER", tf="1D", days_needed=100)
print(Hammer_.check_pattorn(df))
if not df.empty:
    Hammer_.draw(df)
    Bull.draw(df)
    Star.draw(df)