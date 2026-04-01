from abc import ABC, abstractmethod
import pandas as pd
import time
from datetime import datetime, timedelta
from moexalgo import Ticker
import random
import pandas_ta as ta
import mplfinance as mpf
import numpy as np
import asyncio
from aiogram import Bot
from aiogram.types import FSInputFile
from aiogram.client.session.aiohttp import AiohttpSession

class TelegramNotifier:
    def __init__(self, token, timeframe_chats, main_chat_id):
        session = AiohttpSession(proxy="http://127.0.0.1:12334")
        
        # 2. Передаем сессию в объект Bot
        self.bot = Bot(token=token, session=session)
        self.timeframe_chats = timeframe_chats
        self.main_chat_id = main_chat_id
        
    async def send_signal(self, pattern_name, ticker, photo_path, timeframe):
        """Отправляет фото графика с описанием паттерна"""
        
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return
        
        tv_url = f"https://tradingview.com:{ticker}"
        caption = (
        f"🚨 <b>{pattern_name}</b> [{timeframe}]\n\n"
        f"Акция: <b>{ticker}</b>\n"
        f"🕒 ТФ: {timeframe}\n"
        f"📅 {datetime.now().strftime('%d.%m %H:%M')}\n\n"
        f"🔗 <a href='{tv_url}'>Открыть график на TradingView</a>")
        
        # Подготовка файла для отправки в aiogram 3.x
        photo = FSInputFile(photo_path)
        
        try:
            await self.bot.send_photo(
                chat_id=self.main_chat_id,
                message_thread_id=chat_id,
                photo=photo,
                caption=caption,
                parse_mode="HTML"
            )
            print(f"Уведомление по {ticker} отправлено в Telegram")
        except Exception as e:
            print(f"Ошибка отправки в Telegram: {e}")
        #finally:
            #await self.bot.session.close() # Важно закрывать сессию


class candel(ABC):
    name: str
    def __init__(self, name):
        self.name = name
        
    def trend(self, df, window=10):
        df['sma_trend'] = df['close'].rolling(window=window).mean()
    
        df['sma_up'] = df['sma_trend'] > df['sma_trend'].shift(1)
        df['sma_down'] = df['sma_trend'] < df['sma_trend'].shift(1)
    
        #df['strong_down_trend'] = (df['close'] < df['sma_trend']) & (df['sma_down'])
        #df['strong_up_trend'] = (df['close'] > df['sma_trend']) & (df['sma_up'])
        df.drop(columns=["sma_trend"], inplace=True)
        return df
        
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
        
        # Середина тела
        df['mid_body'] = (df['open'] + df['close']) / 2

    
        df['gap_down'] = (df['close'] < df['close'].shift(1))
        df['gap_up'] = df['open'].shift(-1) > df['close']
        
        df['high_body'] = df[['open', 'close']].max(axis=1)
        df['low_body'] = df[['open', 'close']].min(axis=1)
        
        df['rsi'] = ta.rsi(df['close'], length=14)
    
        self.size_bodies(df)
        self.trend(df)
        
        return df
     
    @abstractmethod  
    def check_pattorn(self, df):
        pass
    
    def draw(self, df: pd.DataFrame):
        
        signals = self.check_pattorn(df)
        plot_df = df.copy()
        plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
        plot_df.set_index('datetime', inplace=True)
    
        plot_df['marker_up'] = np.nan
        plot_df['marker_down'] = np.nan
        
        plot_df.loc[signals.values, 'marker_up'] = plot_df['low'] * 0.998
        #plot_df.loc[signals.values, 'marker_down'] = plot_df['high'] * 1.022
        
        view_plot = plot_df.iloc[-30:]
        apds = []
        if view_plot['marker_up'].notna().any():
            apds.append(mpf.make_addplot(view_plot['marker_up'], type='scatter', 
                                         markersize=120, marker='^', color='green'))
        
        file_name = f"Trading/Graf/{self.name}_chart.png"
        mpf.plot(view_plot.iloc[-30:], type='candle', style='charles',
                 title=f"Pattern: {self.name}",
                 addplot=apds, savefig=file_name)
        
        print(f"✅ График для {self.name} сохранен в {file_name}")
        return file_name
    
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
        (df_copy['rsi'] < 30 ))

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
        (df_copy['is_big_body']) &
        (df_copy['sma_down'])    
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
        (df_copy['is_big_body'])&
        (df_copy['sma_up']))
        
class Morning_star(candel):
    def __init__(self):
        super().__init__('Morning_star')
        
    def check_pattorn(self, df):
        df_copy = super().parametrs(df.copy())
        
        return ((df_copy['is_bullish'])&
        (df_copy['is_small_body'].shift(1))&
        (df_copy['is_bearish'].shift(2))&
        (df_copy['is_big_body'].shift(2))&
        (df_copy['gap_down'].shift(1)) &
        (df_copy['sma_down'])
        )
        
class Evening_star(candel):
    def __init__(self):
        super().__init__('Evening_Star')
        
    def check_pattorn(self, df: pd.DataFrame):
        df_cope = super().parametrs(df.copy())
        return (
            (df_cope['is_bearish']) &
            (df_cope['is_small_body'].shift(1)) &         
            (df_cope['is_bullish'].shift(2)) &            
            (df_cope['is_big_body'].shift(2)) &           
            (df_cope['close'] < df_cope['open'].shift(2)) &     
            (df_cope['sma_up'])&
            (df_cope['gap_up'])                        
        )

class Bullish_harami(candel):
    def __init__(self):
        super().__init__('Bullish_Harami')
        
    def check_pattorn(self, df: pd.DataFrame):
        d = self.parametrs(df.copy())
        return (
            (d['is_bearish'].shift(1)) &             
            (d['is_bullish']) &                    
            (d['is_big_body'].shift(1)) &           
            (d['is_small_body']) &
            (d['high_body'] < d['high_body'].shift(1)) &
            (d['low_body'] > d['low_body'].shift(1)) &
            (d['sma_down']) &                      
            (d['confirmed_bull'])            
        )

class Bearish_harami(candel):
    def __init__(self):
        super().__init__('Bearish_Harami')
        
    def check_pattorn(self, df: pd.DataFrame):
        d = self.parametrs(df.copy())
        return (
            (d['is_bullish'].shift(1)) &             
            (d['is_bearish']) &                     
            (d['is_big_body'].shift(1)) &
            (d['is_small_body']) &
            (d['high_body'] < d['high_body'].shift(1)) &
            (d['low_body'] > d['low_body'].shift(1)) &
            (d['sma_up']) &                         
            (d['confirmed_bear'])                   
        )

class Dark_cloud_cover(candel):
    def __init__(self):
        super().__init__('Dark_Cloud_Cover')
        
    def check_pattorn(self, df: pd.DataFrame):
        d = super().parametrs(df.copy())
        return (
            (d['is_bearish']) &
            (d['is_bullish'].shift(1)) &
            (d['is_big_body'].shift(1)) &
            (d['open'] > d['high'].shift(1)) &
            (d['close'] < d['mid_body'].shift(1)) &
            (d['close'] > d['open'].shift(1)) & 
            (d['sma_up'])                           
        )
        
class Three_white_soldiers(candel):
    def __init__(self):
        super().__init__('Three_White_Soldiers')
        
    def check_pattorn(self, df: pd.DataFrame):
        d = super().parametrs(df.copy())
        return (
            (d['is_bullish']) & (d['is_bullish'].shift(1)) & (d['is_bullish'].shift(2)) &
            (d['is_big_body']) & (d['is_big_body'].shift(1)) & (d['is_big_body'].shift(2)) &
            (d['close'] > d['close'].shift(1)) &
            (d['close'].shift(1) > d['close'].shift(2)) &
            (d['open'] > d['open'].shift(1)) &
            (d['open'] < d['close'].shift(1)) &
            (d['upper_shadow'] < d['body_size'] * 0.2)
        )


#Получаем данные
async def get_candles(ticker_name, interval='1h', limit = 100, retries=3):
        
    base_delay = 2 #начальная задержка
    
    for attempt in range(retries):  
              
        try:
            tc = Ticker(ticker_name)
            
            end_date = datetime.now()
            if interval == '1min':
                start_date = end_date - timedelta(minutes=limit * 2) #запас, так как не учтено время биржы
            elif interval == '15min':
                start_date = end_date - timedelta(minutes=limit * 20)
            elif interval == '1h':
                start_date = end_date - timedelta(days=limit // 6)
            else:
                start_date = end_date - timedelta(days=limit * 2)
                
            data = tc.candles(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), period=interval)

            df = pd.DataFrame(data)

            if df.empty:
                print(f"[!] Данные для {ticker_name} отсутствуют (пустой ответ).")
                return pd.DataFrame()
            
            #Если свечей больше limit
            df = df.tail(limit)
            df = df.rename(columns={'begin': 'datetime'})
                
            return df
        
        except Exception as e:
            
            print(f"ERROR: {e}")
            
            if "not found" in str(e) or "NoneType" in str(e):
                print(f"[!] Тикер {ticker_name} неактивен или данные недоступны.")
                return pd.DataFrame()

            jitter = random.uniform(0, 1) 
            wait_time = (base_delay * (2 ** attempt)) + jitter
            print(f"[*] Сбой сети при загрузке {ticker_name}. Попытка {attempt + 1}/{retries}...")
            await asyncio.sleep(wait_time)
                
    return pd.DataFrame()


async def scan(tickers, patterns, notifier, time_frame='1h'):

    print(f"🚀 Запуск сканера подтвержденных сигналов...")
    
    for ticker in tickers:
        df = await get_candles(ticker, interval=time_frame, limit=80)
        
        if df.empty:
            continue
              
        for pattern in patterns:
            signals = pattern.check_pattorn(df)
            if signals.iloc[-2] == True:
                print(f"🚨 СИГНАЛ! {pattern.name} по {ticker}")
                
                chart_path = pattern.draw(df)
                await notifier.send_signal(f"{pattern.name} (Подтвержден)", ticker, chart_path, time_frame)
        await asyncio.sleep(0.5)
        
    print(f'{time_frame} проверен!')
    
async def multiscan(tickers, patterns, notifier):
    targets = list(notifier.timeframe_chats.keys())
    
    tasks = []
    for tf in targets:
        tasks.append(scan(tickers, patterns, notifier, time_frame=tf))
        
    await asyncio.gather(*tasks)
    print('И всё...')
                
async def main():
    Token = "8715766790:AAFQd7LOY2qOqvxgTaMKz7rJbEuM9t5VrZc"
    CHATS = {
        '15min': 3851510557, # ID чата для 15-минут
        '1h': 3851510557,  # ID чата для часа
        '1D': 3851510557    # ID чата для дня
    }
    my_tickers = ['SBER', 'GAZP', 'LKOH', 'NVTK', 'MGNT', 'ROSN', 'T', 'IMOEX']
    my_patterns = [
        Hammer(), 
        Bullish_engulfing(),
        Bullish_harami(),
        Morning_star(),
        Dark_cloud_cover(),
        Three_white_soldiers(),
        Bearish_engulfing(),
        Bearish_harami(),
        Evening_star()
    ]
    
    main_chat_id = -1003851510557
    #notifier = TelegramNotifier("8715766790:AAFQd7LOY2qOqvxgTaMKz7rJbEuM9t5VrZc", 5595690153)
    notifier = TelegramNotifier(Token, CHATS, main_chat_id)
    
    print("Бот-сканер запущен в режиме сервера.")
    print("Для ручной остановки нажмите Ctrl+C")

    try:
        while True:
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"[{current_time}] Начинаю плановое сканирование...")
            
            await multiscan(my_tickers, my_patterns, notifier)

            wait_time = 300
            print(f"Сканирование окончено. Сон {wait_time//60} мин...")
            await asyncio.sleep(wait_time)
            
    except asyncio.CancelledError:
        print("Задача сканирования отменена.")
    except KeyboardInterrupt:
        print("Ручная остановка.")
    finally:
        await notifier.bot.session.close()
        print("Сессия бота закрыта. Бот полностью остановлен.")
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass