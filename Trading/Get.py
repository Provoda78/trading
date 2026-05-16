from datetime import datetime, timedelta
from moexalgo import Ticker
import random
import pandas as pd
import asyncio

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