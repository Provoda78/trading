import pandas as pd
from datetime import datetime
import mplfinance as mpf
import numpy as np
import asyncio
from aiogram import Bot
from aiogram.types import FSInputFile
from aiogram.client.session.aiohttp import AiohttpSession

from Get import get_candles

class LevelsTelegrammNotifier:
    def __init__(self, token: int, timeframe_chats: list, main_chat_id: int):
        session = AiohttpSession(proxy="http://127.0.0.1:12334")
        
        #Передаем сессию в объект Bot
        self.bot = Bot(token=token, session=session)
        self.timeframe_chats = timeframe_chats
        self.main_chat_id = main_chat_id
        
    async def send_signal_levels(self, ticker: str, 
                                 photo_path: str, 
                                 timeframe: str, 
                                 supports:dict, 
                                 resistances: dict):
        
        chat_id = self.timeframe_chats.get(timeframe)
        if not chat_id:
            print(f"⚠️ Чат для таймфрейма {timeframe} не настроен!")
            return
        
        s_points = [str(v[0]) for v in supports.values()]
        s_prices = [str(p) for p in supports.keys()]
        r_points = [str(v[0]) for v in resistances.values()]
        r_prices = [str(p) for p in resistances.keys()]
        
        score = ""
        if s_points:
            score += f"Цены:  {s_prices} 🟢 Поддержка: {', '.join(s_points)} б.\n"   
        if r_points:
            score += f"Цены: {r_prices} 🔴 Сопротивление: {', '.join(r_points)} б.\n"
            
        tv_url = f"https://tradingview.com/symbols/RUS-{ticker}/"
        caption = (
        f"🚨 <b>Обнаружены уровни!</b>\n\n"
        f"Акция: <b>{ticker}</b>\n"
        f"{score}\n"
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
            print(f"Уведомление по {ticker} отправлено в Telegram. id группы {chat_id}")
        except Exception as e:
            print(f"Ошибка отправки в Telegram: {e}")

class LevelDetector:
    def __init__(self, window: int, sensitivity: float):
        self.window = window
        self.sensitivity = sensitivity
        
    def get_levels(self, df: pd.DataFrame) -> tuple[dict, dict]:
        
        df['is_min'] = df['low'] == df['low'].rolling(window=self.window*2+1, center=True).min()
    
        df['is_max'] = df['high'] == df['high'].rolling(window=self.window*2+1, center=True).max()
    
        supports = df[df['is_min']]['low'].reset_index().values.tolist() #[[index, level], ...]
        resistances = df[df['is_max']]['high'].reset_index().values.tolist()
    
        score_supports = self.cluster_levels(supports)
        score_resistances = self.cluster_levels(resistances)
        
        return self.find_mirro(score_supports, score_resistances)
    
    def cluster_levels(self, points: list) -> list:
        
        levels = {}
        
        for i, point in points:
            found_nearby = False
            for level_price in list(levels.items()):
                if abs(point - level_price[0]) / level_price[0] <= self.sensitivity:
                    levels[level_price[0]][0] += 1
                    levels[level_price[0]][1] = i
                    found_nearby = True
                    break
            
            if not found_nearby:
                #[1 касание, индекс этой свечи]
                levels[point] = [1, i]
            
        #{цена: [баллы, последний_индекс]}
        return {p: [t[0] // 2, t[1]] for p, t in levels.items() if t[0] >= 2}
    
    def find_mirro(self, score1: dict, score2: dict) -> tuple[dict, dict]:
        
        #разные типы (поддержка / сопротивление)
        levels1 = list(score1.keys())
        levels2 = list(score2.keys())
            
        s1 = score1.copy()
        s2 = score2.copy()

        
        for p1 in levels1:
            for p2 in levels2:
                #учитывание погрешности
                if abs(p1 - p2) / p1 <= self.sensitivity:
                    total_score = s1[p1][0] + s2[p2][0] + 2
                    
                    if s1[p1][1] > s2[p2][1]:
                        s1[p1][0] = total_score
                        if p2 in s2: 
                            del s2[p2]
                    else:
                        s2[p2][0] = total_score
                        if p1 in s1: 
                            del s1[p1]
                    break 
        
        return s1, s2

    def prepare_hlines(self, final_levels: tuple[dict, dict]) -> dict:

        s_dict, r_dict = final_levels
    
        hlines_prices = []
        hlines_colors = []
        hlines_widths = []
        hlines_styles = []
        
        combined = [(s_dict, 'green'), (r_dict, 'red')]

        for d, color in combined:
            for price, data in d.items():
                score = data[0]

                margin = price * self.sensitivity
                
                #Верхняя граница диапазона
                hlines_prices.append(price + margin)
                hlines_colors.append(color)
                hlines_widths.append(min(score, 4))
                hlines_styles.append(':')
                
                #Центральная линия (уровень)
                hlines_prices.append(price)
                hlines_colors.append(color)
                hlines_widths.append(min(score, 4))
                hlines_styles.append('-')
                
                #Нижняя граница диапазона
                hlines_prices.append(price - margin)
                hlines_colors.append(color)
                hlines_widths.append(min(score, 4))
                hlines_styles.append(':')

        return dict(hlines=hlines_prices, 
                    colors=hlines_colors, 
                    linewidths=hlines_widths, 
                    linestyle=hlines_styles)
    
    def draw_levels(self, df: pd.DataFrame, 
                    ticker: str,
                    tf: str, 
                    final_levels: tuple[dict, dict], 
                    pattern_name="Signal") -> str:
        
        plot_df = df.iloc[-100:].copy()
        plot_df['datetime'] = pd.to_datetime(plot_df['datetime'])
        plot_df.set_index('datetime', inplace=True)

        hlines_config = self.prepare_hlines(final_levels)

        file_name = f"Trading/Graf/{ticker}_levels.png"
    
        mpf.plot(plot_df, type='candle', style='charles',
                title=f"{ticker} {tf} | Levels Analysis",
                hlines=hlines_config,
                savefig=file_name,
                tight_layout=True)
    
        return file_name
    
async def scan_levels(tickers: list, time_frame: str, notifier):
    levels = LevelDetector(10, 0.003)
    
    for ticker in tickers:
        df = await get_candles(ticker, interval=time_frame, limit=100)
        
        supports, resistances = levels.get_levels(df)
        if len(supports) > 0 or len(resistances) > 0:
            print(f'Обнаружен уровень: [{ticker}_{time_frame}]!')
            file = levels.draw_levels(df, ticker, time_frame, (supports, resistances))
            await notifier.send_signal_levels(ticker, file, time_frame, supports, resistances)
    
    await asyncio.sleep(0.5)

async def multiscan_levels(tickers: list, notifier):
    tasks = []
    time_frames = list(notifier.timeframe_chats.keys())
    
    for tf in time_frames:
        tasks.append(scan_levels(tickers, tf, notifier))
    
    await asyncio.gather(*tasks)
    print('И всё по уровням на этом...')
    
async def main():
    Token = "8710028983:AAGv2BFxaOXZeeH389PLnhqtkOPO9WeuLnA"
    CHATS = {
        '15min': 4, # ID чата для 15-минут
        '1h': 3,  # ID чата для часа
        '1D': 2    # ID чата для дня
    }
    
    my_tickers = ['SBER', 'GAZP', 'LKOH', 'NVTK', 'MGNT', 'ROSN', 'T', 'IMOEX']
    main_chat_id = -1003763095545
    
    notifier = LevelsTelegrammNotifier(Token, CHATS, main_chat_id)
    
    print("Бот-сканер уровней запущен в режиме сервера.")
    print("Для ручной остановки нажмите Ctrl+C")
    try:
        while True:
            current_time = datetime.now().strftime('%H:%M:%S')
            print(f"Начало сканирования уровней: [{current_time}]...")
            
            await multiscan_levels(my_tickers, notifier)

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
        
    
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except:
        pass
    