import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from moexalgo import Ticker
from datetime import datetime, timedelta
import requests
import io
import time
import logging


# ============================================================
# НАСТРОЙКИ
# ============================================================
TELEGRAM_TOKEN  = "8687255977:AAGIA51KSFbWord-vIqz7pPju2xIo3ojjjU"
TELEGRAM_CHAT_ID = "1316279449"

TICKER_NAME     = 'BRH6'
TIMEFRAME       = '15min'         # таймфрейм свечи
TF_SECONDS      = 15 * 60        # длина свечи в секундах (15 мин = 900 сек)
POLL_INTERVAL   = 15             # как часто проверяем закрытие свечи (сек)
CONTEXT_CANDLES = 60             # свечей в окне контекста на графике
LOAD_DAYS       = 7              # глубина загрузки истории

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%d.%m.%Y %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('alma_bot.log', encoding='utf-8')
    ]
)
log = logging.getLogger(__name__)


# --- ЗАГРУЗКА ДАННЫХ ---
def get_moex_data(ticker_symbol, days=LOAD_DAYS, tf=TIMEFRAME):
    t = Ticker(ticker_symbol)
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=days)
    candles    = t.candles(start=start_date, end=end_date, period=tf)
    df         = pd.DataFrame(candles)

    if df.empty:
        return None

    df['begin'] = pd.to_datetime(df['begin'])
    df.rename(columns={'begin': 'datetime'}, inplace=True)
    return df


# --- ALMA + WAVETREND ---
def calculate_alma(series, window=9, offset=0.85, sigma=6):
    m = offset * (window - 1)
    s = window / sigma
    weights = np.exp(-((np.arange(window) - m) ** 2) / (2 * s ** 2))
    weights /= weights.sum()
    return series.rolling(window).apply(lambda x: (x * weights).sum(), raw=True)


def apply_strategy(df):
    n1, n2 = 14, 21
    ap  = (df['high'] + df['low'] + df['close']) / 3
    esa = ap.ewm(span=n1, adjust=False).mean()
    d   = abs(ap - esa).ewm(span=n1, adjust=False).mean()
    ci  = (ap - esa) / (0.015 * d)

    df['wt1'] = ci.ewm(span=n2, adjust=False).mean()
    df['wt2'] = calculate_alma(df['wt1'], window=4, offset=0.85, sigma=6)

    df['buy_sig']  = (df['wt1'] > df['wt2']) & (df['wt1'].shift(1) <= df['wt2'].shift(1))
    df['sell_sig'] = (df['wt1'] < df['wt2']) & (df['wt1'].shift(1) >= df['wt2'].shift(1))
    return df


# --- ПОСТРОЕНИЕ ГРАФИКА ---
def build_chart(df, ticker, signal_idx, signal_type):
    start = max(0, signal_idx - CONTEXT_CANDLES)
    end   = min(len(df), signal_idx + 10)
    ctx   = df.iloc[start:end].copy()

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.05, row_heights=[0.7, 0.3])

    fig.add_trace(go.Candlestick(
        x=ctx['datetime'], open=ctx['open'], high=ctx['high'],
        low=ctx['low'],    close=ctx['close'], name='Свечи'
    ), row=1, col=1)

    sig_time = df.iloc[signal_idx]['datetime']
    fig.add_vline(x=sig_time, line_width=2,
                  line_color='lime' if signal_type == 'BUY' else 'tomato',
                  line_dash='dot')

    buys  = ctx[ctx['buy_sig']]
    sells = ctx[ctx['sell_sig']]

    if not buys.empty:
        fig.add_trace(go.Scatter(
            x=buys['datetime'], y=buys['low'] * 0.999,
            mode='markers', marker=dict(symbol='triangle-up', size=15, color='lime'),
            name='BUY'
        ), row=1, col=1)

    if not sells.empty:
        fig.add_trace(go.Scatter(
            x=sells['datetime'], y=sells['high'] * 1.001,
            mode='markers', marker=dict(symbol='triangle-down', size=15, color='tomato'),
            name='SELL'
        ), row=1, col=1)

    fig.add_trace(go.Scatter(x=ctx['datetime'], y=ctx['wt1'],
                             line=dict(color='deepskyblue', width=1.5), name='WT1'), row=2, col=1)
    fig.add_trace(go.Scatter(x=ctx['datetime'], y=ctx['wt2'],
                             line=dict(color='orange', width=1.5, dash='dot'),
                             name='WT2 (ALMA)'), row=2, col=1)

    for level, color in [(60, 'tomato'), (-60, 'lime'), (0, 'gray')]:
        fig.add_hline(y=level, line_dash='dash', line_color=color, opacity=0.5, row=2, col=1)

    emoji = '🟢' if signal_type == 'BUY' else '🔴'
    fig.update_layout(
        title=f'{emoji} {signal_type} | {ticker} | {sig_time.strftime("%d.%m.%Y %H:%M")}',
        xaxis_rangeslider_visible=False,
        height=700, width=1100,
        template='plotly_dark'
    )
    return fig.to_image(format='png', scale=1.5)


# --- ТЕКСТ СООБЩЕНИЯ ---
def build_message(df, ticker, signal_idx, signal_type):
    row    = df.iloc[signal_idx]
    prev   = df.iloc[max(0, signal_idx - 1)]
    emoji  = '🟢 ПОКУПКА' if signal_type == 'BUY' else '🔴 ПРОДАЖА'
    change = (row['close'] - prev['close']) / prev['close'] * 100

    ctx_rows = df.iloc[max(0, signal_idx - 4): signal_idx + 1]
    candle_lines = '\n'.join(
        f"  {r['datetime'].strftime('%H:%M')}  "
        f"O:{r['open']:.2f} H:{r['high']:.2f} L:{r['low']:.2f} C:{r['close']:.2f}"
        for _, r in ctx_rows.iterrows()
    )

    wt1_status = ('перекуплен ⚠️' if row['wt1'] > 60
                  else 'перепродан ⚠️' if row['wt1'] < -60
                  else 'нейтральная зона')

    return (
        f"*{emoji}*\n"
        f"📌 Тикер: `{ticker}`\n"
        f"🕐 Время: `{row['datetime'].strftime('%d.%m.%Y %H:%M')}`\n"
        f"💰 Цена закрытия: `{row['close']:.2f}`\n"
        f"📈 Изменение к пред. свече: `{change:+.2f}%`\n"
        f"〰️ WT1: `{row['wt1']:.2f}` ({wt1_status})\n"
        f"〰️ WT2 (ALMA): `{row['wt2']:.2f}`\n"
        f"\n📊 *Последние 5 свечей:*\n```\n{candle_lines}\n```"
    )


# --- ОТПРАВКА ТЕКСТА В ТЕЛЕГРАМ ---
def send_text(text):
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'Markdown'}
    try:
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        log.info("✅ Текстовое сообщение отправлено")
    except requests.RequestException as e:
        log.error(f"❌ Ошибка отправки текста: {e}")


# --- ОТПРАВКА ФОТО В ТЕЛЕГРАМ ---
def send_telegram(text, image_bytes):
    url   = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {'photo': ('chart.png', io.BytesIO(image_bytes), 'image/png')}
    data  = {'chat_id': TELEGRAM_CHAT_ID, 'caption': text, 'parse_mode': 'Markdown'}
    try:
        resp = requests.post(url, files=files, data=data, timeout=30)
        resp.raise_for_status()
        log.info("✅ Уведомление отправлено в Telegram")
    except requests.RequestException as e:
        log.error(f"❌ Ошибка отправки: {e}")


# --- ПРИВЕТСТВЕННОЕ СООБЩЕНИЕ ---
def send_welcome():
    now        = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    wait       = seconds_to_next_candle()
    next_candle = (datetime.now() + timedelta(seconds=wait)).strftime('%H:%M:%S')

    text = (
        f"🤖 *ALMA + WaveTrend бот запущен*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📌 Тикер: `{TICKER_NAME}`\n"
        f"⏱ Таймфрейм: `{TIMEFRAME}`\n"
        f"🕐 Время запуска: `{now}`\n"
        f"⏳ Следующая свеча закроется в: `{next_candle}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Бот будет присылать сигналы сразу после закрытия свечи.\n"
        f"Для остановки нажмите Ctrl+C."
    )
    send_text(text)


# --- ВЫЧИСЛЕНИЕ СЕКУНД ДО ЗАКРЫТИЯ СЛЕДУЮЩЕЙ СВЕЧИ ---
def seconds_to_next_candle(tf_seconds=TF_SECONDS):
    now     = datetime.now()
    elapsed = (now.minute * 60 + now.second) % tf_seconds
    return tf_seconds - elapsed


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
def run_bot():
    log.info(f"🚀 Бот запущен | Тикер: {TICKER_NAME} | Таймфрейм: {TIMEFRAME}")
    send_welcome()

    # Множество уже обработанных сигналов: (datetime_str, signal_type)
    # Сохраняем только за последние сутки, чтобы не разрастался
    processed: set = set()

    while True:
        try:
            # --- Ждём закрытия свечи ---
            wait = seconds_to_next_candle()

            if wait > POLL_INTERVAL:
                # Далеко до закрытия — просто ждём
                log.info(f"⏳ До закрытия свечи: {wait // 60:02d}:{wait % 60:02d} мин")
                time.sleep(min(wait - POLL_INTERVAL, 60))
                continue

            # Осталось мало — ждём точного момента закрытия
            log.info(f"⏳ Ожидание закрытия свечи через {wait} сек...")
            time.sleep(wait + 3)   # +3 сек буфер, чтобы биржа успела отдать данные

            # --- Загружаем свежие данные ---
            log.info("📥 Загрузка данных с MOEX...")
            df = get_moex_data(TICKER_NAME)

            if df is None or len(df) < 30:
                log.warning("Недостаточно данных, пропускаем итерацию")
                time.sleep(POLL_INTERVAL)
                continue

            df = apply_strategy(df)

            # --- Проверяем последнюю ЗАКРЫТУЮ свечу (предпоследняя строка) ---
            # Последняя строка df — текущая (ещё открытая) свеча, поэтому берём [-2]
            check_idx = len(df) - 2

            if check_idx < 1:
                time.sleep(POLL_INTERVAL)
                continue

            row         = df.iloc[check_idx]
            candle_time = str(row['datetime'])

            for sig_type, sig_col in [('BUY', 'buy_sig'), ('SELL', 'sell_sig')]:
                key = (candle_time, sig_type)

                if row[sig_col] and key not in processed:
                    log.info(f"🔔 Новый сигнал: {sig_type} @ {candle_time}")

                    img  = build_chart(df, TICKER_NAME, check_idx, sig_type)
                    text = build_message(df, TICKER_NAME, check_idx, sig_type)
                    send_telegram(text, img)

                    processed.add(key)

                    # Чистим старые записи (старше суток), чтобы set не рос бесконечно
                    cutoff = datetime.now() - timedelta(days=1)
                    processed = {
                        k for k in processed
                        if pd.to_datetime(k[0]) > cutoff
                    }

        except KeyboardInterrupt:
            log.info("🛑 Бот остановлен пользователем")
            break

        except Exception as e:
            log.error(f"⚠️ Ошибка в основном цикле: {e}", exc_info=True)
            log.info(f"Повтор через {POLL_INTERVAL} сек...")
            time.sleep(POLL_INTERVAL)


# --- ЗАПУСК ---
if __name__ == "__main__":
    run_bot()