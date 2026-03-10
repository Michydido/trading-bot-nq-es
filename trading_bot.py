#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BOT DI TRADING PER NASDAQ E S&P500 - VERSIONE CON NEWS DA CSV
- IVFG integrate come confluences
- Stop loss dinamico basato su livelli reali
- Dati reali da iTick (REST polling)
- Calendario news da file CSV (aggiornato automaticamente da GitHub Actions)
- Backtesting integrato
- Esecuzione ordini su TradeLocker (simulata se non connesso)
- Orario UTC-5 (New York)
"""

import os
import sys
import json
import time
import logging
import threading
import queue
import asyncio
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, time as dtime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------------------
# CONFIGURAZIONE (parametri modificabili dall'utente)
# ------------------------------------------------------------------------------
class Config:
    SYMBOLS = ['NQ', 'ES']
    
    # Orari di trading (New York)
    PREMARKET_START = dtime(8, 30)
    PREMARKET_END = dtime(9, 30)
    MAIN_START = dtime(9, 30)
    MAIN_END = dtime(10, 0)
    POST_START = dtime(10, 0)
    POST_END = dtime(10, 30)
    CLOSE_NONEWS = dtime(14, 0)
    CLOSE_NEWS = dtime(11, 0)
    
    # Timeframes in minuti
    TF_1M = 1
    TF_1H = 60
    TF_4H = 240
    
    # Rischio e money management
    RISK_PER_TRADE = 0.01          # 1% per trade singolo
    RISK_PER_TRADE_DUAL = 0.005     # 0.5% per indice se due trade contemporanei
    MAX_DAILY_LOSS = 0.015           # 1.5%
    INITIAL_CAPITAL = 100000
    
    # Parametri strategia
    MIN_CONFLUENCES = 2              # numero minimo di confluences per entrare
    STOP_LOSS_OFFSET = 5              # punti di fallback se non si trova stop migliore
    IVFG_AS_LAST = True               # se False, le IVFG non possono essere l'ultima confluence
    
    # Valore per punto dei futures
    POINT_VALUE = {'NQ': 20, 'ES': 50}
    SWEEP_TOLERANCE = 0.1
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'trading_bot.log')
    KILL_SWITCH = False
    
    # API Keys (da .env)
    ITICK_TOKEN = os.getenv('ITICK_TOKEN', '')
    TL_ENVIRONMENT = os.getenv('TL_ENVIRONMENT', '')
    TL_USERNAME = os.getenv('TL_USERNAME', '')
    TL_PASSWORD = os.getenv('TL_PASSWORD', '')
    TL_SERVER = os.getenv('TL_SERVER', '')

# ------------------------------------------------------------------------------
# LOGGING (con supporto Unicode per console Windows)
# ------------------------------------------------------------------------------
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(Config.LOG_LEVEL)
    if logger.handlers:
        return logger
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = setup_logger('TradingBot')

# ------------------------------------------------------------------------------
# TRADELOCKER CLIENT
# ------------------------------------------------------------------------------
try:
    from tradelocker import TLAPI
except ImportError:
    logger.error("Libreria tradelocker non installata. Eseguire: pip install tradelocker")
    sys.exit(1)

class TradeLockerClient:
    def __init__(self):
        self.logger = logging.getLogger('TradeLocker')
        self.tl = None
        self.connected = False
        self.instrument_ids = {}
        
    def connect(self):
        if not all([Config.TL_ENVIRONMENT, Config.TL_USERNAME, Config.TL_PASSWORD, Config.TL_SERVER]):
            self.logger.warning("Credenziali TradeLocker mancanti, modalità simulata")
            return False
        try:
            self.tl = TLAPI(
                environment=Config.TL_ENVIRONMENT,
                username=Config.TL_USERNAME,
                password=Config.TL_PASSWORD,
                server=Config.TL_SERVER
            )
            self.connected = True
            self.logger.info(f"✅ Connesso a TradeLocker ({Config.TL_ENVIRONMENT})")
            self._map_instruments()
            return True
        except Exception as e:
            self.logger.error(f"❌ Connessione fallita: {e}")
            return False
    
    def _map_instruments(self):
        try:
            df = self.tl.get_all_instruments()
            if df.empty:
                self.logger.warning("Nessuno strumento trovato")
                return
            for sym in Config.SYMBOLS:
                mask = df['symbol'].str.contains(sym, case=False, na=False)
                if mask.any():
                    row = df.loc[mask].iloc[0]
                    self.instrument_ids[sym] = int(row['tradableInstrumentId'])
                    self.logger.info(f"   {sym} -> ID {self.instrument_ids[sym]}")
                else:
                    self.logger.warning(f"Simbolo {sym} non trovato")
        except Exception as e:
            self.logger.error(f"Errore mapping strumenti: {e}")
    
    def place_order(self, symbol: str, side: str, quantity: int) -> bool:
        if not self.connected or symbol not in self.instrument_ids:
            self.logger.error("Non connesso o simbolo non mappato")
            return False
        try:
            order_id = self.tl.create_order(
                instrument_id=self.instrument_ids[symbol],
                quantity=float(quantity),
                side=side,
                type_='market'
            )
            self.logger.info(f"📈 Ordine inviato: {side.upper()} {quantity} {symbol} ID={order_id}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Errore invio ordine: {e}")
            return False
    
    def disconnect(self):
        self.connected = False
        self.logger.info("Disconnesso da TradeLocker")

# ------------------------------------------------------------------------------
# ITICK CLIENT (REST POLLING)
# ------------------------------------------------------------------------------
import requests

class iTickClient:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://api.itick.org"
        self.headers = {"accept": "application/json", "token": self.token}
        self.latest_quotes = {sym: None for sym in Config.SYMBOLS}
        self.historical_data = {sym: pd.DataFrame() for sym in Config.SYMBOLS}
        self.callback = None
        self.thread = None
        self.logger = logging.getLogger('iTick')
        if not token:
            self.logger.warning("Token iTick mancante, disabilitato")

    def start(self, callback):
        if not self.token:
            return
        self.callback = callback
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()
        self.logger.info("Avviato polling iTick (aggiornamento ogni 5 secondi)")

    def _poll_loop(self):
        while True:
            try:
                for sym in Config.SYMBOLS:
                    url = f"{self.base_url}/stock/quote?region=US&code={sym}"
                    resp = requests.get(url, headers=self.headers, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data and isinstance(data, dict):
                            quote_data = data.get('data')
                            if quote_data:
                                quote = {
                                    'bid': quote_data.get('b'),
                                    'ask': quote_data.get('a'),
                                    'last': quote_data.get('ld'),
                                    'volume': quote_data.get('v'),
                                    'timestamp': quote_data.get('t'),
                                    'time': datetime.now()
                                }
                                self.latest_quotes[sym] = quote
                                if self.callback:
                                    self.callback(sym, quote)
            except Exception as e:
                self.logger.error(f"Errore polling: {e}")
            time.sleep(5)

    def fetch_historical(self, symbol, days=5, interval='1m'):
        interval_map = {'1m': 60, '5m': 300, '1h': 3600, '1d': 86400}
        seconds = interval_map.get(interval, 60)
        end = datetime.now()
        start = end - timedelta(days=days)
        url = f"{self.base_url}/stock/kline"
        params = {
            "region": "US",
            "symbol": symbol,
            "kType": str(seconds),
            "st": int(start.timestamp() * 1000),
            "et": int(end.timestamp() * 1000),
            "limit": 10000
        }
        try:
            resp = requests.get(url, params=params, headers=self.headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                bars = data.get('data', [])
                if bars:
                    df = pd.DataFrame(bars)
                    df.rename(columns={
                        't': 'time', 'o': 'open', 'h': 'high',
                        'l': 'low', 'c': 'close', 'v': 'volume'
                    }, inplace=True)
                    df['time'] = pd.to_datetime(df['time'])
                    df.set_index('time', inplace=True)
                    df.sort_index(inplace=True)
                    self.historical_data[symbol] = df
                    self.logger.info(f"Scaricati {len(df)} {interval} per {symbol}")
                    return df
                else:
                    self.logger.warning(f"Nessun dato storico per {symbol}")
            else:
                self.logger.error(f"Errore API storico {symbol}: {resp.status_code}")
        except Exception as e:
            self.logger.error(f"Errore fetch storico {symbol}: {e}")
        return pd.DataFrame()

    def get_latest_price(self, symbol):
        quote = self.latest_quotes.get(symbol)
        if quote and quote.get('last'):
            return quote['last']
        return None

    def get_1m_df(self, symbol, minutes=120):
        df = self.historical_data.get(symbol, pd.DataFrame())
        if df.empty:
            return df
        now = datetime.now()
        start = now - timedelta(minutes=minutes)
        mask = df.index >= start
        recent = df.loc[mask].copy()
        price = self.get_latest_price(symbol)
        if price and (recent.empty or recent.index[-1] < now - timedelta(minutes=1)):
            recent.loc[now.replace(second=0, microsecond=0)] = {
                'open': price, 'high': price, 'low': price, 'close': price, 'volume': 0
            }
        return recent

    def stop(self):
        pass

# ------------------------------------------------------------------------------
# NEWS CALENDAR (legge da CSV generato esternamente)
# ------------------------------------------------------------------------------
class NewsCalendar:
    def __init__(self, csv_file='news.csv'):
        self.csv_file = csv_file
        self.events = []
        self.logger = logging.getLogger('NewsCalendar')
        self.load()
        
    def load(self):
        """Carica gli eventi dal file CSV."""
        try:
            df = pd.read_csv(self.csv_file)
            df['date'] = pd.to_datetime(df['date']).dt.date
            self.events = df.to_dict('records')
            self.logger.info(f"✅ Caricati {len(self.events)} eventi da {self.csv_file}")
        except Exception as e:
            self.logger.error(f"❌ Errore caricamento {self.csv_file}: {e}")
            self.events = []

    def get_day_type(self, date: datetime.date) -> str:
        """Restituisce 'news' se c'è un evento high/medium che non è non_news."""
        for ev in self.events:
            if ev['date'] == date:
                if ev['importance'] in ('high', 'medium') and not ev.get('is_non_news', 0):
                    return 'news'
        return 'non_news'

# ------------------------------------------------------------------------------
# KEY LEVEL DETECTOR
# ------------------------------------------------------------------------------
@dataclass
class KeyLevel:
    time: datetime
    price: float
    type: str
    swept: bool = False
    timeframe: str = '1H'

class KeyLevelDetector:
    def __init__(self, name: str, tf_minutes: int):
        self.name = name
        self.tf = tf_minutes
        self.levels: List[KeyLevel] = []
        
    def update(self, df: pd.DataFrame):
        if len(df) < 3:
            return
        for i in range(1, len(df)-1):
            c1 = df.iloc[i-1]
            c2 = df.iloc[i]
            if c1['close'] > c1['open'] and c2['close'] < c2['open']:
                price = max(c1['high'], c2['high'])
                if not self._exists(price):
                    self.levels.append(KeyLevel(
                        time=df.index[i],
                        price=price,
                        type='high',
                        timeframe='1H' if self.tf==60 else '4H'
                    ))
            if c1['close'] < c1['open'] and c2['close'] > c2['open']:
                price = min(c1['low'], c2['low'])
                if not self._exists(price):
                    self.levels.append(KeyLevel(
                        time=df.index[i],
                        price=price,
                        type='low',
                        timeframe='1H' if self.tf==60 else '4H'
                    ))
        cutoff = datetime.now() - timedelta(days=30)
        self.levels = [l for l in self.levels if l.time > cutoff]
    
    def _exists(self, price: float, tolerance=0.5) -> bool:
        return any(abs(l.price - price) < tolerance for l in self.levels)
    
    def check_sweep(self, price: float, current_time: datetime, tolerance=Config.SWEEP_TOLERANCE) -> Optional[KeyLevel]:
        for lev in self.levels:
            if not lev.swept and abs(price - lev.price) <= tolerance:
                lev.swept = True
                lev.time = current_time
                return lev
        return None
    
    def get_nearest_levels(self, price: float) -> Tuple[Optional[float], Optional[float]]:
        above = [l.price for l in self.levels if not l.swept and l.price > price]
        below = [l.price for l in self.levels if not l.swept and l.price < price]
        above.sort()
        below.sort(reverse=True)
        return (below[0] if below else None, above[0] if above else None)

# ------------------------------------------------------------------------------
# BOS DETECTOR
# ------------------------------------------------------------------------------
class BOSDetector:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.trend: Optional[str] = None
        self.last_bos: Optional[Dict] = None
        self.last_high: Optional[float] = None
        self.last_low: Optional[float] = None
        
    def update(self, df: pd.DataFrame):
        if len(df) < 3:
            return
        highs, lows = [], []
        for i in range(1, len(df)-1):
            c1 = df.iloc[i-1]
            c2 = df.iloc[i]
            if c1['close'] > c1['open'] and c2['close'] < c2['open']:
                highs.append((df.index[i], max(c1['high'], c2['high'])))
            if c1['close'] < c1['open'] and c2['close'] > c2['open']:
                lows.append((df.index[i], min(c1['low'], c2['low'])))
        if self.trend == 'bullish' and lows:
            self.last_low = lows[-1][1]
        elif self.trend == 'bearish' and highs:
            self.last_high = highs[-1][1]
        elif not self.trend:
            if highs:
                self.last_high = highs[-1][1]
                self.trend = 'bearish'
            elif lows:
                self.last_low = lows[-1][1]
                self.trend = 'bullish'
        last = df.iloc[-1]
        close = last['close']
        now = df.index[-1]
        if self.trend == 'bearish' and self.last_high and close > self.last_high:
            self.trend = 'bullish'
            self.last_bos = {'time': now, 'price': close, 'type': 'bullish', 'level': self.last_high}
            self.last_high = None
        elif self.trend == 'bullish' and self.last_low and close < self.last_low:
            self.trend = 'bearish'
            self.last_bos = {'time': now, 'price': close, 'type': 'bearish', 'level': self.last_low}
            self.last_low = None
    
    def get_current_trend(self):
        return self.trend

# ------------------------------------------------------------------------------
# FVG E IVFG DETECTOR
# ------------------------------------------------------------------------------
@dataclass
class FVG:
    time: datetime
    top: float
    bottom: float
    type: str
    used: bool = False
    inverted: bool = False
    inverted_time: Optional[datetime] = None

class FVGDetector:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.fvgs: List[FVG] = []
        self.logger = logging.getLogger(f'FVG_{symbol}')   
        
    def update(self, df: pd.DataFrame):
        if len(df) < 3:
            return
        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]
        now = df.index[-1]
        
        # Nuove FVG
        if c3['low'] > c1['high']:
            self.fvgs.append(FVG(df.index[-2], c3['low'], c1['high'], 'bullish'))
        elif c3['high'] < c1['low']:
            self.fvgs.append(FVG(df.index[-2], c1['low'], c3['high'], 'bearish'))
        
        # Controlla inversioni (IVFG)
        for fvg in self.fvgs:
            if fvg.used or fvg.inverted:
                continue
            price = c3['close']
            if fvg.type == 'bullish' and price < fvg.bottom:
                fvg.inverted = True
                fvg.inverted_time = now
                fvg.used = True
                self.logger.info(f"IVFG ribassista su FVG rialzista {fvg.bottom:.2f}-{fvg.top:.2f}")
            elif fvg.type == 'bearish' and price > fvg.top:
                fvg.inverted = True
                fvg.inverted_time = now
                fvg.used = True
                self.logger.info(f"IVFG rialzista su FVG ribassista {fvg.bottom:.2f}-{fvg.top:.2f}")
        
        # Rimuovi FVG vecchie (>20 min)
        cutoff = now - timedelta(minutes=20)
        self.fvgs = [f for f in self.fvgs if f.time > cutoff]
    
    def get_active_fvgs(self, direction: Optional[str] = None) -> List[FVG]:
        active = [f for f in self.fvgs if not f.used and not f.inverted]
        if direction:
            active = [f for f in active if f.type == direction]
        return active
    
    def get_active_ivfgs(self, direction: Optional[str] = None) -> List[FVG]:
        active = [f for f in self.fvgs if f.inverted and not f.used]
        if direction:
            active = [f for f in active if f.type != direction]
        return active

# ------------------------------------------------------------------------------
# RISK MANAGER
# ------------------------------------------------------------------------------
class RiskManager:
    def __init__(self):
        self.initial_capital = Config.INITIAL_CAPITAL
        self.current_capital = Config.INITIAL_CAPITAL
        self.daily_pnl = 0.0
        self.monthly_pnl = 0.0
        self.trades_today = []
        self.trades_month = []
        self.last_reset_day = None
        self.last_reset_month = None
        
    def reset_daily(self, date):
        self.daily_pnl = 0.0
        self.trades_today = []
        self.last_reset_day = date
        
    def reset_monthly(self, date):
        self.monthly_pnl = 0.0
        self.trades_month = []
        self.last_reset_month = date
    
    def can_trade(self):
        if Config.KILL_SWITCH:
            return False
        daily_limit = self.initial_capital * Config.MAX_DAILY_LOSS
        if self.daily_pnl <= -daily_limit:
            return False
        # monthly limit disattivato (commentato)
        return True
    
    def calculate_quantity(self, symbol, entry, stop, num_trades=1):
        risk_per_trade = self.current_capital * (Config.RISK_PER_TRADE if num_trades == 1 else Config.RISK_PER_TRADE_DUAL)
        distance = abs(entry - stop)
        if distance == 0:
            return 0
        qty = risk_per_trade / (distance * Config.POINT_VALUE.get(symbol, 1))
        return max(1, int(qty))
    
    def update(self, pnl):
        self.daily_pnl += pnl
        self.monthly_pnl += pnl
        self.current_capital += pnl

# ------------------------------------------------------------------------------
# TRADE MODELS
# ------------------------------------------------------------------------------
class TradeSide(Enum):
    LONG = 'long'
    SHORT = 'short'

@dataclass
class Trade:
    id: str
    symbol: str
    side: TradeSide
    entry_time: datetime
    entry_price: float
    quantity: int
    stop_loss: float
    status: str = 'open'
    pnl: float = 0.0
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None

# ------------------------------------------------------------------------------
# STRATEGY ENGINE
# ------------------------------------------------------------------------------
class StrategyEngine:
    def __init__(self, tl_client: Optional[TradeLockerClient], news_calendar: NewsCalendar):
        self.tl = tl_client
        self.news = news_calendar
        self.risk = RiskManager()
        
        self.kl_1h = {}
        self.kl_4h = {}
        self.bos = {}
        self.fvg = {}
        for sym in Config.SYMBOLS:
            self.kl_1h[sym] = KeyLevelDetector(f"{sym}_1H", Config.TF_1H)
            self.kl_4h[sym] = KeyLevelDetector(f"{sym}_4H", Config.TF_4H)
            self.bos[sym] = BOSDetector(sym)
            self.fvg[sym] = FVGDetector(sym)
        
        self.current_date = None
        self.day_type = 'non_news'
        self.session = None
        
        self.touched_kl = {sym: [] for sym in Config.SYMBOLS}
        self.blocked = {sym: False for sym in Config.SYMBOLS}
        
        self.open_trades = []
        self.trade_counter = 0
        
        self.range_half = {sym: None for sym in Config.SYMBOLS}
        self.last_price = {sym: None for sym in Config.SYMBOLS}
        self.df_1m = {sym: pd.DataFrame() for sym in Config.SYMBOLS}
        
    def update_data(self, symbol: str, price: float, df_1m: pd.DataFrame):
        self.last_price[symbol] = price
        self.df_1m[symbol] = df_1m
        
        df_1h = df_1m.resample('1h').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
        df_4h = df_1m.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
        
        self.kl_1h[symbol].update(df_1h)
        self.kl_4h[symbol].update(df_4h)
        self.bos[symbol].update(df_1m)
        self.fvg[symbol].update(df_1m)
        
        if price:
            below, above = self.kl_4h[symbol].get_nearest_levels(price)
            if below and above:
                self.range_half[symbol] = (below + above) / 2
            else:
                self.range_half[symbol] = None
    
    def get_confluences(self, symbol: str, direction: str, allow_ivfg_as_last: bool = True) -> int:
        count = 0
        bos = self.bos[symbol].last_bos
        if bos and bos['type'] == direction:
            now_ny = datetime.now(ZoneInfo('America/New_York')).replace(tzinfo=None)
            age = (now_ny - bos['time']).total_seconds()
            if age < 300:
                count += 1
                logger.info(f"DEBUG {symbol}: BOS {direction} recente ({age:.0f}s fa) - contribuisce")
        
        fvgs = self.fvg[symbol].get_active_fvgs(direction)
        count += len(fvgs)
        if fvgs:
            logger.info(f"DEBUG {symbol}: {len(fvgs)} FVG {direction} attive")
        
        ivfgs = self.fvg[symbol].get_active_ivfgs(direction)
        if ivfgs:
            logger.info(f"DEBUG {symbol}: {len(ivfgs)} IVFG {direction} attive")
            if allow_ivfg_as_last:
                count += len(ivfgs)
            else:
                if count > 0:
                    count += len(ivfgs)
                else:
                    logger.info(f"DEBUG {symbol}: IVFG non contano (nessun'altra confluence)")
        logger.info(f"DEBUG {symbol}: Confluences totali = {count}")
        return count
    
    def calculate_stop_loss(self, symbol: str, direction: str, entry_price: float, bos_level: float = None, fvg: FVG = None, sweep_level: KeyLevel = None) -> float:
        logger.info(f"DEBUG {symbol}: Calcolo stop loss per {direction} a {entry_price}")
        if bos_level is not None:
            logger.info(f"DEBUG {symbol}: Stop basato su BOS level = {bos_level}")
            return bos_level
        
        if fvg is not None:
            logger.info(f"DEBUG {symbol}: Stop basato su FVG (top={fvg.top}, bottom={fvg.bottom})")
            df = self.df_1m.get(symbol, pd.DataFrame())
            if not df.empty:
                recent = df.iloc[-20:]
                if direction == 'bullish':
                    lows_below = recent[recent['low'] < fvg.bottom]['low']
                    if not lows_below.empty:
                        return lows_below.max()
                    else:
                        return fvg.bottom - Config.STOP_LOSS_OFFSET
                else:
                    highs_above = recent[recent['high'] > fvg.top]['high']
                    if not highs_above.empty:
                        return highs_above.min()
                    else:
                        return fvg.top + Config.STOP_LOSS_OFFSET
            else:
                return fvg.bottom - Config.STOP_LOSS_OFFSET if direction == 'bullish' else fvg.top + Config.STOP_LOSS_OFFSET
        
        if sweep_level is not None:
            logger.info(f"DEBUG {symbol}: Stop basato su sweep level = {sweep_level.price}")
            if direction == 'bullish':
                return sweep_level.price - Config.STOP_LOSS_OFFSET
            else:
                return sweep_level.price + Config.STOP_LOSS_OFFSET
        
        logger.info(f"DEBUG {symbol}: Stop fallback = {entry_price - Config.STOP_LOSS_OFFSET if direction == 'bullish' else entry_price + Config.STOP_LOSS_OFFSET}")
        return entry_price - Config.STOP_LOSS_OFFSET if direction == 'bullish' else entry_price + Config.STOP_LOSS_OFFSET
    
    def check_session(self, t: dtime) -> Optional[str]:
        if Config.PREMARKET_START <= t < Config.PREMARKET_END:
            return 'premarket'
        elif Config.MAIN_START <= t < Config.MAIN_END:
            return 'main'
        elif Config.POST_START <= t < Config.POST_END:
            return 'post'
        return None
    
    def process_minute(self, current_time: datetime):
        if self.current_date != current_time.date():
            self.current_date = current_time.date()
            self.day_type = self.news.get_day_type(self.current_date)
            logger.info(f"Nuovo giorno: {self.current_date} tipo: {self.day_type}")
            for sym in Config.SYMBOLS:
                self.touched_kl[sym] = []
                self.blocked[sym] = False
            if self.risk.last_reset_day != self.current_date:
                self.risk.reset_daily(self.current_date)
            first_of_month = self.current_date.replace(day=1)
            if self.risk.last_reset_month != first_of_month:
                self.risk.reset_monthly(first_of_month)
        
        self.session = self.check_session(current_time.time())
        if not self.session or Config.KILL_SWITCH:
            return
        
        if self.session == 'premarket':
            self._premarket_rules(current_time)
        elif self.session == 'main':
            self._main_rules(current_time)
        elif self.session == 'post':
            self._post_rules(current_time)
        
        self._manage_trades(current_time)
    
    def _premarket_rules(self, now: datetime):
        for sym in Config.SYMBOLS:
            price = self.last_price.get(sym)
            if not price:
                continue
            sweep = self.kl_4h[sym].check_sweep(price, now)
            if not sweep:
                continue
            self.touched_kl[sym].append(sweep)
            direction = 'bearish' if sweep.type == 'high' else 'bullish'
            other = 'ES' if sym == 'NQ' else 'NQ'
            if len(self.touched_kl[other]) == 0:
                continue
            conf_count = self.get_confluences(sym, direction, allow_ivfg_as_last=False)
            if conf_count >= Config.MIN_CONFLUENCES:
                stop = self.calculate_stop_loss(sym, direction, price, sweep_level=sweep)
                self._enter_trade(sym, now, price, direction, stop)
    
    def _main_rules(self, now: datetime):
        h, m = now.hour, now.minute
        for sym in Config.SYMBOLS:
            price = self.last_price.get(sym)
            if not price:
                continue
            s1h = self.kl_1h[sym].check_sweep(price, now)
            s4h = self.kl_4h[sym].check_sweep(price, now)
            
            if s1h and self.touched_kl[sym]:
                self.blocked[sym] = True
                self.touched_kl[sym].append(s1h)
                continue
            if s4h:
                self.blocked[sym] = False
                self.touched_kl[sym].append(s4h)
            
            if self.blocked[sym]:
                continue
            
            direction = None
            window = False
            fvg = None
            
            if s1h and h == 9 and m < 50:
                window = True
                if self.range_half[sym] is not None:
                    if price > self.range_half[sym]:
                        direction = 'bullish' if self.day_type == 'non_news' else 'bearish'
                    else:
                        direction = 'bearish' if self.day_type == 'non_news' else 'bullish'
            elif s4h and h == 9 and m >= 50:
                window = True
                if self.day_type == 'non_news':
                    direction = 'bullish' if s4h.type == 'low' else 'bearish'
                else:
                    direction = 'bearish' if s4h.type == 'low' else 'bullish'
            
            if window and direction:
                conf_count = self.get_confluences(sym, direction, allow_ivfg_as_last=Config.IVFG_AS_LAST)
                if conf_count >= Config.MIN_CONFLUENCES:
                    stop = self.calculate_stop_loss(sym, direction, price, sweep_level=s4h)
                    self._enter_trade(sym, now, price, direction, stop)
    
    def _post_rules(self, now: datetime):
        for sym in Config.SYMBOLS:
            price = self.last_price.get(sym)
            if not price:
                continue
            trend = self.bos[sym].get_current_trend()
            if not trend:
                continue
            bos = self.bos[sym].last_bos
            if not bos or bos['type'] != trend:
                continue
            if (now - bos['time']).total_seconds() > 120:
                continue
            other_conf = self.get_confluences(sym, trend, allow_ivfg_as_last=False) - 1
            if other_conf < 1:
                continue
            stop = self.calculate_stop_loss(sym, trend, price, bos_level=bos['level'])
            self._enter_trade(sym, now, price, trend, stop)
    
    def _enter_trade(self, symbol: str, now: datetime, price: float, direction: str, stop: float):
        if not self.risk.can_trade():
            logger.warning("Risk limits impediscono nuovo trade")
            return
        num_open = len([t for t in self.open_trades if t.status == 'open'])
        qty = self.risk.calculate_quantity(symbol, price, stop, num_trades=1 if num_open < 2 else 2)
        if qty == 0:
            return
        self.trade_counter += 1
        trade_id = f"{symbol}_{now.strftime('%Y%m%d%H%M')}_{self.trade_counter}"
        side = TradeSide.LONG if direction == 'bullish' else TradeSide.SHORT
        trade = Trade(
            id=trade_id,
            symbol=symbol,
            side=side,
            entry_time=now,
            entry_price=price,
            quantity=qty,
            stop_loss=stop,
            status='open'
        )
        self.open_trades.append(trade)
        logger.info(f"🚀 NUOVO TRADE: {trade_id} {side.value.upper()} {symbol} x{qty} @ {price:.2f} stop {stop:.2f}")
        if self.tl and self.tl.connected:
            tl_side = 'buy' if side == TradeSide.LONG else 'sell'
            self.tl.place_order(symbol, tl_side, qty)
        else:
            logger.info("(ordine simulato)")
    
    def _manage_trades(self, now: datetime):
        for trade in self.open_trades[:]:
            if trade.status != 'open':
                continue
            price = self.last_price.get(trade.symbol)
            if not price:
                continue
            
            if (trade.side == TradeSide.LONG and price <= trade.stop_loss) or \
               (trade.side == TradeSide.SHORT and price >= trade.stop_loss):
                self._close_trade(trade, now, price, 'stop_loss')
                continue
            
            if now.hour == 10 and now.minute == 1 and trade.entry_time < now:
                if trade.side == TradeSide.LONG:
                    rr = (price - trade.entry_price) / (trade.entry_price - trade.stop_loss)
                else:
                    rr = (trade.entry_price - price) / (trade.stop_loss - trade.entry_price)
                if rr >= 1.0:
                    self._close_trade(trade, now, price, 'tp50', fraction=0.5)
                    trade.status = 'closed_50'
                    continue
            
            for kl in self.kl_4h[trade.symbol].levels:
                if not kl.swept and abs(price - kl.price) < Config.SWEEP_TOLERANCE:
                    if now.time() < Config.POST_END:
                        if trade.status == 'open':
                            self._close_trade(trade, now, price, '4h50', fraction=0.5)
                            trade.status = 'closed_50'
                    else:
                        self._close_trade(trade, now, price, '4h100')
                    break
            
            close_time = Config.CLOSE_NONEWS if self.day_type == 'non_news' else Config.CLOSE_NEWS
            if now.time() >= close_time and trade.status != 'closed':
                self._close_trade(trade, now, price, 'timeout')
    
    def _close_trade(self, trade: Trade, now: datetime, price: float, reason: str, fraction: float = 1.0):
        point_value = Config.POINT_VALUE.get(trade.symbol, 1)
        if trade.side == TradeSide.LONG:
            pnl = (price - trade.entry_price) * trade.quantity * fraction * point_value
        else:
            pnl = (trade.entry_price - price) * trade.quantity * fraction * point_value
        self.risk.update(pnl)
        if fraction < 1.0:
            closed_qty = int(trade.quantity * fraction)
            trade.quantity -= closed_qty
            logger.info(f"✂️ Chiuso {fraction*100:.0f}% {trade.id} P&L {pnl:.2f} ({reason})")
        else:
            trade.status = 'closed'
            trade.exit_time = now
            trade.exit_price = price
            trade.pnl = pnl
            self.open_trades.remove(trade)
            logger.info(f"✅ Chiuso {trade.id} P&L {pnl:.2f} ({reason})")

# ------------------------------------------------------------------------------
# BACKTEST
# ------------------------------------------------------------------------------
def run_backtest(strategy: StrategyEngine, data_nq: pd.DataFrame, data_es: pd.DataFrame):
    logger.info("=== AVVIO BACKTEST ===")
    common_index = data_nq.index.intersection(data_es.index)
    data_nq = data_nq.loc[common_index]
    data_es = data_es.loc[common_index]
    total = len(common_index)
    processed = 0
    for dt in common_index:
        processed += 1
        if processed % 1000 == 0:
            logger.info(f"Progresso: {processed}/{total}")
        price_nq = data_nq.loc[dt, 'close']
        price_es = data_es.loc[dt, 'close']
        start = dt - timedelta(minutes=120)
        df_nq = data_nq.loc[start:dt].copy()
        df_es = data_es.loc[start:dt].copy()
        if df_nq.empty or df_es.empty:
            continue
        strategy.update_data('NQ', price_nq, df_nq)
        strategy.update_data('ES', price_es, df_es)
        strategy.process_minute(dt)
    logger.info("=== BACKTEST COMPLETATO ===")
    total_pnl = strategy.risk.current_capital - Config.INITIAL_CAPITAL
    logger.info(f"P&L totale: {total_pnl:.2f}")
    logger.info(f"Trade eseguiti: {len(strategy.risk.trades_month)}")
    win = [t for t in strategy.risk.trades_month if t.get('pnl',0)>0]
    win_rate = len(win)/max(1,len(strategy.risk.trades_month))*100
    logger.info(f"Win rate: {win_rate:.1f}%")

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    logger.info("="*50)
    logger.info("AVVIO BOT DI TRADING (IVFG + STOP DINAMICO)")
    logger.info("="*50)
    
    news = NewsCalendar()
    
    tl = TradeLockerClient()
    if tl.connect():
        logger.info("TradeLocker connesso")
    else:
        logger.warning("TradeLocker non connesso – modalità simulata")
    
    strategy = StrategyEngine(tl if tl.connected else None, news)
    
    itick = None
    if Config.ITICK_TOKEN:
        itick = iTickClient(Config.ITICK_TOKEN)
        for sym in Config.SYMBOLS:
            itick.fetch_historical(sym, days=5, interval='1m')
        def on_quote(symbol, quote):
            price = quote.get('last') or (quote.get('bid')+quote.get('ask'))/2
            if price:
                df = itick.get_1m_df(symbol, minutes=120)
                if not df.empty:
                    strategy.update_data(symbol, price, df)
        itick.start(on_quote)
        logger.info("✅ iTick avviato")
    else:
        logger.warning("⚠️ ITICK_TOKEN non fornito, uso simulazione prezzi")
        def simulate():
            while True:
                now_ny = datetime.now(ZoneInfo('America/New_York')).replace(tzinfo=None)
                for sym in Config.SYMBOLS:
                    base_price = 10000 if sym == 'NQ' else 4000
                    price = base_price + np.random.randn() * 10
                    end = now_ny
                    start = end - timedelta(minutes=120)
                    idx = pd.date_range(start, end, freq='1min')
                    df = pd.DataFrame({
                        'open': price + np.random.randn(len(idx))*2,
                        'high': price + 5 + np.random.rand(len(idx))*5,
                        'low': price - 5 - np.random.rand(len(idx))*5,
                        'close': price + np.random.randn(len(idx))*2,
                        'volume': np.random.randint(100, 1000, len(idx))
                    }, index=idx)
                    strategy.update_data(sym, price, df)
                time.sleep(1)
        sim = threading.Thread(target=simulate, daemon=True)
        sim.start()
    
    last_min = None
    try:
        while True:
            now_ny = datetime.now(ZoneInfo('America/New_York'))
            now_naive = now_ny.replace(tzinfo=None)
            cur = now_naive.replace(second=0, microsecond=0)
            if cur != last_min:
                last_min = cur
                strategy.process_minute(cur)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Arresto...")
    finally:
        if itick:
            itick.stop()
        tl.disconnect()
        logger.info("Bot fermato.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'backtest':
        if not os.path.exists('NQ_1m.csv') or not os.path.exists('ES_1m.csv'):
            logger.error("Servono i file NQ_1m.csv e ES_1m.csv per il backtest")
            sys.exit(1)
        data_nq = pd.read_csv('NQ_1m.csv', index_col=0, parse_dates=True)
        data_es = pd.read_csv('ES_1m.csv', index_col=0, parse_dates=True)
        news = NewsCalendar()  # carica il CSV all'avvio, nessuna chiamata update
        strategy = StrategyEngine(None, news)
        run_backtest(strategy, data_nq, data_es)
    else:
        main()