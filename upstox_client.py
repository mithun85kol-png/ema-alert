"""
Thin wrapper around the Upstox v2 historical/intraday candle endpoints and
instrument search, authenticated with an Analytics Token (read-only market
data access).
"""
import gzip
import io
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

import config

log = config.get_logger(__name__)


class UpstoxClient:
    _instrument_cache = None  # class-level cache, one download per run

    def __init__(self, token: str = None):
        self.token = token or config.UPSTOX_ANALYTICS_TOKEN
        if not self.token:
            raise ValueError("UPSTOX_ANALYTICS_TOKEN is not set. Check your .env file.")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        })

    def _get(self, url: str) -> dict:
        resp = self.session.get(url, timeout=15)
        if resp.status_code != 200:
            log.error("Upstox API error %s: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()
        return resp.json()

    def get_intraday_candles(self, instrument_key: str, interval: str = "1minute") -> pd.DataFrame:
        url = f"{config.UPSTOX_BASE_URL}/historical-candle/intraday/{instrument_key}/{interval}"
        data = self._get(url)
        return self._to_dataframe(data)

    def get_historical_candles(self, instrument_key: str, interval: str,
                                from_date: str, to_date: str) -> pd.DataFrame:
        url = (f"{config.UPSTOX_BASE_URL}/historical-candle/{instrument_key}/"
               f"{interval}/{to_date}/{from_date}")
        data = self._get(url)
        return self._to_dataframe(data)

    def get_recent_candles(self, instrument_key: str, interval: str = "1minute",
                            lookback_days: int = 5) -> pd.DataFrame:
        today = datetime.now()
        from_date = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

        frames = []
        try:
            hist = self.get_historical_candles(instrument_key, interval, from_date, to_date)
            if not hist.empty:
                frames.append(hist)
        except requests.HTTPError:
            log.warning("Historical candle fetch failed for %s, continuing with intraday only", instrument_key)

        try:
            intraday = self.get_intraday_candles(instrument_key, interval)
            if not intraday.empty:
                frames.append(intraday)
        except requests.HTTPError:
            log.warning("Intraday candle fetch failed for %s", instrument_key)

        if not frames:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        return df

    def _load_instrument_master(self) -> list:
        """
        Downloads and caches Upstox's MCX instrument master file.
        Upstox does not expose a live query-based search endpoint, so this
        is the correct way to look up tradable contracts.
        """
        if UpstoxClient._instrument_cache is not None:
            return UpstoxClient._instrument_cache

        url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
                data = json.loads(gz.read().decode("utf-8"))
            UpstoxClient._instrument_cache = data
            log.info("Loaded MCX instrument master: %d instruments", len(data))
            if data:
                log.info("Sample instrument: %s", data[0])
            return data
        except Exception as e:
            log.error("Failed to load MCX instrument master: %s", e)
            UpstoxClient._instrument_cache = []
            return []

    def search_instruments(self, query: str, exchanges: str = "MCX", segments: str = "FO",
                            expiry: str = "current_month,next_month") -> list:
        """
        Filters the MCX instrument master for futures contracts matching
        the given underlying symbol (e.g. GOLD, SILVER, CRUDEOIL).
        """
        instruments = self._load_instrument_master()
        if not instruments:
            return []

        query_upper = query.upper()
        results = []
        for inst in instruments:
            name = (inst.get("name") or inst.get("underlying_symbol") or "").upper()
            inst_type = inst.get("instrument_type", "")
            if name == query_upper and inst_type == "FUT":
                results.append({
                    "underlying_symbol": name,
                    "instrument_type": inst_type,
                    "instrument_key": inst.get("instrument_key"),
                    "trading_symbol": inst.get("trading_symbol"),
                    "expiry": inst.get("expiry"),
                })
        return results

    @staticmethod
    def _to_dataframe(data: dict) -> pd.DataFrame:
        candles = data.get("data", {}).get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
