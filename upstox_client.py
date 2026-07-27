"""
Thin wrapper around the Upstox v2 historical/intraday candle endpoints and
instrument search, authenticated with an Analytics Token (read-only market
data access).
"""
import requests
import pandas as pd
from datetime import datetime, timedelta

import config

log = config.get_logger(__name__)


class UpstoxClient:
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

    def search_instruments(self, query: str, exchanges: str = "MCX", segments: str = "FO",
                            expiry: str = "current_month,next_month") -> list:
        url = (f"{config.UPSTOX_BASE_URL}/instruments/search?query={query}"
               f"&exchanges={exchanges}&segments={segments}&expiry={expiry}&records=30")
        data = self._get(url)
        return data.get("data", [])

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
