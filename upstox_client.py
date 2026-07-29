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

        Matching is done in two passes:
        1. Strict pass - exact "name" match (fast path, works as long as
           Upstox's naming hasn't changed).
        2. Fallback pass - if the strict pass finds nothing, retry with a
           normalized/token-based match (ignores extra spaces/punctuation,
           and just requires every word of the query - e.g. "GOLD" and
           "MINI" - to appear somewhere in the instrument name, in any
           order). This is tolerant of minor naming format changes on
           Upstox's side (e.g. "GOLD MINI" vs "GOLD  MINI" vs a reordered
           name), and accepts any instrument_type starting with "FUT"
           instead of requiring an exact "FUT" match.

        If BOTH passes come up empty, the actual distinct name/type values
        seen for the base commodity keyword are logged at WARNING level,
        so the real field values are visible in the run's logs instead of
        guessing blind. (This replaces the old debug block, which checked
        query_upper == "CRUDEOIL" but was actually called with
        "CRUDE OIL MINI" - so it never fired.)
        """
        instruments = self._load_instrument_master()
        if not instruments:
            return []

        query_upper = query.upper().strip()

        def normalize(s: str) -> str:
            return " ".join((s or "").upper().split())

        query_norm = normalize(query_upper)
        query_tokens = query_norm.split()
        base_keyword = query_tokens[0] if query_tokens else query_norm

        # Pass 1: strict exact match (original behavior, kept as fast path)
        strict_matches = []
        for inst in instruments:
            name = normalize(inst.get("name") or inst.get("underlying_symbol") or "")
            inst_type = inst.get("instrument_type", "")
            if name == query_norm and inst_type == "FUT":
                strict_matches.append(inst)

        if strict_matches:
            candidates = strict_matches
        else:
            # Pass 2: tolerant token-based match
            fallback_matches = []
            for inst in instruments:
                name = normalize(inst.get("name") or inst.get("underlying_symbol") or "")
                inst_type = str(inst.get("instrument_type", "")).upper()
                if all(tok in name for tok in query_tokens) and inst_type.startswith("FUT"):
                    fallback_matches.append(inst)

            if fallback_matches:
                log.warning(
                    "%s: strict name match failed, but found %d contract(s) via "
                    "tolerant match - Upstox's naming format may have changed. "
                    "Matched name(s): %s",
                    query_upper, len(fallback_matches),
                    sorted(set(normalize(m.get("name") or "") for m in fallback_matches)),
                )
                candidates = fallback_matches
            else:
                # Nothing matched either way - log what's actually out there
                # for this commodity so the real field values are visible.
                related = [inst for inst in instruments
                           if base_keyword in normalize(inst.get("name") or inst.get("underlying_symbol") or "")]
                distinct_names = sorted(set(normalize(m.get("name") or "") for m in related))
                distinct_types = sorted(set(str(m.get("instrument_type", "")) for m in related))
                log.warning(
                    "%s: no match found (strict or tolerant). Instrument names containing "
                    "%r: %s | instrument_type values seen: %s",
                    query_upper, base_keyword, distinct_names, distinct_types,
                )

                # Extra diagnostic: MCX doesn't seem to encode MINI in the
                # "name" field at all (it's just "GOLD", "SILVER", etc).
                # The MINI/full-size distinction likely lives in
                # trading_symbol or lot_size instead - log those for FUT
                # contracts on the base name so we can see the real pattern.
                fut_on_base_name = [
                    inst for inst in related
                    if normalize(inst.get("name") or "") == base_keyword
                    and str(inst.get("instrument_type", "")).upper() == "FUT"
                ]
                sample = [
                    {
                        "trading_symbol": inst.get("trading_symbol"),
                        "lot_size": inst.get("lot_size"),
                        "expiry": inst.get("expiry"),
                    }
                    for inst in fut_on_base_name[:15]
                ]
                log.warning(
                    "%s: FUT contracts under name=%r (sample of trading_symbol/lot_size/expiry "
                    "to identify the MINI variant): %s",
                    query_upper, base_keyword, sample,
                )
                candidates = []

        results = []
        for inst in candidates:
            name = normalize(inst.get("name") or inst.get("underlying_symbol") or "")
            results.append({
                "underlying_symbol": name,
                "instrument_type": inst.get("instrument_type", ""),
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
