# save_data.py

import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# --- symbol & timeframe lists ---
CRYPTO = ["btcusd", "ethusd", "xrpusd"]
FOREX = ["usdx", "eurusd", "gbpusd", "usdchf", "audusd", "nzdusd", "usdcad"]
SYMBOLS = CRYPTO + FOREX

TIMEFRAMES = ["1", "5", "15", "30", "60", "240", "D"]


# --- timeframe to seconds ---
def tf_to_seconds(tf: str) -> int:
    if tf == "D":
        return 86400
    return int(tf) * 60


# --- improved helper to convert many input types to unix seconds ---
def to_unix_timestamp(value, assume_tz: str | None = "Asia/Qatar") -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv > 1_000_000_000_000:
            return iv // 1000
        return iv
    if isinstance(value, (pd.Timestamp, datetime)):
        ts = pd.to_datetime(value)
        if ts.tz is None:
            ts = ts.tz_localize(assume_tz if assume_tz else "UTC")
        ts = ts.tz_convert("UTC")
        return int(ts.timestamp())
    if isinstance(value, str):
        ts = pd.to_datetime(value, errors="raise")
        if ts.tz is None:
            ts = ts.tz_localize(assume_tz if assume_tz else "UTC")
        ts = ts.tz_convert("UTC")
        return int(ts.timestamp())
    raise TypeError(f"Unsupported type for timestamp conversion: {type(value)}")


# --- normalize OHLC and convert datetimes ---
def normalize_ohlc(ohlc_data: dict, return_tz_offset_minutes: int = 210) -> pd.DataFrame:
    if not ohlc_data:
        return pd.DataFrame()

    t_list = list(ohlc_data.get("t", []))
    if max(t_list or [0]) > 1_000_000_000_000:
        t_list = [int(x) // 1000 if (x is not None and pd.notna(x)) else None for x in t_list]

    target_tz = timezone(timedelta(minutes=return_tz_offset_minutes))
    datetimes = [
        datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(target_tz) if ts else pd.NaT
        for ts in t_list
    ]

    df = pd.DataFrame({
        "Date": pd.DatetimeIndex(datetimes),
        "Open": pd.to_numeric(ohlc_data.get("o", []), errors="coerce"),
        "High": pd.to_numeric(ohlc_data.get("h", []), errors="coerce"),
        "Low": pd.to_numeric(ohlc_data.get("l", []), errors="coerce"),
        "Close": pd.to_numeric(ohlc_data.get("c", []), errors="coerce"),
        "Volume": pd.to_numeric(ohlc_data.get("v", []), errors="coerce") if "v" in ohlc_data else 0,
    })

    df.sort_values("Date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["Date"] = df["Date"].dt.tz_localize(None)  # remove timezone
    return df


# --- get_ohlc with dynamic lookback ---
def get_ohlc(symbol: str, timeframe: str, input_tz: str | None = "Asia/Qatar") -> pd.DataFrame:
    now = int(time.time())
    candle_sec = tf_to_seconds(timeframe)
    from_unix = now - candle_sec * 10_000
    to_unix = now

    try:
        lite_finance_url = (
            "https://my.litefinance.org/chart/get-history"
            f"?symbol={symbol.upper()}&resolution={timeframe}&from={from_unix}&to={to_unix}"
        )
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://my.litefinance.org/",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = requests.get(lite_finance_url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        ohlc_data = data.get("data", {})
        if ohlc_data:
            return normalize_ohlc(ohlc_data, return_tz_offset_minutes=3 * 60 + 30)
    except Exception as e:
        print(f"[LiteFinance] OHLC error: {e}")
    return pd.DataFrame()


# --- save/append to csv ---
def save_to_csv(df: pd.DataFrame, symbol: str, timeframe: str):
    os.makedirs("csv_data", exist_ok=True)
    filename = f"csv_data/csv_{symbol.lower()}_{timeframe}.csv"

    if os.path.exists(filename):
        old_df = pd.read_csv(filename, parse_dates=["Date"])
        df = pd.concat([old_df, df], ignore_index=True)
        df.drop_duplicates(subset=["Date"], inplace=True)
        df.sort_values("Date", inplace=True)

    df.to_csv(filename, index=False, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    print(f"Updated: {filename}")


if __name__ == "__main__":
    MAX_RETRIES = 3

    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            print(f"Fetching {symbol} {tf} ...")
            df = pd.DataFrame()

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = get_ohlc(symbol, tf)
                    if not df.empty:
                        break  # success, no need to retry
                except Exception as e:
                    print(f"Attempt {attempt} failed for {symbol} {tf}: {e}")
                time.sleep(2)  # small delay before retry

            if not df.empty:
                save_to_csv(df, symbol, tf)
                print(f"Saved {symbol} {tf} ✅")
            else:
                print(f"Failed to fetch {symbol} {tf} after {MAX_RETRIES} retries ❌")