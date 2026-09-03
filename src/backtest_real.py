"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  Bull Hunter v2 — backtest_real.py                                          ║
║  بک‌تست واقعی روی Top 200 با داده واقعی Binance                            ║
║                                                                              ║
║  این اسکریپت طراحی شده تا داخل GitHub Actions اجرا شود، جایی که           ║
║  برخلاف sandbox های محدود، دسترسی کامل به اینترنت (و API صرافی‌ها) دارید. ║
║                                                                              ║
║  اجرای محلی:                                                                ║
║      pip install -r requirements.txt                                         ║
║      python src/backtest_real.py --days 14 --interval 5m                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BINANCE_BASE = "https://api.binance.com/api/v3"

# ══════════════════════════════════════════════════════════════════════════════
#  Top 200 — همان لیست bull_hunter_v2.py
# ══════════════════════════════════════════════════════════════════════════════
TOP200 = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","AVAXUSDT","DOTUSDT","TRXUSDT","LINKUSDT",
    "MATICUSDT","NEARUSDT","ICPUSDT","APTUSDT","ATOMUSDT",
    "FILUSDT","VETUSDT","HBARUSDT","ALGOUSDT","XLMUSDT",
    "ARBUSDT","OPUSDT","STXUSDT","INJUSDT","SUIUSDT",
    "SEIUSDT","TIAUSDT","JUPUSDT","PYTHUSDT","UNIUSDT",
    "AAVEUSDT","MKRUSDT","SNXUSDT","CRVUSDT","COMPUSDT",
    "LDOUSDT","RUNEUSDT","DYDXUSDT","GMXUSDT","SUSHIUSDT",
    "PENDLEUSDT","ANKRUSDT","DOGEUSDT","SHIBUSDT","PEPEUSDT",
    "FLOKIUSDT","BONKUSDT","WIFUSDT","MEMEUSDT","BOMEUSDT",
    "NEIROUSDT","POPCATUSDT","MEWUSDT","AXSUSDT","SANDUSDT",
    "MANAUSDT","ENJUSDT","GALAUSDT","RNDRUSDT","IMXUSDT",
    "FETUSDT","AGIXUSDT","OCEANUSDT","RENDERUSDT","WLDUSDT",
    "GRTUSDT","XMRUSDT","ZECUSDT","DASHUSDT","ROSEUSDT",
    "QNTUSDT","GLMUSDT","STORJUSDT","BANDUSDT","XTZUSDT",
    "EOSUSDT","MINAUSDT","KASUSDT","CFXUSDT","ORDIUSDT",
    "ETCUSDT","BCHUSDT","LTCUSDT","ZRXUSDT","BATUSDT",
    "APEUSDT","CHZUSDT","FLOWUSDT","PERPUSDT","SFPUSDT",
    "NKNUSDT","ONDOUSDT","NOTUSDT","IOTAUSDT","TONUSDT",
    "FTMUSDT","1INCHUSDT","CAKEUSDT","RAYUSDT","JITOUSDT",
    "YGGUSDT","SLPUSDT","ALICEUSDT","BLZUSDT","CTSIUSDT",
    "STRKUSDT","BIOUSDT","MOVEUSDT","SONICUSDT","TRUMPUSDT",
]


# ══════════════════════════════════════════════════════════════════════════════
#  دریافت داده واقعی از Binance با pagination
# ══════════════════════════════════════════════════════════════════════════════
DEBUG_LOG = []

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int,
                  session: requests.Session) -> list:
    """دریافت همه کندل‌های واقعی بین دو زمان، با pagination خودکار (حداکثر 1000 در هر درخواست)"""
    all_klines = []
    cur = start_ms
    while cur < end_ms:
        try:
            r = session.get(f"{BINANCE_BASE}/klines", params={
                "symbol": symbol, "interval": interval,
                "startTime": cur, "endTime": end_ms, "limit": 1000,
            }, timeout=15)
            if r.status_code != 200:
                msg = f"{symbol}: HTTP {r.status_code} — {r.text[:200]}"
                log.warning(f"  {msg}")
                DEBUG_LOG.append(msg)
                break
            batch = r.json()
            if not batch:
                break
            all_klines.extend(batch)
            last_open_time = batch[-1][0]
            if last_open_time <= cur:  # جلوگیری از حلقه بی‌نهایت
                break
            cur = last_open_time + 1
            if len(batch) < 1000:
                break
            time.sleep(0.05)  # احترام به rate limit
        except Exception as e:
            msg = f"{symbol}: خطا در دریافت — {type(e).__name__}: {e}"
            log.warning(f"  {msg}")
            DEBUG_LOG.append(msg)
            break
    if not all_klines:
        DEBUG_LOG.append(f"{symbol}: هیچ کندلی دریافت نشد (all_klines خالی ماند)")
    return all_klines


def klines_to_df(raw: list) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    for c in ["open","high","low","close","volume","quote_volume"]:
        df[c] = df[c].astype(float)
    df["dt"] = pd.to_datetime(df["open_time"], unit="ms")
    return df.sort_values("dt").reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  اندیکاتورها — دقیقاً همان فرمول‌های bull_hunter_v2.py
# ══════════════════════════════════════════════════════════════════════════════
def rsi(closes, period=7):
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)

def macd_hist(closes, fast=12, slow=26, sig=9):
    ema_f = closes.ewm(span=fast, adjust=False).mean()
    ema_s = closes.ewm(span=slow, adjust=False).mean()
    macd = ema_f - ema_s
    signal = macd.ewm(span=sig, adjust=False).mean()
    return macd - signal

def bollinger_upper(closes, period=20, k=2.0):
    sma = closes.rolling(period).mean()
    std = closes.rolling(period).std()
    return sma + k * std

def vwap_rolling(df, window):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    pv = tp * df["volume"]
    return pv.rolling(window).sum() / df["volume"].rolling(window).sum()

def atr(df, period=10):
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def vol_ratio(volumes, lookback=20):
    avg = volumes.rolling(lookback).mean().shift(1)
    return volumes / avg


def compute_indicators(df: pd.DataFrame, vwap_window: int) -> pd.DataFrame:
    df = df.copy()
    df["rsi7"]      = rsi(df["close"], 7)
    df["macd_hist"] = macd_hist(df["close"])
    df["bb_upper"]  = bollinger_upper(df["close"])
    df["vwap"]      = vwap_rolling(df, vwap_window)
    df["ema20"]     = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"]     = df["close"].ewm(span=50, adjust=False).mean()
    df["atr10"]     = atr(df, 10)
    df["vol_ratio"] = vol_ratio(df["volume"], 20)
    df["change_2candle"] = df["close"].pct_change(periods=2) * 100
    bb_std = df["close"].rolling(20).std()
    df["atr_squeeze"] = df["atr10"] > (2 * bb_std * 0.8)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  موتور بک‌تست — شبیه‌سازی کامل ورود/خروج
# ══════════════════════════════════════════════════════════════════════════════
def backtest_symbol(symbol: str, df: pd.DataFrame, cfg: dict) -> dict:
    signals, trades = [], []
    open_trade = None
    last_signal_idx = None
    start_idx = max(60, cfg["cooldown_candles"])

    if len(df) <= start_idx:
        return {"symbol": symbol, "signals": [], "trades": []}

    for i in range(start_idx, len(df)):
        row = df.iloc[i]

        if open_trade is not None:
            price = row["close"]
            if price > open_trade["high"]:
                open_trade["high"] = price
            pnl = (price - open_trade["entry"]) / open_trade["entry"] * 100

            if not open_trade["trail_on"] and pnl >= cfg["trail_activate_pct"]:
                open_trade["trail_on"] = True
                open_trade["trail_stop"] = open_trade["high"] - cfg["atr_mult"] * open_trade["atr"]
            elif open_trade["trail_on"]:
                ns = open_trade["high"] - cfg["atr_mult"] * open_trade["atr"]
                if ns > open_trade["trail_stop"]:
                    open_trade["trail_stop"] = ns

            reason = None
            if open_trade["trail_on"] and price <= open_trade["trail_stop"]:
                reason = "ATR Trail"
            elif price <= open_trade["hard_stop"]:
                reason = "Hard Stop"

            if reason:
                pnl_pct = (price - open_trade["entry"]) / open_trade["entry"] * 100
                pnl_usdt = (cfg["trade_usdt"] / open_trade["entry"]) * (price - open_trade["entry"])
                trades.append({
                    "symbol": symbol,
                    "entry_time": str(open_trade["entry_time"]), "exit_time": str(row["dt"]),
                    "entry": round(float(open_trade["entry"]), 8), "exit": round(float(price), 8),
                    "pnl_pct": round(float(pnl_pct), 3), "pnl_usdt": round(float(pnl_usdt), 3),
                    "reason": reason,
                    "duration_min": round((row["dt"] - open_trade["entry_time"]).total_seconds() / 60, 1),
                })
                open_trade = None
            continue

        if pd.isna(row["rsi7"]) or pd.isna(row["macd_hist"]) or pd.isna(row["bb_upper"]) or pd.isna(row["vwap"]):
            continue

        chg = row["change_2candle"]
        if pd.isna(chg) or chg < cfg["min_change_pct"]:
            continue

        t1 = (row["vol_ratio"] >= cfg["volume_mult"]) if not pd.isna(row["vol_ratio"]) else False
        t2 = cfg["rsi_min"] <= row["rsi7"] <= cfg["rsi_max"]
        t3 = row["macd_hist"] > 0
        t4 = row["close"] > row["bb_upper"]
        t5 = row["close"] > row["vwap"]
        t6 = (last_signal_idx is None) or (i - last_signal_idx >= cfg["cooldown_candles"])
        t7 = row["ema20"] > row["ema50"]
        t8 = bool(row["atr_squeeze"])

        tests = {"t1": t1, "t2": t2, "t3": t3, "t4": t4, "t5": t5, "t6": t6, "t7": t7, "t8": t8}
        passed = sum(tests.values())
        confirmed = passed >= cfg["min_tests"] and t6

        signals.append({"time": str(row["dt"]), "price": float(row["close"]),
                        "change_pct": round(float(chg), 3), "tests_passed": passed, "confirmed": confirmed})

        if confirmed:
            last_signal_idx = i
            open_trade = {
                "entry": row["close"], "entry_time": row["dt"], "high": row["close"],
                "atr": row["atr10"], "trail_on": False, "trail_stop": None,
                "hard_stop": row["close"] * (1 - cfg["hard_stop_pct"] / 100),
            }

    if open_trade is not None:
        lr = df.iloc[-1]
        pnl_pct = (lr["close"] - open_trade["entry"]) / open_trade["entry"] * 100
        pnl_usdt = (cfg["trade_usdt"] / open_trade["entry"]) * (lr["close"] - open_trade["entry"])
        trades.append({
            "symbol": symbol,
            "entry_time": str(open_trade["entry_time"]), "exit_time": str(lr["dt"]),
            "entry": round(float(open_trade["entry"]), 8), "exit": round(float(lr["close"]), 8),
            "pnl_pct": round(float(pnl_pct), 3), "pnl_usdt": round(float(pnl_usdt), 3),
            "reason": "End of Data (still open)",
            "duration_min": round((lr["dt"] - open_trade["entry_time"]).total_seconds() / 60, 1),
        })

    return {"symbol": symbol, "signals": signals, "trades": trades}


# ══════════════════════════════════════════════════════════════════════════════
#  اجرای اصلی
# ══════════════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(description="بک‌تست واقعی Bull Hunter v2 روی Top 200 — داده واقعی Binance")
    ap.add_argument("--days", type=int, default=14, help="تعداد روزهای بک‌تست")
    ap.add_argument("--interval", type=str, default="5m", help="تایم‌فریم کندل (1m, 5m, 15m, ...)")
    ap.add_argument("--symbols", type=int, default=200, help="تعداد ارز از لیست Top200 (برای تست سریع می‌توان کم کرد)")
    ap.add_argument("--min-change", type=float, default=1.5, help="حداقل رشد ٪ در ۲ کندل")
    ap.add_argument("--volume-mult", type=float, default=5.0)
    ap.add_argument("--min-tests", type=int, default=7)
    ap.add_argument("--cooldown-days", type=float, default=5.0)
    ap.add_argument("--trade-usdt", type=float, default=50.0)
    ap.add_argument("--out", type=str, default="results")
    args = ap.parse_args()

    interval_minutes = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
    candle_min = interval_minutes.get(args.interval, 5)
    vwap_window = max(20, int(24 * 60 / candle_min))          # پنجره VWAP ~۱ روزه
    cooldown_candles = int(args.cooldown_days * 24 * 60 / candle_min)

    cfg = {
        "min_change_pct": args.min_change, "volume_mult": args.volume_mult,
        "rsi_min": 45, "rsi_max": 70, "min_tests": args.min_tests,
        "atr_mult": 3.0, "trail_activate_pct": 10.0, "hard_stop_pct": 5.0,
        "trade_usdt": args.trade_usdt, "cooldown_candles": cooldown_candles,
    }

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)

    symbols = TOP200[: args.symbols]
    log.info(f"🐂 Bull Hunter v2 — بک‌تست واقعی")
    log.info(f"   بازه: {args.days} روز | تایم‌فریم: {args.interval} | ارزها: {len(symbols)}")
    log.info(f"   از {datetime.fromtimestamp(start_ms/1000, timezone.utc)} تا {datetime.fromtimestamp(end_ms/1000, timezone.utc)}")

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    # ── تست اتصال اولیه به Binance (برای عیب‌یابی مسدود بودن IP) ─────────────
    try:
        ping = session.get(f"{BINANCE_BASE}/ping", timeout=10)
        DEBUG_LOG.append(f"[ping] status={ping.status_code} body={ping.text[:150]}")
        server_time = session.get(f"{BINANCE_BASE}/time", timeout=10)
        DEBUG_LOG.append(f"[time] status={server_time.status_code} body={server_time.text[:150]}")
        exch_info = session.get(f"{BINANCE_BASE}/exchangeInfo", params={"symbol": "BTCUSDT"}, timeout=10)
        DEBUG_LOG.append(f"[exchangeInfo BTCUSDT] status={exch_info.status_code} body={exch_info.text[:300]}")
    except Exception as e:
        DEBUG_LOG.append(f"[connectivity test] {type(e).__name__}: {e}")
    log.info(f"تست اتصال Binance: {DEBUG_LOG[-3:] if len(DEBUG_LOG)>=3 else DEBUG_LOG}")

    all_results = []
    for idx, sym in enumerate(symbols, 1):
        log.info(f"[{idx}/{len(symbols)}] دریافت داده واقعی {sym}...")
        raw = fetch_klines(sym, args.interval, start_ms, end_ms, session)
        df = klines_to_df(raw)
        if df.empty or len(df) < 100:
            log.info(f"   ⚠️  داده ناکافی برای {sym} ({len(df)} کندل) — رد شد")
            continue
        df = compute_indicators(df, vwap_window)
        result = backtest_symbol(sym, df, cfg)
        n_conf = sum(1 for s in result["signals"] if s["confirmed"])
        n_trades = len(result["trades"])
        if n_conf > 0 or n_trades > 0:
            log.info(f"   ✅ {sym}: {len(df)} کندل | {n_conf} سیگنال تأیید | {n_trades} معامله")
        all_results.append(result)
        time.sleep(0.1)

    # ── تجمیع نتایج ──────────────────────────────────────────────────────────
    all_trades = [t for r in all_results for t in r["trades"]]
    all_signals_confirmed = sum(1 for r in all_results for s in r["signals"] if s["confirmed"])
    all_signals_total = sum(len(r["signals"]) for r in all_results)

    wins = [t for t in all_trades if t["pnl_pct"] >= 0]
    losses = [t for t in all_trades if t["pnl_pct"] < 0]
    total_pnl_usdt = sum(t["pnl_usdt"] for t in all_trades)
    win_rate = (len(wins) / len(all_trades) * 100) if all_trades else 0
    gross_profit = sum(t["pnl_usdt"] for t in wins)
    gross_loss = abs(sum(t["pnl_usdt"] for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf") if gross_profit > 0 else 0

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config": cfg,
        "period_days": args.days,
        "interval": args.interval,
        "symbols_tested": len(symbols),
        "symbols_with_data": len(all_results),
        "total_signals": all_signals_total,
        "confirmed_signals": all_signals_confirmed,
        "total_trades": len(all_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "total_pnl_usdt": round(total_pnl_usdt, 3),
        "profit_factor": round(profit_factor, 3) if profit_factor != float("inf") else "inf",
        "capital_deployed_usdt": round(len(all_trades) * args.trade_usdt, 2),
        "return_pct": round(
            total_pnl_usdt / (len(all_trades) * args.trade_usdt) * 100, 3
        ) if all_trades else 0,
    }

    out_dir = Path(args.out)
    out_dir.mkdir(exist_ok=True, parents=True)

    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(out_dir / "trades.json", "w", encoding="utf-8") as f:
        json.dump(all_trades, f, ensure_ascii=False, indent=2)

    # گزارش مارک‌داون خوانا
    md = [
        f"# گزارش بک‌تست واقعی Bull Hunter v2\n",
        f"تولید شده: {summary['generated_at_utc']}\n",
        f"- بازه: {args.days} روز | تایم‌فریم: {args.interval}",
        f"- ارزهای بررسی‌شده: {summary['symbols_with_data']}/{summary['symbols_tested']}",
        f"- کل سیگنال: {summary['total_signals']} | سیگنال تأیید‌شده: {summary['confirmed_signals']}",
        f"- معاملات: {summary['total_trades']} | برنده: {summary['wins']} | بازنده: {summary['losses']}",
        f"- نرخ برد: {summary['win_rate_pct']}%",
        f"- Profit Factor: {summary['profit_factor']}",
        f"- سود/زیان کل: ${summary['total_pnl_usdt']:+.2f}",
        f"- بازده: {summary['return_pct']:+.2f}%\n",
        f"## معاملات\n",
        f"| ارز | ورود | خروج | PnL% | PnL$ | دلیل |",
        f"|---|---|---|---|---|---|",
    ]
    for t in sorted(all_trades, key=lambda x: x["entry_time"]):
        md.append(f"| {t['symbol']} | {t['entry_time'][:16]} | {t['exit_time'][:16]} | "
                   f"{t['pnl_pct']:+.2f}% | ${t['pnl_usdt']:+.2f} | {t['reason']} |")

    with open(out_dir / "report.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    # ── ذخیره لاگ عیب‌یابی (شامل تست اتصال + خطاهای هر نماد) ─────────────────
    # نکته: نام فایل عمداً .txt است نه .log چون .gitignore الگوی *.log را نادیده می‌گیرد
    with open(out_dir / "debug_info.txt", "w", encoding="utf-8") as f:
        f.write(f"اجرا در: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"تعداد رکورد لاگ: {len(DEBUG_LOG)}\n")
        f.write("=" * 60 + "\n")
        f.write("\n".join(DEBUG_LOG))

    log.info("=" * 60)
    log.info(f"✅ بک‌تست تمام شد — {summary['total_trades']} معامله | "
             f"نرخ برد {summary['win_rate_pct']}% | PnL ${summary['total_pnl_usdt']:+.2f}")
    log.info(f"   نتایج در {out_dir}/ ذخیره شد")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
