#!/usr/bin/env python3
"""
ارزیابی خارج از نمونه (OOS) — خروج فعلی (ATR Trail) در برابر خروج ترکیبی (Ladder + ATR Trail)

دو حالت:
  --mode hist     داده‌ی N روز گذشته؛ بخش «دیده‌نشده» (قبل از --seen-cutoff) جداگانه گزارش می‌شود
  --mode forward  از --start (روی لیست نمادهای فریزشده) تا الان؛ برای تست رو‌به‌جلو

اصول:
  * دریافت داده با retry/backoff؛ هر نماد ناقص (قطع‌شدن وسط دریافت) کنار گذاشته و گزارش می‌شود، نه اینکه بی‌صدا کوتاه شود.
  * کندلِ ناتمام (close_time > الان) حذف می‌شود.
  * هر دو حالت خروج با همان backtest_symbol و همان محدودیت پورتفولیو (۱۵ معامله، ۱۰۰/۴۰ دلار) اجرا می‌شوند.
  * مقایسه‌ی جفتی روی ورودی‌های یکسان با یک بازپخش مستقل (replay) انجام می‌شود و پایه‌ی آن با خروجی ماژول سنجیده می‌شود (assert).
  * هیچ داده‌ی ساختگی یا ثابتی وجود ندارد؛ همه‌چیز از Binance یا از فایل کش واقعی می‌آید.
"""
import argparse, json, math, os, subprocess, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import backtest_real as B  # noqa: E402

INTERVAL_MIN = 5
CANDLE_MS = INTERVAL_MIN * 60 * 1000
VWAP_WINDOW = 288
COOLDOWN_CANDLES = 1440          # ۵ روز
MAX_OPEN, RELIABLE_USDT, OTHER_USDT = 15, 100.0, 40.0
FEE_PER_SIDE = 0.001             # فرض: ۰٫۱٪ هر طرف (تخمین؛ داده‌ی واقعی کارمزد نیست)


def final_cfg(mode: str) -> dict:
    """پارامترهای فریزشده‌ی نهایی. فقط exit_mode بین دو حالت فرق دارد."""
    return B.build_cfg(3.0, 2.0, 5, COOLDOWN_CANDLES, 50.0, t9_max_dist_pct=-7.0,
                       max_hold_candles=48, hard_stop_atr_mult=2.5, exit_mode=mode)


# ───────────────────────────── دریافت داده ─────────────────────────────
class Fetcher:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json"})
        self.base = B.BINANCE_BASE
        B._active_base["url"] = self.base

    def get(self, path, params=None, tries=8):
        last = None
        for k in range(tries):
            try:
                r = self.s.get(f"{self.base}/{path}", params=params, timeout=25)
            except requests.RequestException as e:
                last = f"{type(e).__name__}: {e}"; time.sleep(min(60, 2 ** k)); continue
            if r.status_code == 451 and self.base == B.BINANCE_BASE:
                self.base = B.BINANCE_MIRROR; B._active_base["url"] = self.base; continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):
                wait = int(r.headers.get("Retry-After", 5 * 2 ** k)); last = f"HTTP {r.status_code}"
                time.sleep(min(wait, 300)); continue
            if r.status_code >= 500:
                last = f"HTTP {r.status_code}"; time.sleep(min(60, 2 ** k)); continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        raise RuntimeError(f"retries exhausted ({last})")

    def klines(self, symbol, start_ms, end_ms):
        out, cur = [], start_ms
        while cur < end_ms:
            batch = self.get("klines", {"symbol": symbol, "interval": "5m", "startTime": cur,
                                        "endTime": end_ms, "limit": 1000})
            if not batch:
                break
            out.extend(batch)
            nxt = batch[-1][0] + 1
            if nxt <= cur:
                break
            cur = nxt
            if len(batch) < 1000:
                break
            time.sleep(0.05)
        return out


def fetch_all(symbols, start_ms, end_ms, log):
    f = Fetcher(); now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    raw, quality = {}, {"failed": {}, "short": {}, "gaps": {}}
    for i, sym in enumerate(symbols, 1):
        try:
            rows = f.klines(sym, start_ms, end_ms)
        except Exception as e:                       # ناقص‌ماندن = حذف نماد (نه داده‌ی کوتاه‌شده‌ی بی‌صدا)
            quality["failed"][sym] = str(e); log(f"[{i}/{len(symbols)}] {sym}: FAILED {e}"); continue
        rows = [r for r in rows if r[6] <= now_ms]    # فقط کندل‌های تمام‌شده
        df = B.klines_to_df(rows)
        if df.empty or len(df) < 288 + 100:
            quality["short"][sym] = int(len(df)); continue
        df = df.drop_duplicates("dt").sort_values("dt").reset_index(drop=True)
        d = df["dt"].diff().dropna().dt.total_seconds() / 60
        ngap = int((d > INTERVAL_MIN).sum())
        if ngap:
            quality["gaps"][sym] = {"n": ngap, "max_candles": int(d.max() / INTERVAL_MIN)}
        raw[sym] = df[["dt", "open", "high", "low", "close", "volume"]]
        if i % 20 == 0:
            log(f"[{i}/{len(symbols)}] fetched, {len(raw)} symbols kept")
        time.sleep(0.05)
    return raw, quality


def save_cache(path, raw):
    parts = []
    for sym, df in raw.items():
        d = df.copy(); d["symbol"] = sym; parts.append(d)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.concat(parts, ignore_index=True).to_csv(path, index=False, compression="gzip")


# ───────────────────────────── معیارها ─────────────────────────────
def metrics(acc, label=""):
    if not acc:
        return {"label": label, "n": 0}
    df = pd.DataFrame(acc)
    usd = df["actual_pnl_usdt"].astype(float); size = df["actual_size"].astype(float)
    gp = float(usd[usd > 0].sum()); gl = float(-usd[usd <= 0].sum())
    fee = float((size * 2 * FEE_PER_SIDE).sum())
    order = df.assign(_x=pd.to_datetime(df["exit_time"]), _u=usd).sort_values("_x")
    eq = np.r_[0.0, order["_u"].cumsum().values]; dd = float((np.maximum.accumulate(eq) - eq).max())
    by = df.assign(_u=usd).groupby("reason").agg(n=("_u", "size"), usd=("_u", "sum"), mean_pct=("pnl_pct", "mean"))
    return {"label": label, "n": int(len(df)), "win_pct": round(float((df["pnl_pct"] > 0).mean() * 100), 2),
            "net_usd": round(float(usd.sum()), 2), "gross_profit": round(gp, 2), "gross_loss": round(-gl, 2),
            "profit_factor": round(gp / gl, 3) if gl > 0 else None, "fee_estimate_usd": round(fee, 2),
            "net_after_fee_usd": round(float(usd.sum()) - fee, 2), "max_drawdown_usd": round(dd, 2),
            "still_open_at_end": int((df["reason"].str.startswith("End of Data")).sum()),
            "by_reason": {k: {"n": int(v.n), "usd": round(float(v.usd), 2), "mean_pct": round(float(v.mean_pct), 2)}
                          for k, v in by.iterrows()}}


def run_mode(dfs, mode):
    return B.run_backtest_on_cache(dfs, final_cfg(mode))["trades"]


def window_port(trades, entry_lt=None, entry_ge=None):
    if entry_lt is not None:
        trades = [t for t in trades if t["entry_time"] < entry_lt]
    if entry_ge is not None:
        trades = [t for t in trades if t["entry_time"] >= entry_ge]
    port = B.apply_portfolio_capacity_weighted(trades, MAX_OPEN, RELIABLE_USDT, OTHER_USDT)
    return trades, port


def parts_table(acc_by_mode, t0, part_days):
    out = []
    if not acc_by_mode:
        return out
    t_end = max(pd.to_datetime(t["entry_time"]) for acc in acc_by_mode.values() for t in acc) if any(acc_by_mode.values()) else t0
    k = 0
    while t0 + timedelta(days=k * part_days) <= t_end:
        a, b = t0 + timedelta(days=k * part_days), t0 + timedelta(days=(k + 1) * part_days)
        row = {"part": k + 1, "from": str(a.date()), "to": str(b.date())}
        for mode, acc in acc_by_mode.items():
            sub = [t for t in acc if a <= pd.to_datetime(t["entry_time"]) < b]
            row[mode] = {"n": len(sub), "net_usd": round(sum(t["actual_pnl_usdt"] for t in sub), 2)}
        out.append(row); k += 1
    return out


# ───────────────── مقایسه‌ی جفتی با بازپخش مستقل ─────────────────
def replay(c, i0, atr, mode, cfg):
    """بازپخش خروج از یک ورودی مشخص. mode='atr' منطق فعلی، 'hybrid' منطق ترکیبی؛ همه‌چیز روی قیمت بسته‌شدن."""
    e = c[i0]; hs = e - cfg["hard_stop_atr_mult"] * atr; peak = -1e9; hi = e; on = False; tr = None
    atr_pct = atr / e * 100
    for i in range(i0 + 1, len(c)):
        p = c[i]; pnl = (p - e) / e * 100
        if mode == "atr":
            hi = max(hi, p)
            if not on and pnl >= cfg["trail_activate_pct"]:
                on = True; tr = hi - cfg["atr_mult"] * atr
            elif on:
                ns = hi - cfg["atr_mult"] * atr
                if ns > tr: tr = ns
            if on and p <= tr: return i, pnl
        else:
            peak = max(peak, pnl)
            sp = B.hybrid_stop_pct(peak, atr_pct, cfg["trail_activate_pct"], cfg["atr_mult"])
            on = sp is not None
            if on and pnl <= sp: return i, pnl
        if p <= hs: return i, pnl
        if cfg["max_hold_candles"] and not on and i - i0 >= cfg["max_hold_candles"]: return i, pnl
    return len(c) - 1, (c[-1] - e) / e * 100


def paired(dfs, atr_trades, seed=7):
    cfg_a, cfg_h = final_cfg("atr"), final_cfg("hybrid")
    idx = {s: pd.Series(np.arange(len(d)), index=d["dt"]) for s, d in dfs.items()}
    rows, mism = [], 0
    for t in atr_trades:
        d = dfs[t["symbol"]]; i0 = int(idx[t["symbol"]][pd.to_datetime(t["entry_time"])])
        c = d["close"].values; atr = float(d["atr10"].iloc[i0])
        ia, pa = replay(c, i0, atr, "atr", cfg_a)
        if abs(pa - t["pnl_pct"]) > 5e-3:                   # (pnl_pct در ماژول تا ۳ رقم گرد می‌شود)
            mism += 1
        _, ph = replay(c, i0, atr, "hybrid", cfg_h)
        sz = RELIABLE_USDT if t["symbol"] in B.RELIABLE_SYMBOLS else OTHER_USDT
        rows.append({"symbol": t["symbol"], "entry_time": t["entry_time"], "atr_pct": t["pnl_pct"], "hybrid_pct": ph,
                     "size": sz, "diff_usd": (ph - pa) / 100 * sz})
    if mism:
        raise AssertionError(f"baseline replay disagrees with module on {mism}/{len(atr_trades)} trades")
    X = pd.DataFrame(rows)
    if X.empty:
        return X, {}
    X["day"] = pd.to_datetime(X["entry_time"]).dt.floor("D")
    m = X["diff_usd"].mean(); se = X["diff_usd"].std(ddof=1) / math.sqrt(len(X)) if len(X) > 1 else float("nan")
    rng = np.random.default_rng(seed); days = X["day"].unique(); g = {d: X["diff_usd"][X["day"] == d].values for d in days}
    bs = [np.concatenate([g[d] for d in rng.choice(days, len(days))]).sum() for _ in range(3000)]
    lo, hi = np.percentile(bs, [2.5, 97.5])
    return X, {"n": int(len(X)), "total_diff_usd": round(float(X["diff_usd"].sum()), 2), "per_trade_usd": round(float(m), 4),
               "se": round(float(se), 4), "t": round(float(m / se), 2) if se and not math.isnan(se) else None,
               "bootstrap95_total": [round(float(lo), 1), round(float(hi), 1)],
               "better": int((X["diff_usd"] > 1e-9).sum()), "worse": int((X["diff_usd"] < -1e-9).sum()),
               "baseline_replay_matches_module": True}


# ───────────────────────────── main ─────────────────────────────
def git_sha():
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["hist", "forward"], required=True)
    ap.add_argument("--label", default="run")
    ap.add_argument("--days", type=int, default=300)
    ap.add_argument("--start", default=None, help="forward: UTC date YYYY-MM-DD؛ اولین روز معاملات")
    ap.add_argument("--symbols-file", default=None)
    ap.add_argument("--n-symbols", type=int, default=200)
    ap.add_argument("--seen-cutoff", default="2026-05-24 22:55:00", help="hist: ورودی‌های قبل از این زمان «دیده‌نشده» حساب می‌شوند")
    ap.add_argument("--part-days", type=int, default=30)
    ap.add_argument("--cache-in", default=None, help="به‌جای دریافت از Binance از این کش واقعی بخوان")
    ap.add_argument("--cache-out", default=None)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    log = lambda m: print(datetime.now(timezone.utc).strftime("%H:%M:%S"), m, flush=True)
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)

    quality = {}
    if a.cache_in:
        raw = B.load_raw_cache(Path(a.cache_in)); symbols = list(raw)
        log(f"cache loaded: {len(raw)} symbols")
    else:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000) // CANDLE_MS * CANDLE_MS
        if a.mode == "forward":
            start_day = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
            start_ms = int((start_day - timedelta(days=1)).timestamp() * 1000)   # ۲۴ ساعت warm-up
            if end_ms < int(start_day.timestamp() * 1000) + CANDLE_MS * 12:
                log("forward window has not started yet — only freezing the symbol list")
                start_ms = None
        else:
            start_ms = end_ms - a.days * 86400 * 1000
        # لیست نمادها
        sf = Path(a.symbols_file) if a.symbols_file else None
        if sf and sf.exists():
            symbols = json.loads(sf.read_text())["symbols"]; log(f"symbols loaded from {sf}: {len(symbols)}")
        else:
            B.DEBUG_LOG.clear()
            symbols = B.fetch_top_symbols(a.n_symbols, Fetcher().s)
            if any("fallback" in m for m in B.DEBUG_LOG) or len(symbols) < a.n_symbols * 0.9:
                raise RuntimeError(f"live top-symbol list unavailable: {B.DEBUG_LOG[-3:]}")
            if sf:
                sf.parent.mkdir(parents=True, exist_ok=True)
                sf.write_text(json.dumps({"frozen_at_utc": datetime.now(timezone.utc).isoformat(),
                                          "selection": "top by 24h quoteVolume, USDT pairs (backtest_real.fetch_top_symbols)",
                                          "symbols": symbols}, indent=1))
                log(f"symbols frozen -> {sf}")
        if start_ms is None:
            return
        raw, quality = fetch_all(symbols, start_ms, end_ms, log)
        if a.cache_out:
            save_cache(a.cache_out, raw)
    # نمادهای با داده‌ی ناقص در انتهای بازه (قطع دریافت) حذف می‌شوند
    t_end = max(df["dt"].max() for df in raw.values())
    stale = [s for s, d in raw.items() if (t_end - d["dt"].max()).total_seconds() > 3600]
    for s in stale:
        raw.pop(s); quality.setdefault("truncated_dropped", []).append(s)
    dfs = {s: B.compute_indicators(d.reset_index(drop=True), VWAP_WINDOW) for s, d in raw.items()}
    t_start = min(df["dt"].min() for df in dfs.values())
    first_signal = t_start + timedelta(minutes=288 * INTERVAL_MIN)
    log(f"data {t_start} .. {t_end} | {len(dfs)} symbols | first possible signal {first_signal}")

    summary = {"label": a.label, "mode": a.mode, "git_sha": git_sha(), "generated_at_utc": datetime.now(timezone.utc).isoformat(),
               "data": {"from": str(t_start), "to": str(t_end), "symbols": len(dfs), "symbol_list": sorted(dfs), "first_possible_signal": str(first_signal)},
               "data_quality": quality, "fee_assumption_per_side": FEE_PER_SIDE,
               "frozen_params": {"min_change": 3.0, "volume_mult": 2.0, "min_tests": 5, "cooldown_days": 5, "t9": -7.0,
                                 "max_hold_hours": 4.0, "hard_stop_atr_mult": 2.5, "max_open": MAX_OPEN,
                                 "reliable_usdt": RELIABLE_USDT, "other_usdt": OTHER_USDT}}
    windows = {"full": dict()}
    if a.mode == "hist":
        windows["unseen_before_cutoff"] = dict(entry_lt=a.seen_cutoff)
        windows["seen_from_cutoff"] = dict(entry_ge=a.seen_cutoff)
        summary["seen_cutoff"] = a.seen_cutoff
    raw_trades = {m: run_mode(dfs, m) for m in ("atr", "hybrid")}
    log(f"backtests done: atr {len(raw_trades['atr'])} raw trades, hybrid {len(raw_trades['hybrid'])}")
    for wname, kw in windows.items():
        summary[wname] = {}
        acc_by_mode = {}
        for mode in ("atr", "hybrid"):
            trades, port = window_port(raw_trades[mode], **kw)
            summary[wname][mode] = metrics(port["trades"], f"{wname}/{mode}")
            acc_by_mode[mode] = port["trades"]
            if wname == "full":
                pd.DataFrame(port["trades"]).to_csv(out / f"trades_{mode}_accepted.csv", index=False)
        base = first_signal if wname != "seen_from_cutoff" else pd.to_datetime(a.seen_cutoff)
        summary[wname]["parts"] = parts_table(acc_by_mode, base, a.part_days if a.mode == "hist" else 14)
        d_net = summary[wname]["hybrid"].get("net_usd", 0) - summary[wname]["atr"].get("net_usd", 0)
        summary[wname]["hybrid_minus_atr_net_usd"] = round(d_net, 2)
        log(f"{wname}: atr {summary[wname]['atr'].get('net_usd')} | hybrid {summary[wname]['hybrid'].get('net_usd')}")
    # مقایسه‌ی جفتی روی ورودی‌های حالت فعلی (کل بازه + بخش دیده‌نشده)
    X, pstat = paired(dfs, raw_trades["atr"])
    summary["paired_same_entries_full"] = pstat
    if a.mode == "hist":
        Xu = X[X["entry_time"] < a.seen_cutoff]
        if len(Xu) > 1:
            m = Xu["diff_usd"].mean(); se = Xu["diff_usd"].std(ddof=1) / math.sqrt(len(Xu))
            summary["paired_same_entries_unseen"] = {"n": int(len(Xu)), "total_diff_usd": round(float(Xu["diff_usd"].sum()), 2),
                                                      "per_trade_usd": round(float(m), 4), "se": round(float(se), 4), "t": round(float(m / se), 2)}
    X.drop(columns=["day"]).to_csv(out / "paired_same_entries.csv", index=False)
    (out / "summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False, default=str))
    log(f"done -> {out}/summary.json")


if __name__ == "__main__":
    main()
