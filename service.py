"""lytrade service — 多策略并行模拟交易 + HTTP API（服务器常驻版）

策略池（并行独立虚拟账户）:
  supertrend  ATR 通道 + ADX 过滤（趋势）   抄 freqtrade-strategies 社区思路
  ma          MA5/20 交叉（基线对照组）
  pairs       Z-score 均值回归（震荡市）     抄 wb-finance-skill pair_trading 方法论

引擎线程每 5 分钟: quote/kline → 各策略信号 → 各自虚拟账户撮合
API: GET /v1/strategies 绩效对比 · GET /v1/{s}/positions · GET /v1/{s}/trades
鉴权: 两级 —
  管理员: X-API-Token == LYTRADE_TOKEN（env，兼容旧配置）→ 全权限 + 发放令牌
  用户:   /v1/admin/tokens 发放的令牌（哈希入库，可设有效期/可撤销）→ 只读数据
⚠️ 仅供策略学习与验证，不构成投资建议；绝不调用真实下单接口。
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
import sqlite3
import statistics
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import lytrade as lt

BASE = Path(__file__).resolve().parent
CFG = yaml.safe_load((BASE / "config.yaml").read_text(encoding="utf-8"))
DB_PATH = BASE / "data" / "service.db"
TOKEN = os.environ.get("LYTRADE_TOKEN", "")
INTERVAL = int(os.environ.get("LYTRADE_INTERVAL", "60"))
STRATEGIES = ["supertrend", "ma", "pairs", "arb", "bb",
              "macd", "psar", "rsi", "dualthrust"]
PAIRS = [("00700.HK", "09988.HK")]          # 同行业高相关对（v1 固定）

lt.DB_PATH = DB_PATH
LTOKEN = os.environ.get("LYSOURCE_TOKEN", "")

# 面板只读缓存（引擎线程写 / API 线程读；整体替换避免读到半截状态）
MARKET_CACHE: dict = {}   # {symbol: {name, price, chg_pct, closes:[近90收盘]}}
HEAT_CACHE: dict = {}     # {heat:{名称:0-1}, signals:{策略:{sym:sig}}, ts}


# ---------------------------------------------------------------- storage
def sdb() -> sqlite3.Connection:
    """service 自己的固定 DB（api_tokens），不受引擎切换 lt.DB_PATH 影响。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    lt.init_db()
    with sdb() as c:
        c.execute("CREATE TABLE IF NOT EXISTS strategy_state("
                  "strategy TEXT PRIMARY KEY, cash REAL, equity REAL, updated REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS api_tokens("
                  "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                  "token_hash TEXT UNIQUE NOT NULL,"     # sha256(token)
                  "owner TEXT NOT NULL,"
                  "note TEXT DEFAULT '',"
                  "created_at REAL NOT NULL,"
                  "expires_at REAL,"                      # NULL = 永久
                  "revoked INTEGER NOT NULL DEFAULT 0)")


def acct(strategy: str) -> str:
    """每策略独立记账: 通过 strategy 前缀隔离 trades/positions 行。"""
    return strategy


# ---------------------------------------------------------------- auth (两级)
def _token_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def require_token(x_api_token: str = Header(default="")) -> None:
    """数据接口鉴权: 管理员 env token 或有效用户令牌。"""
    if not TOKEN:
        return
    if x_api_token == TOKEN:
        return
    with sdb() as c:
        row = c.execute(
            "SELECT expires_at, revoked FROM api_tokens WHERE token_hash=?",
            (_token_hash(x_api_token),)).fetchone()
    if row and not row["revoked"] and (row["expires_at"] is None
                                       or row["expires_at"] > time.time()):
        return
    raise HTTPException(401, "invalid token")


def require_admin(x_api_token: str = Header(default="")) -> None:
    """管理接口鉴权: 仅管理员 env token。"""
    if TOKEN and x_api_token != TOKEN:
        raise HTTPException(401, "admin token required")


class TokenIn(BaseModel):
    owner: str                     # 归属人（邮箱/ID/备注名）
    days: int | None = None        # 有效期天数，None = 永久
    note: str = ""


# ---------------------------------------------------------------- pairs signal
def _zscore(a: list[float], b: list[float], lookback: int = 60,
            entry_z: float = 2.0, exit_z: float = 0.5) -> float:
    """价格比值 Z-score（抄 wb-finance-skill pair_trading 方法论，纯 python 版）。"""
    n = min(len(a), len(b))
    if n < lookback:
        return 0.0
    ratios = [a[-n + i] / b[-n + i] for i in range(n) if b[-n + i]]
    win = ratios[-lookback:]
    mean = statistics.mean(win)
    std = statistics.stdev(win)
    if std == 0:
        return 0.0
    return (ratios[-1] - mean) / std


def pairs_signal(kl_a: list[dict], kl_b: list[dict]) -> tuple[str, str]:
    """返回 (A 侧信号, B 侧信号)。比值偏低→买A；偏高→买B；|z|<0.5 平仓。"""
    z = _zscore([k["close"] for k in kl_a], [k["close"] for k in kl_b],
                lt.STRAT.get("pair_lookback", 60),
                lt.STRAT.get("pair_entry_z", 2.0), lt.STRAT.get("pair_exit_z", 0.5))
    if z <= -lt.STRAT.get("pair_entry_z", 2.0):
        return "BUY", "HOLD"
    if z >= lt.STRAT.get("pair_entry_z", 2.0):
        return "HOLD", "BUY"
    if abs(z) < lt.STRAT.get("pair_exit_z", 0.5):
        return "SELL", "SELL"
    return "HOLD", "HOLD"


# ---------------------------------------------------------------- arb (跨市场 ADR 套利)
ARB_CFG = CFG.get("arb", {})


def arb_signal(premiums: list[float]) -> tuple[str, float]:
    """溢价率序列 → (action, z)。
    premium = 港股×ADS比率/汇率 / 美股ADS价 - 1；偏高(港股贵)→做空溢价，偏低→做多溢价。"""
    lookback = ARB_CFG.get("lookback", 60)
    if len(premiums) < lookback + 1:
        return "WAIT", 0.0
    win = premiums[-lookback:]
    mean, std = statistics.mean(win), statistics.stdev(win)
    if std < 1e-9:
        return "WAIT", 0.0
    z = (premiums[-1] - mean) / std
    if abs(z) >= ARB_CFG.get("entry_z", 2.0):
        return ("OPEN_SHORT" if z > 0 else "OPEN_LONG"), z   # 对溢价率的方向
    if abs(z) <= ARB_CFG.get("exit_z", 0.5):
        return "CLOSE", z
    return "WAIT", z


def arb_cycle(klines: dict, quotes: dict) -> dict:
    """一轮跨市场套利: 09988.HK × BABA.US 溢价率均值回归。
    独立账本 svc_arb.db —— trades/equity 复用通用表结构（API 零改动），
    价差头寸存 spread_pos；PnL = (premium - entry) × dir × notional。"""
    lt.DB_PATH = BASE / "data" / "svc_arb.db"
    lt.init_db()
    a_sym, b_sym = ARB_CFG.get("pair", ["09988.HK", "BABA.US"])
    ratio, fx = ARB_CFG.get("ads_ratio", 8), ARB_CFG.get("fx_usdhkd", 7.8)
    ka, kb = klines.get(a_sym) or [], klines.get(b_sym) or []
    pa, pb = quotes.get(a_sym), quotes.get(b_sym)
    if not (pa and pb) or len(ka) < ARB_CFG.get("lookback", 60) + 1:
        return {"signals": {f"{a_sym}×{b_sym}": "WAIT"}, "equity": lt.ACC["initial_cash"]}

    # 溢价率序列（对齐两腿历史收盘）
    n = min(len(ka), len(kb))
    premiums = []
    for i in range(n):
        va, vb = ka[-n + i]["close"], kb[-n + i]["close"]
        if va and vb:
            premiums.append(va * ratio / fx / vb - 1)

    action, z = arb_signal(premiums)
    cur = pa * ratio / fx / pb - 1          # 实时溢价（含最新报价）
    pair_label = f"{a_sym}×{b_sym}"

    with lt.db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS spread_pos("
                  "id INTEGER PRIMARY KEY CHECK(id=1), dir INTEGER, "
                  "entry_premium REAL, notional REAL, ts REAL)")
        if not c.execute("SELECT 1 FROM account WHERE id=1").fetchone():
            c.execute("INSERT INTO account(id,cash) VALUES(1,?)",
                      (lt.ACC["initial_cash"],))
        row = c.execute("SELECT * FROM spread_pos WHERE id=1").fetchone()
        cash = c.execute("SELECT cash FROM account WHERE id=1").fetchone()["cash"]

        if action in ("OPEN_SHORT", "OPEN_LONG") and not row:
            notional = lt.ACC["initial_cash"] * ARB_CFG.get("notional_pct", 0.3)
            dirn = -1 if action == "OPEN_SHORT" else 1
            c.execute("INSERT OR REPLACE INTO spread_pos(id,dir,entry_premium,notional,ts)"
                      " VALUES(1,?,?,?,?)", (dirn, cur, notional, time.time()))
            c.execute("INSERT INTO trades(ts,symbol,side,qty,price,fee,note)"
                      " VALUES(?,?,?,?,?,?,?)",
                      (time.time(), pair_label, action, dirn, round(cur * 100, 3), 0,
                       f"z={z:.2f} notional={notional:.0f} 双腿模拟(多便宜腿+空贵腿)"))
            print(f"[arb] 开仓 {action} premium={cur:.3%} z={z:.2f}")
        elif action == "CLOSE" and row:
            pnl = (cur - row["entry_premium"]) * row["dir"] * row["notional"]
            cash += pnl
            c.execute("UPDATE account SET cash=? WHERE id=1", (cash,))
            c.execute("DELETE FROM spread_pos WHERE id=1")
            c.execute("INSERT INTO trades(ts,symbol,side,qty,price,fee,note)"
                      " VALUES(?,?,?,?,?,?,?)",
                      (time.time(), pair_label, "CLOSE", row["dir"],
                       round(cur * 100, 3), 0,
                       f"z={z:.2f} pnl={pnl:+.1f} 溢价回归平仓"))
            print(f"[arb] 平仓 premium={cur:.3%} z={z:.2f} pnl={pnl:+.1f}")

        row = c.execute("SELECT * FROM spread_pos WHERE id=1").fetchone()
        unreal = ((cur - row["entry_premium"]) * row["dir"] * row["notional"]) if row else 0.0
        total = cash + unreal
        c.execute("INSERT OR REPLACE INTO equity(ts,cash,market_value,total)"
                  " VALUES(?,?,?,?)", (time.time(), round(cash, 2),
                                       round(unreal, 2), round(total, 2)))
    sig = action if action != "WAIT" else ("HOLD" if row else "WAIT")
    return {"signals": {pair_label: sig, "premium": f"{cur:.2%}", "z": round(z, 2)},
            "equity": round(total, 2)}


# ---------------------------------------------------------------- backtest (空闲时段历史验证)
BT_CFG = CFG.get("backtest", {})   # 回测专用参数覆写（日线语义），与实时盘 5m 参数隔离


def _bt_metrics(equity: list[float], trades_n: int) -> dict:
    """净值序列 → 绩效指标（日频）。"""
    if len(equity) < 3 or equity[0] <= 0:
        return {}
    init = lt.ACC["initial_cash"]
    total_ret = equity[-1] / init - 1
    years = max(len(equity) / 252, 1e-9)
    # 空头无保证金约束时净值可能穿零, 防御负底数开方
    ann = (max(equity[-1], 1e-6) / init) ** (1 / years) - 1
    rets = [equity[i] / equity[i - 1] - 1 for i in range(1, len(equity)) if equity[i - 1] > 0]
    sd = statistics.stdev(rets) if len(rets) > 2 else 0.0
    sharpe = (statistics.mean(rets) / sd * math.sqrt(252)) if sd > 0 else 0.0
    peak, mdd = equity[0], 0.0
    for v in equity:
        peak = max(peak, v)
        if peak > 0:
            mdd = max(mdd, (peak - v) / peak)
    return {"total_return": f"{total_ret:+.2%}", "annual": f"{ann:+.2%}",
            "sharpe": round(sharpe, 2), "max_drawdown": f"{-mdd:.2%}", "trades": trades_n}


def _bt_signal_strategy(strategy: str, symbol: str) -> tuple[list, int]:
    """单策略单标的日线回测（临时 DB 隔离 + 参数覆写，不碰实时模拟盘账本）。"""
    kl = lt.fetch_kline(symbol, BT_CFG.get("kline_count", 500), "day")
    saved = dict(lt.STRAT)
    lt.STRAT.update(BT_CFG)                    # 覆写为日线语义参数
    try:
        warmup = max(48, lt.STRAT.get("adx_filter_warmup", 30), lt.STRAT.get("bb_period", 20),
                     lt.STRAT.get("macd_slow", 26) + lt.STRAT.get("macd_signal", 9),
                     lt.STRAT.get("dt_lookback", 20))
        if len(kl) < warmup + 2:
            return [], 0
        def fn(k):
            return {"ma": lambda: lt.ma_signal([x["close"] for x in k]),
                    "supertrend": lambda: lt.supertrend_signal(
                        [x["high"] for x in k], [x["low"] for x in k], [x["close"] for x in k]),
                    "bb": lambda: lt.bb_signal([x["close"] for x in k]),
                    "macd": lambda: lt.macd_signal([x["close"] for x in k]),
                    "psar": lambda: lt.psar_signal([x["high"] for x in k],
                                                   [x["low"] for x in k], [x["close"] for x in k]),
                    "rsi": lambda: lt.rsi_signal([x["close"] for x in k]),
                    "dualthrust": lambda: lt.dual_thrust_signal(k)}[strategy]()
        tmp = BASE / "data" / f"bt_{strategy}_{symbol.replace('.', '_')}.db"
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        old_db = lt.DB_PATH
        lt.DB_PATH = tmp
        lt.init_db()
        try:
            with lt.db() as c:
                c.execute("INSERT OR IGNORE INTO account(id,cash) VALUES(1,?)",
                          (lt.ACC["initial_cash"],))
            conn = lt.db()
            try:
                for i in range(warmup, len(kl)):
                    sig = fn(kl[:i + 1])
                    if sig in ("BUY", "SELL"):
                        lt.execute(conn, symbol, symbol, sig, kl[i]["close"], kl[i]["ts"],
                                   note=f"bt:{strategy}")
                        lt.mark_equity(conn, {symbol: kl[i]["close"]})
                eq = [[r["ts"] * 1000, r["total"]] for r in
                      conn.execute("SELECT ts,total FROM equity ORDER BY ts")]
                n = conn.execute("SELECT COUNT(*) n FROM trades").fetchone()["n"]
            finally:
                conn.close()
        finally:
            lt.DB_PATH = old_db
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        return eq, n
    finally:
        lt.STRAT.clear()
        lt.STRAT.update(saved)


def _bt_spread(ka: list, kb: list, transform, lookback: int,
               entry_z: float, exit_z: float) -> dict:
    """价差类回测（pairs/arb 同构）: Z-score 开平, 价差 PnL 直接记账。"""
    n = min(len(ka), len(kb))
    if n < lookback + 2:
        return {"equity": [], "metrics": {}}
    spread = [transform(ka[-n + i]["close"], kb[-n + i]["close"]) for i in range(n)]
    ts = [ka[-n + i]["ts"] for i in range(n)]
    cash = lt.ACC["initial_cash"]
    notional = lt.ACC["initial_cash"] * 0.3
    pos, eq, trades = None, [], 0
    for i in range(lookback, n):
        win = spread[i - lookback:i]
        mean, std = statistics.mean(win), statistics.stdev(win)
        if std < 1e-9:
            continue
        z = (spread[i] - mean) / std
        if pos is None and abs(z) >= entry_z:
            pos = {"dir": -1 if z > 0 else 1, "entry": spread[i]}
            trades += 1
        elif pos is not None and abs(z) <= exit_z:
            cash += (spread[i] - pos["entry"]) * pos["dir"] * notional
            pos = None
            trades += 1
        unreal = (spread[i] - pos["entry"]) * pos["dir"] * notional if pos else 0.0
        eq.append([ts[i] * 1000, round(cash + unreal, 2)])
    return {"equity": eq, "metrics": _bt_metrics([v for _, v in eq], trades)}


def backtest_all() -> dict:
    """全策略回测: 趋势/回归三策略 4 标的等权合成 + 价差类直接回测。日线。"""
    out = {}
    for strat in ("ma", "supertrend", "bb"):
        curves, tn = [], 0
        for s in CFG["symbols"]:
            eq, n = _bt_signal_strategy(strat, s["symbol"])
            if eq:
                curves.append(eq)
                tn += n
        if curves:
            n0 = min(len(c) for c in curves)
            init = lt.ACC["initial_cash"]
            merged = [[curves[0][i][0],
                       round(sum(c[i][1] / init for c in curves) / len(curves) * init, 2)]
                      for i in range(n0)]
            out[strat] = {"equity": merged, "metrics": _bt_metrics([v for _, v in merged], tn)}
    ka = {s["symbol"]: lt.fetch_kline(s["symbol"], BT_CFG.get("kline_count", 500), "day")
          for s in CFG["symbols"]}
    out["pairs"] = _bt_spread(ka["00700.HK"], ka["09988.HK"],
                              lambda a, b: a / b,
                              lt.STRAT.get("pair_lookback", 60),
                              lt.STRAT.get("pair_entry_z", 2.0),
                              lt.STRAT.get("pair_exit_z", 0.5))
    kb = lt.fetch_kline(ARB_CFG.get("pair", ["", "BABA.US"])[1],
                        BT_CFG.get("kline_count", 500), "day")
    out["arb"] = _bt_spread(ka["09988.HK"], kb,
                            lambda a, b: a * ARB_CFG.get("ads_ratio", 8)
                            / ARB_CFG.get("fx_usdhkd", 7.8) / b,
                            ARB_CFG.get("lookback", 60),
                            ARB_CFG.get("entry_z", 2.0),
                            ARB_CFG.get("exit_z", 0.5))
    return out


# ---------------------------------------------------------------- engine
def run_strategy(strat: str, klines: dict, quotes: dict, heat: dict) -> dict:
    """单策略一轮: 独立 DB 账户 → 信号 → 撮合 → 净值。"""
    if strat == "arb":
        return arb_cycle(klines, quotes)   # 价差头寸市场中性, 不吃热度过滤
    lt.DB_PATH = BASE / "data" / f"svc_{strat}.db"
    lt.init_db()
    sigs: dict[str, str] = {}
    if strat == "pairs":
        for a, b in PAIRS:
            if a in klines and b in klines and len(klines[a]) > 60 and len(klines[b]) > 60:
                sa, sb = pairs_signal(klines[a], klines[b])
                sigs[a], sigs[b] = sa, sb
    else:
        fn = {"supertrend": lt.supertrend_signal,
              "ma": lambda h, l, cl: lt.ma_signal(cl),
              "bb": lambda h, l, cl: lt.bb_signal(cl),
              "macd": lambda h, l, cl: lt.macd_signal(cl),
              "psar": lt.psar_signal,
              "rsi": lambda h, l, cl: lt.rsi_signal(cl)}[strat]
        for s in CFG["symbols"]:
            kl = klines[s["symbol"]]
            if len(kl) > lt.STRAT["slow"] + 2:
                sig = (lt.dual_thrust_signal(kl) if strat == "dualthrust"
                       else fn([k["high"] for k in kl],
                               [k["low"] for k in kl],
                               [k["close"] for k in kl]))
                sigs[s["symbol"]] = sig
    with lt.db() as c:
        for sym, sig in sigs.items():
            if sig in ("BUY", "SELL") and quotes.get(sym):
                if strat == "pairs" and sig == "SELL":
                    p = c.execute("SELECT qty FROM positions WHERE symbol=?", (sym,)).fetchone()
                    if not p or p["qty"] <= 0:
                        continue   # pairs 的 SELL 仅为平仓语义, 无多仓不动作
                name = next(s["name"] for s in CFG["symbols"] if s["symbol"] == sym)
                if sig == "BUY" and heat.get(name, 0) < lt.STRAT.get("event_heat_min", 0.3):
                    print(f"[{strat}] {sym} BUY拦截: heat={heat.get(name, 0):.2f} "
                          f"< {lt.STRAT.get('event_heat_min')}")
                    sig = "HOLD"   # 资讯热度不足，禁止开仓
                lt.execute(c, sym, name, sig, quotes[sym], time.time(),
                           note=f"{strat} heat={heat.get(name, 0):.2f}")
        total = lt.mark_equity(c, quotes)
    return {"signals": sigs, "equity": round(total, 2)}


def run_cycle() -> dict:
    """一轮全策略（行情共享拉取，省 API 配额）。"""
    klines, quotes = {}, {}
    syms = [s["symbol"] for s in CFG["symbols"]]
    if ARB_CFG.get("enabled"):
        syms += [x for x in ARB_CFG.get("pair", []) if x not in syms]
    for sym in syms:
        kl = lt.fetch_kline(sym, lt.STRAT["kline_count"],
                            lt.STRAT.get("kline_period", "day"))
        q = lt.fetch_quote(sym)
        if q and q.get("price"):
            kl.append({"ts": q["ts"], "close": q["price"], "open": 0, "high": 0, "low": 0})
            quotes[sym] = q["price"]
        klines[sym] = kl
    heat = {s["name"]: lt.event_heat([s["name"], s["symbol"]]) for s in CFG["symbols"]}
    out = {strat: run_strategy(strat, klines, quotes, heat) for strat in STRATEGIES}
    out["_heat"] = {k: round(v, 3) for k, v in heat.items()}  # 观测: 每轮日志可见热度

    # 面板缓存: 行情 sparkline + 日涨跌 + 热度/信号快照
    mkt = {}
    for s in CFG["symbols"]:
        sym = s["symbol"]
        closes = [k["close"] for k in (klines.get(sym) or []) if k.get("close")][-90:]
        q = quotes.get(sym)
        # longbridge 日线含今日实时K，quote 又拼一根 → 有 quote 时昨收取 [-3]
        base = closes[-3] if q else closes[-2]
        chg = round((q / base - 1) * 100, 2) if q and base else 0.0
        mkt[sym] = {"name": s["name"], "price": q, "chg_pct": chg, "closes": closes}
    MARKET_CACHE.clear(); MARKET_CACHE.update(mkt)
    HEAT_CACHE.clear()
    HEAT_CACHE.update({"heat": out["_heat"],
                       "signals": {st: out[st]["signals"] for st in STRATEGIES},
                       "ts": time.time()})
    return out


def engine_loop() -> None:
    init_db()
    while True:
        try:
            s = run_cycle()
            print(f"[{time.strftime('%H:%M:%S')}] {json.dumps(s, ensure_ascii=False)}",
                  flush=True)
        except Exception as exc:
            print(f"[engine] cycle error: {exc}", flush=True)
        time.sleep(INTERVAL)


# ---------------------------------------------------------------- api
@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    t = threading.Thread(target=engine_loop, daemon=True)
    t.start()
    yield


app = FastAPI(title="lytrade", version="0.3.2",
              description="多策略并行模拟交易 · Supertrend/MA/Pairs",
              lifespan=lifespan)

# 面板可能从本地预览/其他域打开，读接口有 Token 鉴权，CORS 放开
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------- api (管理: 令牌发放)
@app.post("/v1/admin/tokens", dependencies=[Depends(require_admin)])
def admin_create_token(body: TokenIn):
    """管理员发放用户令牌（明文仅此一次返回）。"""
    raw = secrets.token_urlsafe(24)
    expires = time.time() + body.days * 86400 if body.days else None
    with sdb() as c:
        c.execute(
            "INSERT INTO api_tokens(token_hash,owner,note,created_at,expires_at)"
            " VALUES(?,?,?,?,?)",
            (_token_hash(raw), body.owner, body.note, time.time(), expires))
    return {"token": raw, "owner": body.owner,
            "expires_at": expires, "days": body.days}


@app.get("/v1/admin/tokens", dependencies=[Depends(require_admin)])
def admin_list_tokens():
    with sdb() as c:
        rows = c.execute(
            "SELECT id, owner, note, created_at, expires_at, revoked"
            " FROM api_tokens ORDER BY id DESC").fetchall()
    return {"tokens": [dict(r) for r in rows]}


@app.delete("/v1/admin/tokens/{tid}", dependencies=[Depends(require_admin)])
def admin_revoke_token(tid: int):
    with sdb() as c:
        cur = c.execute("UPDATE api_tokens SET revoked=1 WHERE id=?", (tid,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"token id {tid} not found")
    return {"ok": True, "revoked": tid}


@app.get("/healthz")
def healthz():
    return {"ok": True, "time": time.time()}


@app.get("/v1/strategies", dependencies=[Depends(require_token)])
def strategies():
    """多策略绩效对比（每策略独立虚拟账户 DB）。"""
    init = lt.ACC["initial_cash"]
    out = {}
    for strat in STRATEGIES:
        lt.DB_PATH = BASE / "data" / f"svc_{strat}.db"
        lt.init_db()
        with lt.db() as c:
            eq_rows = [r["total"] for r in
                       c.execute("SELECT total FROM equity ORDER BY ts").fetchall()]
            eq = eq_rows[-1] if eq_rows else init
            trades = c.execute("SELECT COUNT(*) n FROM trades").fetchone()["n"]
            sells = c.execute(
                """SELECT COUNT(*) n FROM trades t1 WHERE side='SELL' AND EXISTS(
                   SELECT 1 FROM trades t2 WHERE t2.symbol=t1.symbol
                   AND t2.side='BUY' AND t2.ts < t1.ts)""").fetchone()["n"]
        out[strat] = {"equity": round(eq, 2),
                      "total_return": f"{eq / init - 1:+.2%}",
                      "trades": trades}
    return {"strategies": out, "interval_sec": INTERVAL}


@app.get("/v1/positions/{strategy}", dependencies=[Depends(require_token)])
def positions(strategy: str):
    if strategy not in STRATEGIES:
        raise HTTPException(404, f"unknown strategy: {strategy}")
    lt.DB_PATH = BASE / "data" / f"svc_{strategy}.db"
    lt.init_db()
    with lt.db() as c:
        return {"positions": [dict(r) for r in
                              c.execute("SELECT * FROM positions").fetchall()]}


@app.get("/v1/trades/{strategy}", dependencies=[Depends(require_token)])
def trades(strategy: str, limit: int = 50):
    if strategy not in STRATEGIES:
        raise HTTPException(404, f"unknown strategy: {strategy}")
    lt.DB_PATH = BASE / "data" / f"svc_{strategy}.db"
    lt.init_db()
    with lt.db() as c:
        rows = c.execute("SELECT * FROM trades ORDER BY ts DESC LIMIT ?",
                         (min(limit, 200),)).fetchall()
    return {"trades": [dict(r) for r in rows]}


@app.get("/v1/equity/{strategy}", dependencies=[Depends(require_token)])
def equity(strategy: str, limit: int = 500):
    """净值序列（面板折线用）。"""
    if strategy not in STRATEGIES:
        raise HTTPException(404, f"unknown strategy: {strategy}")
    lt.DB_PATH = BASE / "data" / f"svc_{strategy}.db"
    lt.init_db()
    with lt.db() as c:
        rows = c.execute(
            "SELECT ts, total FROM equity ORDER BY ts DESC LIMIT ?",
            (min(limit, 2000),)).fetchall()
    return {"equity": [[r["ts"] * 1000, r["total"]] for r in reversed(rows)]}


@app.get("/v1/market", dependencies=[Depends(require_token)])
def market():
    """标的池行情快照（引擎每轮刷新: 最新价/日涨跌/近90收盘序列）。"""
    if not MARKET_CACHE:
        raise HTTPException(503, "engine warming up, try again in ~1 min")
    return {"symbols": MARKET_CACHE, "ts": time.time()}


@app.get("/v1/heat", dependencies=[Depends(require_token)])
def heat_snapshot():
    """最近一轮资讯热度与各策略信号（面板'为什么在等'可视化）。"""
    if not HEAT_CACHE:
        raise HTTPException(503, "engine warming up, try again in ~1 min")
    return dict(HEAT_CACHE)


# ---------------------------------------------------------------- api (回测)
def _bt_table():
    with sdb() as c:
        c.execute("CREATE TABLE IF NOT EXISTS backtest_runs("
                  "id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, data TEXT NOT NULL)")


@app.post("/v1/backtest", dependencies=[Depends(require_admin)])
def run_backtest():
    """立即跑全策略历史回测（日线）并落库。空闲时段由 cron 调用。"""
    _bt_table()
    data = backtest_all()
    with sdb() as c:
        c.execute("INSERT INTO backtest_runs(ts,data) VALUES(?,?)",
                  (time.time(), json.dumps(data, ensure_ascii=False)))
    return {"ok": True, "strategies": list(data.keys()),
            "ts": time.time()}


@app.get("/v1/backtest", dependencies=[Depends(require_token)])
def last_backtest():
    """最近一次回测结果（面板历史验证区）。"""
    _bt_table()
    with sdb() as c:
        row = c.execute("SELECT ts, data FROM backtest_runs ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        raise HTTPException(404, "no backtest yet — POST /v1/backtest or wait for cron")
    return {"ts": row["ts"], "result": json.loads(row["data"])}


@app.get("/panel")
def panel():
    """长桥风格交易面板（自包含单文件）。"""
    from fastapi.responses import FileResponse
    p = BASE / "panel.html"
    if not p.exists():
        raise HTTPException(404, "panel.html not found")
    return FileResponse(p, media_type="text/html")


@app.get("/", include_in_schema=False)
def root():
    """根路径直接进面板（pingap /lytrade 前缀部署形态）。"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/lytrade/panel")
