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
INTERVAL = int(os.environ.get("LYTRADE_INTERVAL", "300"))
STRATEGIES = ["supertrend", "ma", "pairs"]
PAIRS = [("00700.HK", "09988.HK")]          # 同行业高相关对（v1 固定）

lt.DB_PATH = DB_PATH
LTOKEN = os.environ.get("LYSOURCE_TOKEN", "")


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


# ---------------------------------------------------------------- engine
def run_strategy(strat: str, klines: dict, quotes: dict, heat: dict) -> dict:
    """单策略一轮: 独立 DB 账户 → 信号 → 撮合 → 净值。"""
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
              "ma": lambda h, l, cl: lt.ma_signal(cl)}[strat]
        for s in CFG["symbols"]:
            kl = klines[s["symbol"]]
            if len(kl) > lt.STRAT["slow"] + 2:
                sigs[s["symbol"]] = fn([k["high"] for k in kl],
                                       [k["low"] for k in kl],
                                       [k["close"] for k in kl])
    with lt.db() as c:
        for sym, sig in sigs.items():
            if sig in ("BUY", "SELL") and quotes.get(sym):
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
    for s in CFG["symbols"]:
        sym = s["symbol"]
        kl = lt.fetch_kline(sym, lt.STRAT["kline_count"])
        q = lt.fetch_quote(sym)
        if q and q.get("price"):
            kl.append({"ts": q["ts"], "close": q["price"], "open": 0, "high": 0, "low": 0})
            quotes[sym] = q["price"]
        klines[sym] = kl
    heat = {s["name"]: lt.event_heat([s["name"], s["symbol"]]) for s in CFG["symbols"]}
    out = {strat: run_strategy(strat, klines, quotes, heat) for strat in STRATEGIES}
    out["_heat"] = {k: round(v, 3) for k, v in heat.items()}  # 观测: 每轮日志可见热度
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
