"""lytrade — 模拟交易策略引擎（paper trading）

行情: longbridge CLI (OAuth token 自动刷新, 只读命令 quote/kline)
策略: 双均线交叉 (MA fast/slow) + lysource 资讯热度过滤 (事件驱动 e^(-λN))
撮合: SQLite 虚拟账户, 市价成交, 佣金+滑点建模
绩效: 总收益 / 年化 / 夏普 / 最大回撤 / 胜率

用法:
  python lytrade.py backtest              # 历史日线回测（MA 策略）
  python lytrade.py live                  # 循环模式: 每 5 分钟拉 quote 跑信号
  python lytrade.py status                # 查看虚拟账户与持仓
  python lytrade.py reset                 # 清空虚拟账户重新开始

⚠️ 仅供策略学习与验证, 不构成投资建议; 引擎绝不调用真实下单接口。
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx
import yaml

BASE = Path(__file__).resolve().parent
CFG = yaml.safe_load((BASE / "config.yaml").read_text(encoding="utf-8"))
DB_PATH = BASE / "data" / "paper.db"
LYSOURCE_TOKEN = os.environ.get("LYSOURCE_TOKEN", "")

ACC, STRAT = CFG["account"], CFG["strategy"]


# ---------------------------------------------------------------- storage
def db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS account(
                id INTEGER PRIMARY KEY CHECK(id=1), cash REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS positions(
                symbol TEXT PRIMARY KEY, name TEXT, qty REAL NOT NULL,
                avg_cost REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS trades(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
                symbol TEXT NOT NULL, side TEXT NOT NULL, qty REAL NOT NULL,
                price REAL NOT NULL, fee REAL NOT NULL, note TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS equity(
                ts REAL PRIMARY KEY, cash REAL, market_value REAL, total REAL);
            """
        )


def reset_account() -> None:
    init_db()
    with db() as c:
        c.executescript(
            "DELETE FROM account; DELETE FROM positions;"
            "DELETE FROM trades; DELETE FROM equity;"
        )
        c.execute("INSERT INTO account(id,cash) VALUES(1,?)", (ACC["initial_cash"],))
    print(f"[lytrade] 虚拟账户已重置, 初始资金 {ACC['initial_cash']:,.0f}")


def get_cash(c) -> float:
    row = c.execute("SELECT cash FROM account WHERE id=1").fetchone()
    if row is None:
        c.execute("INSERT INTO account(id,cash) VALUES(1,?)", (ACC["initial_cash"],))
        return ACC["initial_cash"]
    return row["cash"]


# ---------------------------------------------------------------- market data
def cli_json(args: list[str]) -> dict | list | None:
    """调用 longbridge CLI 并解析 JSON 输出（只读命令）。"""
    cmd = ["longbridge", *args, "--format", "json"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                           encoding="utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        print(f"[warn] CLI 超时: {' '.join(args)}")
        return None
    out = (r.stdout or "").strip()
    if not out or r.returncode != 0:
        print(f"[warn] CLI 失败: {' '.join(args)} :: {(r.stderr or out)[:120]}")
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        print(f"[warn] JSON 解析失败: {out[:120]}")
        return None


def _parse_time(v) -> float:
    """兼容 unix 秒与 ISO8601（CLI 输出 '2026-09-21T16:00:00Z'）。"""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    try:
        return float(s)
    except ValueError:
        pass
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return 0.0


def fetch_kline(symbol: str, count: int, period: str = "day") -> list[dict]:
    """历史 K 线（旧→新）。period: 1m 5m 15m 30m 1h day …（策略参数均按根数，切周期零改动）。
    --session all 含盘前/盘后 K 线——收盘时段美股盘前波动也能驱动日内信号。"""
    data = cli_json(["kline", symbol, "--period", period, "--count", str(count),
                     "--session", "all"])
    if not data:
        return []
    rows = data if isinstance(data, list) else data.get("candles") or data.get("items") or []
    out = []
    for r in rows:
        try:
            out.append({
                "ts": _parse_time(r.get("time") or r.get("timestamp") or r.get("ts")),
                "close": float(r.get("close") or r.get("c")),
                "open": float(r.get("open") or r.get("o") or 0),
                "high": float(r.get("high") or r.get("h") or 0),
                "low": float(r.get("low") or r.get("l") or 0),
            })
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["ts"])
    return out


def fetch_quote(symbol: str) -> dict | None:
    data = cli_json(["quote", symbol])
    if not data:
        return None
    d = data if isinstance(data, dict) else (data[0] if data else None)
    if not d:
        return None
    try:
        return {"price": float(d.get("last_done") or d.get("price") or d.get("last")),
                "ts": float(d.get("timestamp") or d.get("time") or time.time())}
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- news signal
def event_heat(keywords, now: float | None = None) -> float:
    """lysource 资讯热度: 命中条目的 score 求和封顶 1.0（lysource 已含时间衰减）。
    keywords 可为单个关键词或多关键词列表（如 ["腾讯控股", "00700.HK"]），取最大值。
    查询异常时记中性 0.5（高于默认阈值=放行），避免资讯源故障导致永久禁开仓。"""
    if not LYSOURCE_TOKEN:
        return 0.0
    if isinstance(keywords, str):
        keywords = [keywords]
    base = CFG["lysource"]["base_url"]
    best = 0.0
    for kw in keywords:
        try:
            r = httpx.get(f"{base}/v1/resources", params={"q": kw, "limit": 10},
                          headers={"X-API-Token": LYSOURCE_TOKEN}, timeout=10)
            r.raise_for_status()
            items = r.json().get("items", [])
            best = max(best, min(1.0, sum(i.get("score", 0) for i in items)))
        except Exception as exc:
            print(f"[warn] lysource 不可用({exc.__class__.__name__}), 关键词 {kw} 热度记中性 0.5")
            best = max(best, 0.5)
    return best


# ---------------------------------------------------------------- indicators
def _wilder(values: list[float], period: int) -> list[float]:
    """Wilder 平滑（RMA）。"""
    out, prev = [], 0.0
    for i, v in enumerate(values):
        prev = v if i == 0 else (prev * (period - 1) + v) / period
        out.append(prev)
    return out


def atr(highs: list[float], lows: list[float], closes: list[float],
        period: int = 10) -> list[float]:
    trs = []
    for i in range(len(closes)):
        pc = closes[i - 1] if i else closes[i]
        trs.append(max(highs[i] - lows[i], abs(highs[i] - pc), abs(lows[i] - pc)))
    return _wilder(trs, period)


def adx(highs: list[float], lows: list[float], closes: list[float],
        period: int = 14) -> float:
    """ADX 趋势强度（Wilder）。返回最新值。"""
    n = len(closes)
    if n < period * 2 + 1:
        return 0.0
    plus_dm, minus_dm, trs = [0.0], [0.0], []
    for i in range(1, n):
        up, dn = highs[i] - highs[i - 1], lows[i - 1] - lows[i]
        plus_dm.append(up if up > dn and up > 0 else 0.0)
        minus_dm.append(dn if dn > up and dn > 0 else 0.0)
        pc = closes[i - 1]
        trs.append(max(highs[i] - lows[i], abs(highs[i] - pc), abs(lows[i] - pc)))
    tr_s = _wilder(trs[1:], period)
    pdi = [p / t * 100 if t else 0 for p, t in zip(_wilder(plus_dm[1:], period), tr_s)]
    mdi = [m / t * 100 if t else 0 for m, t in zip(_wilder(minus_dm[1:], period), tr_s)]
    dx = [abs(p - m) / (p + m) * 100 if (p + m) else 0 for p, m in zip(pdi, mdi)]
    return _wilder(dx, period)[-1]


def supertrend_dir(highs: list[float], lows: list[float], closes: list[float],
                   period: int = 10, multiplier: float = 3.0) -> list[int]:
    """Supertrend 方向序列（1=up 多头, -1=down 空头）。标准 ATR 通道算法。"""
    a = atr(highs, lows, closes, period)
    n = len(closes)
    direction, upper, lower = [1] * n, [0.0] * n, [0.0] * n
    for i in range(n):
        mid = (highs[i] + lows[i]) / 2
        bub, blb = mid + multiplier * a[i], mid - multiplier * a[i]
        fu = min(bub, upper[i - 1]) if i and closes[i - 1] <= upper[i - 1] else bub
        fl = max(blb, lower[i - 1]) if i and closes[i - 1] >= lower[i - 1] else blb
        upper[i], lower[i] = fu, fl
        if i:
            direction[i] = (1 if closes[i] > upper[i - 1]
                            else -1 if closes[i] < lower[i - 1] else direction[i - 1])
    return direction


def supertrend_signal(highs: list[float], lows: list[float],
                      closes: list[float]) -> str:
    """Supertrend 翻多 BUY / 翻空 SELL；ADX<20 震荡市不追（抄 freqtrade 社区思路）。"""
    if len(closes) < STRAT.get("adx_filter_warmup", 30):
        return "HOLD"
    d = supertrend_dir(highs, lows, closes,
                       STRAT.get("st_period", 10), STRAT.get("st_multiplier", 3.0))
    if len(d) < 2:
        return "HOLD"
    if d[-1] == 1 and d[-2] == -1:
        return "BUY" if adx(highs, lows, closes) >= STRAT.get("adx_min", 20) else "HOLD"
    if d[-1] == -1 and d[-2] == 1:
        return "SELL"
    return "HOLD"


# ---------------------------------------------------------------- strategy
def bb_signal(closes: list[float]) -> str:
    """布林带均值回归（高频）: 触下轨 BUY(超卖回归) / 触上轨 SELL(超买回落或持仓止盈)。"""
    period = STRAT.get("bb_period", 20)
    mult = STRAT.get("bb_multiplier", 2.0)
    if len(closes) < period:
        return "HOLD"
    win = closes[-period:]
    mid = statistics.mean(win)
    std = statistics.pstdev(win)
    if std < 1e-9:
        return "HOLD"
    px = closes[-1]
    if px <= mid - mult * std:
        return "BUY"
    if px >= mid + mult * std:
        return "SELL"
    return "HOLD"


def ma_signal(closes: list[float]) -> str:
    """双均线交叉: 金叉 BUY / 死叉 SELL / 其他 HOLD。"""
    f, s = STRAT["fast"], STRAT["slow"]
    if len(closes) < s + 1:
        return "HOLD"
    ma_f = [statistics.mean(closes[i - f + 1:i + 1]) for i in range(s - 1, len(closes))]
    ma_s = [statistics.mean(closes[i - s + 1:i + 1]) for i in range(s - 1, len(closes))]
    if ma_f[-1] > ma_s[-1] and ma_f[-2] <= ma_s[-2]:
        return "BUY"      # 金叉
    if ma_f[-1] < ma_s[-1] and ma_f[-2] >= ma_s[-2]:
        return "SELL"     # 死叉
    return "HOLD"


# ---------------------------------------------------------------- paper broker
def execute(c, symbol: str, name: str, side: str, price: float, ts: float,
            note: str = "") -> None:
    """模拟撮合: 市价单 + 佣金 + 滑点, 单标的仓位上限风控。支持多空双向:
    positions.qty 负数=空头（cash 不动, 市值 qty*price 为负 → 净值自动反映浮盈亏）。"""
    cash = get_cash(c)
    fee_rate, slip = ACC["fee_rate"], ACC["slippage"]
    pos = c.execute("SELECT * FROM positions WHERE symbol=?", (symbol,)).fetchone()
    qty0 = pos["qty"] if pos else 0.0
    cap = ACC["initial_cash"] * ACC["max_position_pct"]

    if side == "BUY":
        px = round(price * (1 + slip), 4)
        if qty0 < 0:                              # 空仓回补（平空）
            qty = min(-qty0, math.floor(cap / px * 100) / 100)
            fee = round(qty * px * fee_rate, 4)
            new_qty = round(qty0 + qty, 6)
            if new_qty >= 0.01:
                c.execute("UPDATE positions SET qty=? WHERE symbol=?", (new_qty, symbol))
            else:
                c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            c.execute("UPDATE account SET cash=? WHERE id=1", (cash - qty * px - fee,))
        else:                                     # 开多/加多
            budget = min(cash * 0.95, cap)
            qty = math.floor(budget / px * 100) / 100  # 按 0.01 股粒度简化
            if qty < 0.01:
                return
            fee = round(qty * px * fee_rate, 4)
            if qty * px + fee > cash:
                return
            new_qty = qty0 + qty
            new_cost = ((pos["avg_cost"] * qty0) if pos else 0) + qty * px
            c.execute("INSERT INTO positions(symbol,name,qty,avg_cost) VALUES(?,?,?,?)"
                      " ON CONFLICT(symbol) DO UPDATE SET qty=?, avg_cost=?, name=?",
                      (symbol, name, new_qty, new_cost / new_qty, new_qty, new_cost / new_qty, name))
            c.execute("UPDATE account SET cash=? WHERE id=1", (cash - qty * px - fee,))
    elif side == "SELL":
        px = round(price * (1 - slip), 4)
        if qty0 > 0:                              # 平多
            qty = qty0
            fee = round(qty * px * fee_rate, 4)
            c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            c.execute("UPDATE account SET cash=? WHERE id=1", (cash + qty * px - fee,))
        else:                                     # 开空/加空
            budget = min(cash * 0.95, cap)
            qty = math.floor(budget / px * 100) / 100
            if qty < 0.01:
                return
            fee = round(qty * px * fee_rate, 4)
            new_qty = round(qty0 - qty, 6)
            if pos:
                c.execute("UPDATE positions SET qty=?, name=? WHERE symbol=?",
                          (new_qty, name, symbol))
            else:
                c.execute("INSERT INTO positions(symbol,name,qty,avg_cost) VALUES(?,?,?,?)",
                          (symbol, name, new_qty, px))
    else:
        return
    c.execute("INSERT INTO trades(ts,symbol,side,qty,price,fee,note) VALUES(?,?,?,?,?,?,?)",
              (ts, symbol, side, qty, px, fee, note))
    print(f"[trade] {side} {symbol} {qty}@{px} fee={fee} ({note})")


def mark_equity(c, prices: dict[str, float]) -> float:
    ts = time.time()
    cash = get_cash(c)
    mv = sum(r["qty"] * prices.get(r["symbol"], r["avg_cost"])
             for r in c.execute("SELECT * FROM positions").fetchall())
    total = cash + mv
    c.execute("INSERT OR REPLACE INTO equity(ts,cash,market_value,total) VALUES(?,?,?,?)",
              (ts, cash, mv, total))
    return total


# ---------------------------------------------------------------- performance
def performance(c) -> dict:
    eq = [r["total"] for r in c.execute("SELECT total FROM equity ORDER BY ts").fetchall()]
    if len(eq) < 2:
        return {"error": "equity 样本不足, 先跑 backtest/live"}
    init = ACC["initial_cash"]
    total_ret = eq[-1] / init - 1
    rets = [(eq[i] / eq[i - 1]) - 1 for i in range(1, len(eq)) if eq[i - 1] > 0]
    sharpe = (statistics.mean(rets) / statistics.stdev(rets) * math.sqrt(252)
              if len(rets) > 2 and statistics.stdev(rets) > 0 else 0.0)
    peak, mdd = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = max(mdd, (peak - v) / peak)
    trades = c.execute("SELECT side, COUNT(*) n FROM trades GROUP BY side").fetchall()
    sells = c.execute(
        """SELECT t.symbol, t.price px_in, t2.price px_out FROM trades t
           JOIN trades t2 ON t2.symbol=t.symbol AND t2.side='SELL'
           WHERE t.side='BUY'""").fetchall()
    wins = sum(1 for r in sells if r["px_out"] > r["px_in"])
    return {
        "total_return": f"{total_ret:+.2%}", "sharpe": round(sharpe, 2),
        "max_drawdown": f"{mdd:.2%}", "cash": round(get_cash(c), 2),
        "positions": dict((r["symbol"], r["qty"])
                          for r in c.execute("SELECT symbol,qty FROM positions").fetchall()),
        "round_trips": len(sells), "win_rate": f"{wins / len(sells):.0%}" if sells else "n/a",
    }


# ---------------------------------------------------------------- modes
def mode_backtest(strategy: str = "ma") -> None:
    init_db()
    sig_fn = {"ma": lambda k: ma_signal([x["close"] for x in k]),
              "supertrend": lambda k: supertrend_signal(
                  [x["high"] for x in k], [x["low"] for x in k],
                  [x["close"] for x in k])}[strategy]
    print(f"== 回测模式: strategy={strategy} ==")
    for s in CFG["symbols"]:
        sym, name = s["symbol"], s["name"]
        kl = fetch_kline(sym, STRAT["kline_count"])
        if len(kl) < STRAT["slow"] + 2:
            print(f"[skip] {sym} 日线不足({len(kl)})")
            continue
        with db() as c:
            warmup = {"ma": STRAT["slow"], "supertrend": 30}[strategy]
            for i in range(warmup, len(kl)):
                sig = sig_fn(kl[: i + 1])
                if sig in ("BUY", "SELL"):
                    execute(c, sym, name, sig, kl[i]["close"], kl[i]["ts"],
                            note=f"backtest:{strategy}")
            mark_equity(c, {sym: kl[-1]["close"]})
    with db() as c:
        print(json.dumps(performance(c), ensure_ascii=False, indent=2))


def mode_live(interval: int = 300) -> None:
    init_db()
    print(f"== 模拟盘模式: 每 {interval}s 轮询, Ctrl+C 退出 ==")
    while True:
        now = time.time()
        with db() as c:
            for s in CFG["symbols"]:
                sym, name = s["symbol"], s["name"]
                q = fetch_quote(sym)
                if not q or not q.get("price"):
                    continue
                kl = fetch_kline(sym, STRAT["kline_count"])
                closes = [k["close"] for k in kl] + [q["price"]]
                sig = ma_signal(closes)
                heat = event_heat([name, sym])
                pos = c.execute("SELECT qty FROM positions WHERE symbol=?", (sym,)).fetchone()
                if sig == "BUY" and heat >= STRAT["event_heat_min"] and not pos:
                    execute(c, sym, name, "BUY", q["price"], now,
                            note=f"金叉+热度{heat:.2f}")
                elif sig == "SELL" and pos:
                    execute(c, sym, name, "SELL", q["price"], now, note="死叉")
            prices = {s["symbol"]: (fetch_quote(s["symbol"]) or {}).get("price", 0)
                      for s in CFG["symbols"]}
            total = mark_equity(c, prices)
        print(f"[{time.strftime('%H:%M:%S')}] 净值 {total:,.2f}")
        time.sleep(interval)


def mode_status() -> None:
    init_db()
    with db() as c:
        print(json.dumps(performance(c), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "status"
    if mode == "backtest":
        mode_backtest(sys.argv[2] if len(sys.argv) > 2 else "ma")
    else:
        {"live": mode_live, "status": mode_status,
         "reset": reset_account}[mode]()
