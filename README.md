# lytrade — 模拟交易策略引擎（paper trading）

> lyco 生态 · longbridge CLI 只读行情 + lysource 资讯信号 + SQLite 虚拟撮合。
> **仅供策略学习与验证，不构成投资建议；引擎绝不调用真实下单接口。**

## 架构（原子化 v1）

```
longbridge CLI (quote/kline --format json, OAuth token 自动刷新, 只读)
        ↓ 行情
策略: MA(fast/slow) 交叉 + lysource 资讯热度过滤
      └─ 事件驱动方法论: 热度 = Σ score(lysource 自带 e^(-h/72) 衰减), 低于阈值只平不开
        ↓ 信号
paper broker: SQLite 虚拟账户 · 市价成交 · 佣金万3 · 滑点0.1% · 单标的≤30%仓位
        ↓
绩效: 总收益 / 夏普 / 最大回撤 / 胜率 / equity curve
```

## 快速开始

```bash
# 1. 一次性授权（浏览器 OAuth，token 自动刷新）
longbridge auth login

# 2. 设置 lysource 信号源 token（可选，未设则热度恒为 0）
set LYSOURCE_TOKEN=xxx

# 3. 跑
python lytrade.py backtest   # 历史日线回测（MA 策略）
python lytrade.py live       # 模拟盘: 每 5 分钟轮询信号
python lytrade.py status     # 查看账户/持仓/绩效
python lytrade.py reset      # 重置虚拟账户
```

标的池、策略参数、费用模型都在 `config.yaml`。

## 已验证（合成数据单测）

- ✅ ma_signal 金叉/死叉/无信号判定（构造序列断言）
- ✅ 撮合现金流守恒：10 万本金一轮往返损耗 77.92（= 佣金 + 双边滑点，量级正确）
- ✅ 绩效统计（收益/夏普/回撤/胜率）

## 后续模块化路线

- 策略插件化（配对交易 Z-score / 波动率目标 / 事件驱动加权）
- lysource 资讯正负面情绪打分接入（现在只用热度）
- equity 曲线 HTML 报表（ECharts）· Telegram 推送
- Windows 计划任务定时回测日报

---

> **免责声明**：以上内容基于公开数据和量化分析，仅供参考，不构成投资建议。市场有风险，投资需谨慎。任何投资决策应结合个人风险承受能力、资金状况和投资目标独立判断，必要时咨询持牌专业机构。过往表现不预示未来收益。
