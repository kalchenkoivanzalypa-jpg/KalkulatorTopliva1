from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import func, select

from db.database import SpimexPrice


@dataclass
class SeriesPoint:
    dt: datetime
    price: float
    volume_tons: Optional[float] = None
    contracts: Optional[int] = None
    best_ask: Optional[float] = None
    best_bid: Optional[float] = None


@dataclass
class Metrics30d:
    n: int
    curr: float
    prev: Optional[float]

    min30: float
    max30: float
    avg30: float
    std30: float

    range_pos30: Optional[float]
    cheapness: Optional[float]

    trend5: Optional[float]
    trend10: Optional[float]
    trend30: Optional[float]

    deviation30: float
    z30: Optional[float]
    volatility30: float

    avg_volume30: Optional[float]
    active_days: int
    active_ratio: float
    avg_contracts30: Optional[float]
    liquidity_score: float

    spread_abs: Optional[float]
    spread_pct: Optional[float]
    market_to_bid: Optional[float]
    ask_to_market: Optional[float]
    spread_score: Optional[float]

    entry_score: float


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def score_entry(m: Metrics30d) -> float:
    """
    MVP скоринг «сигнала входа» (0..100+).

    Идея:
    - дешевле внутри 30д диапазона → лучше
    - сильные аномалии (|Z|) → хуже (риск)
    - высокая ликвидность → лучше
    - большой спред → хуже
    """
    cheap = float(m.cheapness) if m.cheapness is not None else 0.0
    z_pen = 0.0 if m.z30 is None else min(3.0, abs(float(m.z30))) / 3.0  # 0..1
    spread_pen = 0.0
    if m.spread_pct is not None:
        spread_pen = max(0.0, min(1.0, float(m.spread_pct) / 0.02))  # 2% spread -> full penalty

    raw = (
        65.0 * cheap
        + 0.35 * float(m.liquidity_score)
        - 20.0 * z_pen
        - 15.0 * spread_pen
    )
    return float(raw)


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: list[float]) -> float:
    # population std
    if not xs:
        return 0.0
    mu = _mean(xs)
    v = sum((x - mu) ** 2 for x in xs) / len(xs)
    return math.sqrt(v)


def _norm_0_1(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def _safe_float(x: object, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except Exception:
        return float(default)


def _risk_penalty_0_1(m: Metrics30d, *, z_cap: float = 3.0) -> float:
    if m.z30 is None:
        return 0.0
    return clamp(abs(float(m.z30)) / float(z_cap), 0.0, 1.0)


def compute_final_score_and_signal(
    *,
    total_cost_per_ton: float,
    metrics: Optional[Metrics30d],
    peer_total_min: float,
    peer_total_max: float,
    peer_entry_min: float,
    peer_entry_max: float,
    peer_vol_min: float,
    peer_vol_max: float,
) -> tuple[float, str]:
    """
    FinalScore: 0..100, чем больше — тем лучше.

    Смешиваем:
    - цена+доставка (нормализуем относительно кандидатов)
    - ликвидность (0..100)
    - entry_score (нормализуем относительно кандидатов)
    - штрафы за риск (|Z|), волатильность и спред
    """
    # cost: cheaper -> higher score
    cost_norm = _norm_0_1(float(total_cost_per_ton), float(peer_total_min), float(peer_total_max))
    cost_score = 1.0 - cost_norm

    if metrics is None:
        final = 100.0 * (0.7 * cost_score)
        sig = "WATCH" if final >= 50.0 else "AVOID"
        return clamp(final, 0.0, 100.0), sig

    liq_norm = clamp(float(metrics.liquidity_score) / 100.0, 0.0, 1.0)
    entry_norm = _norm_0_1(float(metrics.entry_score), float(peer_entry_min), float(peer_entry_max))
    risk_pen = _risk_penalty_0_1(metrics)

    vol_norm = _norm_0_1(float(metrics.volatility30), float(peer_vol_min), float(peer_vol_max))
    spread_pen = 0.0
    if metrics.spread_pct is not None:
        spread_pen = clamp(float(metrics.spread_pct) / 0.02, 0.0, 1.0)  # 2% spread is "bad"

    final = 100.0 * (
        0.50 * cost_score
        + 0.20 * liq_norm
        + 0.20 * entry_norm
        - 0.05 * risk_pen
        - 0.03 * vol_norm
        - 0.02 * spread_pen
    )
    final = clamp(final, 0.0, 100.0)

    # signals
    z_ok = (metrics.z30 is None) or (abs(float(metrics.z30)) <= 2.0)
    liq_ok = float(metrics.liquidity_score) >= 40.0
    if final >= 70.0 and z_ok and liq_ok:
        sig = "BUY"
    elif final >= 50.0:
        sig = "WATCH"
    else:
        sig = "AVOID"

    return final, sig


@dataclass
class FinalScoreBreakdown:
    final: float
    signal: str
    # normalized components (0..1) where applicable
    cost_score: float
    liq_norm: float
    entry_norm: float
    risk_pen: float
    vol_norm: float
    spread_pen: float


def compute_final_score_breakdown(
    *,
    total_cost_per_ton: float,
    metrics: Optional[Metrics30d],
    peer_total_min: float,
    peer_total_max: float,
    peer_entry_min: float,
    peer_entry_max: float,
    peer_vol_min: float,
    peer_vol_max: float,
) -> FinalScoreBreakdown:
    cost_norm = _norm_0_1(float(total_cost_per_ton), float(peer_total_min), float(peer_total_max))
    cost_score = 1.0 - cost_norm

    if metrics is None:
        final = clamp(100.0 * (0.7 * cost_score), 0.0, 100.0)
        sig = "WATCH" if final >= 50.0 else "AVOID"
        return FinalScoreBreakdown(
            final=final,
            signal=sig,
            cost_score=cost_score,
            liq_norm=0.0,
            entry_norm=0.0,
            risk_pen=0.0,
            vol_norm=0.0,
            spread_pen=0.0,
        )

    liq_norm = clamp(float(metrics.liquidity_score) / 100.0, 0.0, 1.0)
    entry_norm = _norm_0_1(float(metrics.entry_score), float(peer_entry_min), float(peer_entry_max))
    risk_pen = _risk_penalty_0_1(metrics)
    vol_norm = _norm_0_1(float(metrics.volatility30), float(peer_vol_min), float(peer_vol_max))
    spread_pen = 0.0
    if metrics.spread_pct is not None:
        spread_pen = clamp(float(metrics.spread_pct) / 0.02, 0.0, 1.0)

    final = 100.0 * (
        0.50 * cost_score
        + 0.20 * liq_norm
        + 0.20 * entry_norm
        - 0.05 * risk_pen
        - 0.03 * vol_norm
        - 0.02 * spread_pen
    )
    final = clamp(final, 0.0, 100.0)

    z_ok = (metrics.z30 is None) or (abs(float(metrics.z30)) <= 2.0)
    liq_ok = float(metrics.liquidity_score) >= 40.0
    if final >= 70.0 and z_ok and liq_ok:
        sig = "BUY"
    elif final >= 50.0:
        sig = "WATCH"
    else:
        sig = "AVOID"

    return FinalScoreBreakdown(
        final=final,
        signal=sig,
        cost_score=cost_score,
        liq_norm=liq_norm,
        entry_norm=entry_norm,
        risk_pen=risk_pen,
        vol_norm=vol_norm,
        spread_pen=spread_pen,
    )


async def load_series_30d(session, instrument_code: str) -> list[SeriesPoint]:
    code_u = str(instrument_code or "").strip().upper()
    if not code_u:
        return []
    qh = await session.execute(
        select(SpimexPrice)
        .where(func.upper(SpimexPrice.exchange_product_id) == code_u)
        .order_by(SpimexPrice.date.desc())
        .limit(30)
    )
    rows = qh.scalars().all()
    out: list[SeriesPoint] = []
    for r in rows:
        if r.price is None:
            continue
        out.append(
            SeriesPoint(
                dt=r.date,
                price=float(r.price),
                volume_tons=float(r.volume) if r.volume is not None else None,
                contracts=int(r.contracts) if getattr(r, "contracts", None) is not None else None,
                best_ask=float(r.best_ask) if getattr(r, "best_ask", None) is not None else None,
                best_bid=float(r.best_bid) if getattr(r, "best_bid", None) is not None else None,
            )
        )
    return out


def compute_metrics_30d(points_desc: list[SeriesPoint]) -> Optional[Metrics30d]:
    if not points_desc:
        return None
    prices = [p.price for p in points_desc]
    curr = prices[0]
    prev = prices[1] if len(prices) > 1 else None

    min30 = min(prices)
    max30 = max(prices)
    avg30 = _mean(prices)
    std30 = _std(prices)

    range_pos30 = None
    cheapness = None
    if max30 > min30:
        range_pos30 = (curr - min30) / (max30 - min30)
        cheapness = 1.0 - range_pos30

    def _trend(k: int) -> Optional[float]:
        if len(prices) <= k:
            return None
        return curr - prices[k]

    trend5 = _trend(5)
    trend10 = _trend(10)
    trend30 = _trend(29)  # t-30 ~ index 29 when 30 points exist

    deviation30 = curr - avg30
    z30 = None
    if std30 > 0:
        z30 = deviation30 / std30

    # liquidity: volume + contracts + active days
    vols = [p.volume_tons for p in points_desc if p.volume_tons is not None]
    avg_volume30 = _mean([v for v in vols if v is not None]) if vols else None
    active_days = sum(1 for p in points_desc if (p.volume_tons or 0) > 0)
    active_ratio = active_days / max(1, len(points_desc))

    contracts = [p.contracts for p in points_desc if p.contracts is not None]
    avg_contracts30 = _mean([float(x) for x in contracts]) if contracts else None

    # Normalize volume/contracts within this series (simple robust approach)
    v_norm = 0.0
    if vols:
        v_lo = min(float(v) for v in vols if v is not None)
        v_hi = max(float(v) for v in vols if v is not None)
        v_norm = _norm_0_1(float(avg_volume30 or 0.0), v_lo, v_hi)

    c_norm = 0.0
    if contracts:
        c_lo = min(float(x) for x in contracts if x is not None)
        c_hi = max(float(x) for x in contracts if x is not None)
        c_norm = _norm_0_1(float(avg_contracts30 or 0.0), c_lo, c_hi)

    liquidity_score = 100.0 * (0.4 * v_norm + 0.3 * c_norm + 0.3 * active_ratio)

    # market structure
    spread_abs = None
    spread_pct = None
    market_to_bid = None
    ask_to_market = None
    spread_score = None
    if points_desc[0].best_ask is not None and points_desc[0].best_bid is not None:
        best_ask = float(points_desc[0].best_ask)
        best_bid = float(points_desc[0].best_bid)
        spread_abs = best_ask - best_bid
        if curr > 0:
            spread_pct = spread_abs / curr
            market_to_bid = (curr - best_bid) / curr
            ask_to_market = (best_ask - curr) / curr
        # simple score: tighter spread -> closer to 100
        if spread_pct is not None:
            spread_score = 100.0 * (1.0 - clamp(float(spread_pct) / 0.02, 0.0, 1.0))

    tmp = Metrics30d(
        n=len(points_desc),
        curr=curr,
        prev=prev,
        min30=min30,
        max30=max30,
        avg30=avg30,
        std30=std30,
        range_pos30=range_pos30,
        cheapness=cheapness,
        trend5=trend5,
        trend10=trend10,
        trend30=trend30,
        deviation30=deviation30,
        z30=z30,
        volatility30=std30,
        avg_volume30=avg_volume30,
        active_days=active_days,
        active_ratio=active_ratio,
        avg_contracts30=avg_contracts30,
        liquidity_score=liquidity_score,
        spread_abs=spread_abs,
        spread_pct=spread_pct,
        market_to_bid=market_to_bid,
        ask_to_market=ask_to_market,
        spread_score=spread_score,
        entry_score=0.0,
    )
    tmp.entry_score = score_entry(tmp)
    return tmp

