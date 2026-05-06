#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import re
import sqlite3


def _norm(s: str) -> str:
    s = (s or "").strip().lower().replace("ё", "е")
    s = s.replace("—", "-").replace("–", "-").replace("−", "-").replace("‑", "-")
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s).strip()
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    for prefix in ("ст. ", "станция ", "жд станция ", "ж/д станция "):
        if s.startswith(prefix):
            s = s[len(prefix) :].strip()
            break
    return s


def _words(s: str) -> list[str]:
    return [w for w in _norm(s).split(" ") if w]


def _is_sakhalin(lat: float, lon: float) -> bool:
    return 45.0 <= float(lat) <= 55.0 and 141.0 <= float(lon) <= 146.0


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # cheap haversine-ish approximation is enough for ranking
    # 1 deg lat ~ 111km; lon scaled by cos(lat)
    import math

    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)
    dy = (lat2 - lat1) * 111.0
    dx = (lon2 - lon1) * 111.0 * math.cos(math.radians((lat1 + lat2) / 2.0))
    return float((dx * dx + dy * dy) ** 0.5)


def _best_station_for_basis(
    *,
    basis_name: str,
    basis_lat: float | None,
    basis_lon: float | None,
    stations: list[tuple],
) -> tuple | None:
    """
    Возвращает лучшую станцию из rail_stations для данного базиса по:
    - совпадению слов (name/settlement_name)
    - близости к координатам базиса (если есть)
    - сахалинскому признаку (если базис на Сахалине)
    """
    b_key = _norm(basis_name or "")
    b_words = set(_words(basis_name or ""))
    if not b_key:
        return None

    is_b_sak = False
    if basis_lat is not None and basis_lon is not None:
        is_b_sak = _is_sakhalin(float(basis_lat), float(basis_lon))

    best: tuple[float, tuple] | None = None
    for st in stations:
        st_id, st_name, esr, st_lat, st_lon, set_name, region = st
        if not esr:
            continue
        st_key = _norm(st_name or "")
        set_key = _norm(set_name or "")
        st_words = set(_words(st_name or "")) | set(_words(set_name or ""))

        score = 0.0
        # exact / prefix / substring
        if st_key and st_key == b_key:
            score = max(score, 500.0)
        if set_key and set_key == b_key:
            score = max(score, 480.0)
        if st_key and (b_key.startswith(st_key) or st_key.startswith(b_key)):
            score = max(score, 320.0)
        if set_key and (b_key.startswith(set_key) or set_key.startswith(b_key)):
            score = max(score, 300.0)
        if st_key and (st_key in b_key or b_key in st_key):
            score = max(score, 220.0)
        if set_key and (set_key in b_key or b_key in set_key):
            score = max(score, 210.0)

        # word overlap
        if b_words and st_words:
            inter = b_words & st_words
            if inter:
                score = max(score, 160.0 + 25.0 * len(inter))

        if score <= 0:
            continue

        # geo tie-breaker / penalty if far
        if basis_lat is not None and basis_lon is not None:
            d = _distance_km(float(basis_lat), float(basis_lon), float(st_lat), float(st_lon))
            # prefer closer
            score -= min(180.0, d / 10.0)
            # if extremely far, likely wrong
            if d > 2000.0:
                score -= 200.0

        # sakhalin preference when basis on sakhalin
        if is_b_sak and _is_sakhalin(float(st_lat), float(st_lon)):
            score += 40.0

        if best is None or score > best[0]:
            best = (score, st)

    return best[1] if best else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="Overwrite rail_esr even if already set")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of updated rows (0=all)")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, name, city, transport_type, rail_esr, rail_station_name, rail_latitude, rail_longitude, latitude, longitude
            FROM basis
            WHERE is_active=1 AND transport_type='rail'
            """.strip()
        )
        basises = cur.fetchall()

        # load stations in memory
        cur.execute(
            "SELECT id, name, esr_code, latitude, longitude, settlement_name, region FROM rail_stations WHERE is_active=1"
        )
        stations = cur.fetchall()

        updated = 0
        for (
            bid,
            bname,
            bcity,
            ttype,
            rail_esr,
            rail_station_name,
            rail_lat,
            rail_lon,
            lat,
            lon,
        ) in basises:
            need = args.force or (not rail_esr) or (rail_lat is None) or (rail_lon is None) or (not rail_station_name)
            if not need:
                continue

            chosen = _best_station_for_basis(
                basis_name=str(bname or ""),
                basis_lat=(float(lat) if lat is not None else None),
                basis_lon=(float(lon) if lon is not None else None),
                stations=stations,
            )
            if not chosen:
                continue
            st_id, st_name, esr, st_lat, st_lon, set_name, region = chosen

            updated += 1
            if not args.dry_run:
                cur.execute(
                    """
                    UPDATE basis
                    SET rail_esr=?, rail_station_name=?, rail_latitude=?, rail_longitude=?
                    WHERE id=?
                    """.strip(),
                    (str(esr), str(st_name), float(st_lat), float(st_lon), int(bid)),
                )
            if args.limit and updated >= int(args.limit):
                break

        if not args.dry_run:
            con.commit()
        print(f"updated={updated} dry_run={bool(args.dry_run)}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

