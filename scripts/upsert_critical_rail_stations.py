#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sqlite3


STATIONS = [
    # Свободный (Амурская обл.): в исходном CSV ошибочно были координаты одноимённого пункта (~86°E).
    {
        "name": "Свободный",
        "esr_code": "953627",
        "latitude": 51.3807,
        "longitude": 128.1285,
        "settlement_name": "Свободный",
        "region": "Амурская область",
    },
    # Свободный (Амурская обл.): узел для ТР №4 — Михайло-Чесноковская (Книга 2)
    # Координаты ставим ориентировочно по Свободному (для поиска/гео), тариф считается по ЕСР.
    {
        "name": "Михайло-Чесноковская",
        "esr_code": "953701",
        "latitude": 51.3807,
        "longitude": 128.1285,
        "settlement_name": "Свободный",
        "region": "Амурская область",
    },
    # Артём (Приморский край): узел — Артем-Приморский I (Книга 2)
    {
        "name": "Артем-Приморский I",
        "esr_code": "982403",
        "latitude": 43.3595,
        "longitude": 132.1858,
        "settlement_name": "Артем",
        "region": "Приморский край",
    },
    # Purpe: ESR 798700, coords from Alta/Tutu
    {
        "name": "Пурпе",
        "esr_code": "798700",
        "latitude": 64.486969,
        "longitude": 76.680895,
        "settlement_name": "Пурпе",
        "region": "ЯНАО",
    },
    # Nizhny Bestyakh: ESR 913403, coords from Wikidata/Wikimapia
    {
        "name": "Нижний Бестях",
        "esr_code": "913403",
        "latitude": 61.8675,
        "longitude": 129.9564,
        "settlement_name": "Нижний Бестях",
        "region": "Республика Саха (Якутия)",
    },
    # Novoaleksandrovka (Sakhalin): ESR 991207
    {
        "name": "Новоалександровка",
        "esr_code": "991207",
        "latitude": 47.0533655,
        "longitude": 142.7259046,
        "settlement_name": "Новоалександровка",
        "region": "Сахалинская область",
    },
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()
        for st in STATIONS:
            esr = st["esr_code"]
            cur.execute("SELECT id FROM rail_stations WHERE esr_code = ? LIMIT 1", (esr,))
            row = cur.fetchone()
            if row:
                rail_id = int(row[0])
                cur.execute(
                    """
                    UPDATE rail_stations
                    SET name=?, latitude=?, longitude=?, settlement_name=?, region=?, is_active=1
                    WHERE id=?
                    """.strip(),
                    (
                        st["name"],
                        float(st["latitude"]),
                        float(st["longitude"]),
                        st["settlement_name"],
                        st["region"],
                        rail_id,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO rail_stations (name, esr_code, latitude, longitude, settlement_name, region, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    """.strip(),
                    (
                        st["name"],
                        esr,
                        float(st["latitude"]),
                        float(st["longitude"]),
                        st["settlement_name"],
                        st["region"],
                    ),
                )
        con.commit()
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

