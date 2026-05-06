#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sqlite3


def _q(con: sqlite3.Connection, sql: str, params: tuple = ()) -> list[tuple]:
    cur = con.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def main() -> int:
    ap = argparse.ArgumentParser(description="Диагностика цены ДТ-Л на ст. Зелецино")
    ap.add_argument("--db", required=True, help="Path to sqlite db, e.g. /opt/fuel_bot/data/fuel_bot.db")
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    try:
        con.row_factory = sqlite3.Row  # type: ignore[attr-defined]

        print("== Basis candidates (name like %Зелецино%) ==")
        rows = _q(
            con,
            "SELECT id, name, city, transport_type, rail_esr FROM basis WHERE lower(name) LIKE lower(?) ORDER BY id",
            ("%Зелецино%",),
        )
        for r in rows[:50]:
            print(dict(r))
        if not rows:
            print("NO basis rows found")

        print("\n== Product candidates (name like %ДТ%Л%) ==")
        prows = _q(
            con,
            "SELECT id, name, is_active FROM products WHERE lower(name) LIKE lower(?) ORDER BY id",
            ("%ДТ%Л%",),
        )
        for r in prows[:50]:
            print(dict(r))
        if not prows:
            print("NO products rows found")

        print("\n== ProductBasisPrice rows for Zelecino + DT-L ==")
        pbp_rows = _q(
            con,
            """
            SELECT pbp.id, pbp.instrument_code, pbp.current_price, pbp.last_updated, pbp.is_active,
                   p.name as product_name, b.name as basis_name
            FROM product_basis_prices pbp
            JOIN products p ON p.id = pbp.product_id
            JOIN basis b ON b.id = pbp.basis_id
            WHERE lower(b.name) LIKE lower(?)
              AND lower(p.name) LIKE lower(?)
            ORDER BY pbp.is_active DESC, pbp.last_updated DESC, pbp.id DESC
            """.strip(),
            ("%Зелецино%", "%ДТ%Л%"),
        )
        for r in pbp_rows[:100]:
            print(dict(r))
        if not pbp_rows:
            print("NO product_basis_prices rows found for these filters")
            return 0

        codes = []
        for r in pbp_rows:
            c = (r["instrument_code"] or "").strip()
            if c and c not in codes:
                codes.append(c)

        print("\n== Last spimex_prices by instrument_code (top) ==")
        for code in codes[:20]:
            srows = _q(
                con,
                """
                SELECT exchange_product_id, date(date) as d, price, volume, best_bid, best_ask, price_market, price_avg, price_min, price_max, contracts
                FROM spimex_prices
                WHERE exchange_product_id = ?
                ORDER BY date DESC
                LIMIT ?
                """.strip(),
                (code, int(args.limit)),
            )
            print(f"\n-- {code} --")
            if not srows:
                print("  NO spimex_prices history")
                continue
            for sr in srows:
                print(
                    f"  {sr['d']}: price={sr['price']} market={sr['price_market']} avg={sr['price_avg']} "
                    f"min={sr['price_min']} max={sr['price_max']} bid={sr['best_bid']} ask={sr['best_ask']} "
                    f"vol={sr['volume']} contracts={sr['contracts']}"
                )

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

