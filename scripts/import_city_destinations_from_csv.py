#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт справочника населённых пунктов из CSV в таблицу city_destinations.

CSV ожидается в формате как data.csv:
  region, municipality, settlement, type, latitude_dd, longitude_dd, population, ...

Особенности:
  - В нашей схеме CityDestination.name уникален, поэтому имя делаем устойчиво уникальным:
      "<settlement> (<region>)"
    Регион также сохраняем в поле CityDestination.region.
  - Пропускаем строки без координат.
  - Можно ограничить количество импортируемых строк через --limit (для тестов).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.database import CityDestination, get_session, init_db

load_dotenv()

logger = logging.getLogger(__name__)


def _safe_float(x: str) -> float | None:
    try:
        s = (x or "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _compose_unique_name(settlement: str, region: str) -> str:
    settlement = (settlement or "").strip()
    region = (region or "").strip()
    if not settlement:
        return ""
    if region:
        return f"{settlement} ({region})"
    return settlement


async def import_csv(path: Path, *, limit: int | None = None, batch_size: int = 2000) -> None:
    await init_db()
    session = await get_session()
    inserted = 0
    skipped = 0
    seen = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            pending: list[CityDestination] = []
            for row in reader:
                seen += 1
                if limit is not None and seen > limit:
                    break

                settlement = (row.get("settlement") or "").strip()
                region = (row.get("region") or "").strip()
                lat = _safe_float(str(row.get("latitude_dd") or ""))
                lon = _safe_float(str(row.get("longitude_dd") or ""))
                if not settlement or lat is None or lon is None:
                    skipped += 1
                    continue

                name = _compose_unique_name(settlement, region)
                if not name:
                    skipped += 1
                    continue

                pending.append(
                    CityDestination(
                        name=name,
                        region=region or None,
                        latitude=float(lat),
                        longitude=float(lon),
                    )
                )

                if len(pending) >= batch_size:
                    session.add_all(pending)
                    try:
                        await session.commit()
                        inserted += len(pending)
                    except IntegrityError:
                        await session.rollback()
                        # Если попались дубликаты — вставляем по одному, пропуская конфликтные.
                        for obj in pending:
                            session.add(obj)
                            try:
                                await session.commit()
                                inserted += 1
                            except IntegrityError:
                                await session.rollback()
                                skipped += 1
                    pending.clear()

            if pending:
                session.add_all(pending)
                try:
                    await session.commit()
                    inserted += len(pending)
                except IntegrityError:
                    await session.rollback()
                    for obj in pending:
                        session.add(obj)
                        try:
                            await session.commit()
                            inserted += 1
                        except IntegrityError:
                            await session.rollback()
                            skipped += 1

        # Итоговые числа
        q = await session.execute(select(CityDestination.id))
        total = len(q.scalars().all())
        logger.info("Импорт завершён: прочитано=%s, вставлено=%s, пропущено=%s, всего в БД=%s", seen, inserted, skipped, total)
        print(f"✅ import_city_destinations_from_csv: seen={seen} inserted={inserted} skipped={skipped} total={total}")
    finally:
        await session.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Путь к data.csv")
    ap.add_argument("--limit", type=int, default=None, help="Ограничить количество строк (для теста)")
    ap.add_argument("--batch-size", type=int, default=2000)
    args = ap.parse_args()
    p = Path(args.csv).expanduser().resolve()
    if not p.is_file():
        raise SystemExit(f"Нет файла: {p}")
    asyncio.run(import_csv(p, limit=args.limit, batch_size=int(args.batch_size)))


if __name__ == "__main__":
    main()

