#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Локальный расчёт тарифного расстояния (ТР-4) по книгам 1–3 из папки `railway/data`.

Этот модуль подключается через переменную окружения:
  RZD_TR4_MODULE=/opt/fuel_bot/rzd_tr4_local.py

Должен предоставлять функцию:
  distance_km(origin_esr: str, dest_esr: str) -> float

Мы интерпретируем origin_esr/dest_esr как 6-значные коды из Книги 2 (колонка "код"),
которые совпадают с нашими ESR-кодами в справочнике.
"""

from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from pathlib import Path


_CODE_RE = re.compile(r"^\d{6}$")


def _clean(s: str) -> str:
    return str(s or "").replace("\ufeff", "").strip().strip('"').strip()


def _data_root() -> str:
    # /opt/fuel_bot/railway/data on VPS
    return str((Path(__file__).resolve().parent / "railway" / "data").resolve())


@lru_cache(maxsize=1)
def _book2_code_to_station_name() -> dict[str, str]:
    """
    Строим маппинг code(6) -> station_name из Книги 2.
    Файлы: railway/data/kniga2/*.csv
    """
    root = Path(_data_root()) / "kniga2"
    out: dict[str, str] = {}
    if not root.is_dir():
        return out
    for fp in sorted(root.glob("*.csv")):
        try:
            with fp.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or len(row) < 6:
                        continue
                    station_name = _clean(row[1])
                    station_code = _clean(row[5]).replace(".0", "").replace(" ", "")
                    if not station_name:
                        continue
                    if not _CODE_RE.fullmatch(station_code):
                        continue
                    out.setdefault(station_code, station_name)
        except Exception:
            continue
    return out


def _resolve_station_name(code: str) -> str | None:
    c = _clean(code).replace(".0", "").replace(" ", "")
    if c.isdigit() and len(c) == 5:
        c = c.zfill(6)
    if not _CODE_RE.fullmatch(c):
        return None
    return _book2_code_to_station_name().get(c)


def distance_km(origin_esr: str, dest_esr: str) -> float:
    """
    Возвращает тарифное расстояние в км.
    Если данных не хватает — кидает исключение (rail_tariff поймает и уйдёт в fallback).
    """
    o_name = _resolve_station_name(origin_esr)
    d_name = _resolve_station_name(dest_esr)
    if not o_name or not d_name:
        raise ValueError(f"Не удалось сопоставить ESR с Книгой 2: {origin_esr!r} -> {dest_esr!r}")

    # Важно: расчёт по Книге 1 резолвит станции только по имени и может выбирать
    # одноимённые станции в другом регионе (например, "Ванино"), давая нереально маленькие расстояния.
    # Поэтому сначала считаем по Книгам 2+3, а Книгу 1 используем только как запасной вариант,
    # если Книг 2+3 не хватает.
    data_root = _data_root()
    try:
        from railway.logistics import logistic_distance_verbose

        _from_st, _to_st, dist = logistic_distance_verbose(o_name, d_name, data_root=data_root)
        return float(dist)
    except Exception:
        # fallback to Book1-only
        from railway.tariff4_distance import _try_calc_book1  # type: ignore

        cand = _try_calc_book1(o_name, d_name, data_root=data_root)
        if cand is None:
            raise
        _from_st, _to_st, dist = cand
        return float(dist)


def tariff4_distance(origin_esr: str, dest_esr: str) -> float:
    """Альтернативное имя, если вызывающая сторона ищет tariff4_distance."""
    return float(distance_km(origin_esr, dest_esr))


__all__ = ["distance_km", "tariff4_distance"]

