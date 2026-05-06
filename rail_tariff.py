# -*- coding: utf-8 -*-
"""
Расчёт доставки Ж/Д для финального экрана (вагоны + стоимость).
Расстояние для тарифа: Тарифное руководство №4 (если подключён модуль) иначе оценка по гео.

Ж/Д: по тарифному расстоянию интерполируем эталонную «ставку ₽/т» из CSV (см. data/rail_*.csv).
Дизель — data/rail_rzd_benchmarks.csv; прочие марки — data/rail_benchmark_*.csv.
Порог короткого/длинного плеча (интерполяция): RAIL_SHORT_HAUL_KM (по умолчанию 200) для всех марок.
"""
from __future__ import annotations

import importlib.util
import logging
import math
import os
import csv
import sqlite3
import sys
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional, TypedDict, Literal

from haversine import Unit, haversine

from utils import canonical_fuel_display_name, get_delivery_rate_sync

logger = logging.getLogger(__name__)


def _ensure_railway_modules_importable() -> None:
    """
    Модули в каталоге `railway/` импортируют друг друга как `book2_parser`, `book1_parser_graph` и т.д.
    Без добавления этого каталога в sys.path на VPS получается No module named 'book1_parser_graph'.
    """
    rd = Path(__file__).resolve().parent / "railway"
    if rd.is_dir():
        rs = str(rd)
        if rs not in sys.path:
            sys.path.insert(0, rs)


_ensure_railway_modules_importable()

# Условная грузоподъёмность цистерны (т), для отображения в боте (если продукт не передан)
DEFAULT_TONS_PER_WAGON = 60.0

# Насколько ж/д маршрут длиннее прямой (если нет ТР №4), типично 1.08–1.25
DEFAULT_RAIL_ROUTE_FACTOR = float(os.getenv("RAIL_ROUTE_FACTOR", "1.15"))
DEFAULT_RAIL_DELIVERY_MODE = os.getenv("RAIL_DELIVERY_MODE", "full").strip().lower()
RAIL_SHORT_HAUL_KM = float(os.getenv("RAIL_SHORT_HAUL_KM", "200"))

_DATA_DIR = Path(__file__).resolve().parent / "data"
_RAIL_PROFILE_FILES: dict[str, Path] = {
    "ai92_95": _DATA_DIR / "rail_benchmark_ai92_ai95.csv",
    "ai100": _DATA_DIR / "rail_benchmark_ai100.csv",
    "mazut": _DATA_DIR / "rail_benchmark_mazut.csv",
    "ts1": _DATA_DIR / "rail_benchmark_ts1.csv",
}

_DIESEL_BENCHMARK_ROWS: list[dict[str, float]] | None = None
_PROFILE_ROWS_CACHE: dict[str, list[dict[str, float]]] = {}


def _rail_fuel_profile(product_name: str | None) -> str:
    """
    Профиль эталонных ставок (имя файла data/rail_benchmark_*.csv или diesel).
    """
    if not (product_name or "").strip():
        return "diesel"
    canon = canonical_fuel_display_name(str(product_name).strip())
    if canon.startswith("ДТ-"):
        return "diesel"
    if canon in ("АИ-92-К5", "АИ-95-К5"):
        return "ai92_95"
    if canon == "АИ-100-К5":
        return "ai100"
    if canon == "Мазут топочный М100":
        return "mazut"
    if canon == "ТС-1":
        return "ts1"
    return "diesel"


def _tons_per_wagon_for_profile(profile: str) -> float:
    if profile in ("diesel", "mazut", "ts1"):
        return 65.0
    return 60.0


def _parse_benchmark_csv(path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    if not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                d = float(str(r.get("tariff_distance_km", "0")).replace(" ", "").replace(",", "."))
                rate_per_ton = float(str(r.get("rate_per_ton_rub", "0")).replace(" ", "").replace(",", "."))
                if d > 0 and rate_per_ton > 0:
                    rows.append(
                        {
                            "distance_km": d,
                            "rate_per_ton_rub": rate_per_ton,
                            "transportation_rub": float(str(r.get("transportation_rub", "0")).replace(" ", "").replace(",", ".")),
                            "security_rub": float(str(r.get("security_rub", "0")).replace(" ", "").replace(",", ".")),
                            "wagon_provision_rub": float(str(r.get("wagon_provision_rub", "0")).replace(" ", "").replace(",", ".")),
                        }
                    )
            except Exception:
                continue
    rows.sort(key=lambda x: x["distance_km"])
    return rows


def _load_diesel_benchmark_rows() -> list[dict[str, float]]:
    global _DIESEL_BENCHMARK_ROWS
    if _DIESEL_BENCHMARK_ROWS is not None:
        return _DIESEL_BENCHMARK_ROWS
    path = _DATA_DIR / "rail_rzd_benchmarks.csv"
    _DIESEL_BENCHMARK_ROWS = _parse_benchmark_csv(path)
    return _DIESEL_BENCHMARK_ROWS


def _load_benchmark_rows_for_profile(profile: str) -> list[dict[str, float]]:
    if profile == "diesel":
        return _load_diesel_benchmark_rows()
    if profile in _PROFILE_ROWS_CACHE:
        return _PROFILE_ROWS_CACHE[profile]
    path = _RAIL_PROFILE_FILES.get(profile)
    rows = _parse_benchmark_csv(path) if path else []
    if not rows:
        logger.warning("Нет строк в %s для профиля %s — fallback на дизельный эталон", path, profile)
        rows = _load_diesel_benchmark_rows()
    _PROFILE_ROWS_CACHE[profile] = rows
    return rows


def _load_rail_benchmarks() -> list[dict[str, float]]:
    """Совместимость: эталон дизеля (как раньше)."""
    return _load_diesel_benchmark_rows()


def _interpolate_by_distance(distance_km: float, rows: list[dict[str, float]], key: str) -> float:
    if not rows:
        return 0.0
    d = float(distance_km)
    if d <= rows[0]["distance_km"]:
        return float(rows[0][key])
    if d >= rows[-1]["distance_km"]:
        return float(rows[-1][key])
    for i in range(1, len(rows)):
        a = rows[i - 1]
        b = rows[i]
        da = a["distance_km"]
        db = b["distance_km"]
        if da <= d <= db and db > da:
            t = (d - da) / (db - da)
            return float(a[key]) + t * (float(b[key]) - float(a[key]))
    return float(rows[-1][key])


def _segmented_rate_per_ton_rub(distance_km: float, rows: list[dict[str, float]], short_km: float) -> float:
    """
    Интерполяция ставки ₽/т: отдельно «короткое» и «длинное» плечо (по умол. 200 км), если точек достаточно.
    """
    if not rows:
        return 0.0
    d = float(distance_km)
    short_km = float(short_km)
    rows = sorted(rows, key=lambda x: x["distance_km"])
    short = [r for r in rows if r["distance_km"] <= short_km]
    long = [r for r in rows if r["distance_km"] >= short_km]
    if d <= short_km and len(short) >= 2:
        if d > float(short[-1]["distance_km"]):
            # Нет эталона на «хвост» до порога (у дизеля часто первые точки 35/50 км, дальше пусто до 300+).
            return _interpolate_by_distance(d, rows, "rate_per_ton_rub")
        return _interpolate_by_distance(d, short, "rate_per_ton_rub")
    if d > short_km and len(long) >= 2:
        anchor: float | None = None
        if len(short) >= 2:
            anchor = _interpolate_by_distance(short_km, short, "rate_per_ton_rub")
        long_use = list(long)
        if anchor is not None and long_use and long_use[0]["distance_km"] > short_km + 1e-9:
            long_use = [
                {
                    "distance_km": short_km,
                    "rate_per_ton_rub": anchor,
                    "transportation_rub": 0.0,
                    "security_rub": 0.0,
                    "wagon_provision_rub": 0.0,
                }
            ] + long_use
        long_use.sort(key=lambda x: x["distance_km"])
        return _interpolate_by_distance(d, long_use, "rate_per_ton_rub")
    return _interpolate_by_distance(d, rows, "rate_per_ton_rub")


def straight_line_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    return float(haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS))


def compute_rail_tariff_distance_km(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str] = None,
    dest_esr: Optional[str] = None,
) -> float:
    """
    Километраж для применения ставки Ж/Д.

    1) Если задан RZD_TR4_MODULE (путь к .py), вызывается
       ``distance_km(origin_esr, dest_esr)`` или ``tariff4_distance(...)`` при наличии.
    2) Иначе пытаемся расчёт по локальным Книгам 2/3 (railway/logistics.py).
    3) Если и это не удалось: прямая между точками * DEFAULT_RAIL_ROUTE_FACTOR.
    """
    straight = straight_line_km(origin_lat, origin_lon, dest_lat, dest_lon)
    geo = max(1.0, straight * DEFAULT_RAIL_ROUTE_FACTOR)

    def _clamp_distance(d: float) -> float:
        """
        Защита от «неадекватных» ESR-расстояний.
        Не отбрасываем полностью, а "зажимаем" в разумный коридор относительно гео-оценки,
        чтобы не получались слишком дешёвые/короткие или наоборот запредельно длинные маршруты
        из-за неверных кодов/справочников.
        """
        if d <= 0:
            return geo
        lo = 0.85 * geo
        hi = 2.20 * geo
        return max(lo, min(hi, float(d)))

    def _canon_esr6(v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        s = str(v).strip().replace(" ", "").replace(".0", "")
        if not s.isdigit():
            return None
        if len(s) == 5:
            s = s.zfill(6)
        if len(s) != 6:
            return None
        return s

    o_esr = _canon_esr6(origin_esr)
    d_esr = _canon_esr6(dest_esr)

    strict_tr4 = (os.getenv("TR4_STRICT", "") or "0").strip().lower() in ("1", "true", "yes", "on")
    tr4_path = os.getenv("RZD_TR4_MODULE", "").strip()
    if tr4_path and o_esr and d_esr:
        try:
            spec = importlib.util.spec_from_file_location("rzd_tr4_dynamic", tr4_path)
            mod = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(mod)  # type: ignore
            if hasattr(mod, "distance_km"):
                d = float(mod.distance_km(str(o_esr), str(d_esr)))
                if d > 0:
                    return _clamp_distance(d)
            if hasattr(mod, "tariff4_distance"):
                d = float(mod.tariff4_distance(str(o_esr), str(d_esr)))
                if d > 0:
                    return _clamp_distance(d)
        except Exception as exc:
            logger.warning("ТР №4 модуль не сработал (%s), используем гео-оценку", exc)

    if o_esr and d_esr:
        try:
            d = _distance_from_local_tariff_books(str(o_esr), str(d_esr))
            if d > 0:
                return _clamp_distance(d)
        except Exception as exc:
            logger.warning("Локальные Книги 2/3 не сработали (%s), используем гео-оценку", exc)

    if strict_tr4:
        raise ValueError(f"TR4_STRICT: cannot compute TR4 distance for ESR {origin_esr!r}->{dest_esr!r}")

    return geo


_DIST_CACHE_LOCK = threading.Lock()
_DIST_CACHE: dict[str, tuple[float, float]] = {}

def _dist_cache_db_path() -> str:
    """
    Путь к дисковому кэшу дистанций ТР №4.
    По умолчанию: ./data/tr4_dist_cache.db (рядом с проектом).
    """
    p = (os.getenv("TR4_DIST_CACHE_DB") or "").strip()
    if p:
        return p
    return str((Path(__file__).resolve().parent / "data" / "tr4_dist_cache.db"))


def _ensure_dist_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tr4_dist_cache (
          k TEXT PRIMARY KEY,
          exp INTEGER NOT NULL,
          v REAL NOT NULL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tr4_dist_cache_exp ON tr4_dist_cache(exp);")


def _disk_cache_get(key: str, now: float) -> Optional[float]:
    if not (os.getenv("TR4_DIST_CACHE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")):
        return None
    db_path = _dist_cache_db_path()
    try:
        conn = sqlite3.connect(db_path, timeout=2.0)
        try:
            _ensure_dist_cache_table(conn)
            cur = conn.cursor()
            cur.execute("SELECT exp, v FROM tr4_dist_cache WHERE k=? LIMIT 1", (key,))
            row = cur.fetchone()
            if not row:
                return None
            exp, v = int(row[0]), float(row[1])
            if exp >= int(now):
                return float(v)
            # expired
            try:
                cur.execute("DELETE FROM tr4_dist_cache WHERE k=?", (key,))
                conn.commit()
            except Exception:
                pass
            return None
        finally:
            conn.close()
    except Exception:
        return None


def _disk_cache_set(key: str, *, exp: float, val: float) -> None:
    if not (os.getenv("TR4_DIST_CACHE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")):
        return
    db_path = _dist_cache_db_path()
    try:
        conn = sqlite3.connect(db_path, timeout=2.0)
        try:
            _ensure_dist_cache_table(conn)
            conn.execute(
                "INSERT OR REPLACE INTO tr4_dist_cache(k, exp, v) VALUES(?, ?, ?)",
                (key, int(exp), float(val)),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        return


def _dist_cache_key(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str],
    dest_esr: Optional[str],
) -> str:
    """
    Ключ кэша для расстояний.
    Приоритет — ESR→ESR, иначе округлённые координаты.
    """
    def _canon(v: Optional[str]) -> str:
        if not v:
            return ""
        s = str(v).strip().replace(" ", "").replace(".0", "")
        if s.isdigit() and len(s) in (5, 6):
            return s.zfill(6)
        return s

    o = _canon(origin_esr)
    d = _canon(dest_esr)
    if o and d:
        return f"esr:{o}->{d}"
    # округляем, чтобы кэш работал на близких точках
    return (
        "geo:"
        f"{float(origin_lat):.3f},{float(origin_lon):.3f}"
        "->"
        f"{float(dest_lat):.3f},{float(dest_lon):.3f}"
    )


def compute_rail_tariff_distance_km_cached(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str] = None,
    dest_esr: Optional[str] = None,
    *,
    ttl_sec: float = 24 * 60 * 60,
) -> float:
    """
    То же, что compute_rail_tariff_distance_km, но с TTL-кэшем.

    Это безопасная оптимизация: кэшируем чистую функцию, которая зависит только от входных параметров
    и локальных справочников/настроек.
    """
    key = _dist_cache_key(origin_lat, origin_lon, dest_lat, dest_lon, origin_esr, dest_esr)
    now = time.time()
    with _DIST_CACHE_LOCK:
        hit = _DIST_CACHE.get(key)
        if hit is not None:
            exp, val = hit
            if exp >= now:
                return float(val)
            _DIST_CACHE.pop(key, None)

    # 2) Disk cache (shared across uvicorn workers / survives restarts)
    d_hit = _disk_cache_get(key, now)
    if d_hit is not None:
        with _DIST_CACHE_LOCK:
            _DIST_CACHE[key] = (now + float(ttl_sec), float(d_hit))
        return float(d_hit)

    val = float(
        compute_rail_tariff_distance_km(
            origin_lat,
            origin_lon,
            dest_lat,
            dest_lon,
            origin_esr=origin_esr,
            dest_esr=dest_esr,
        )
    )

    with _DIST_CACHE_LOCK:
        _DIST_CACHE[key] = (now + float(ttl_sec), val)
        # простая защита от разрастания
        if len(_DIST_CACHE) > 50000:
            _DIST_CACHE.clear()
    _disk_cache_set(key, exp=now + float(ttl_sec), val=float(val))
    return val


class RailDistanceDebug(TypedDict, total=False):
    distance_km: float
    source: Literal["tr4_module", "local_books", "geo_fallback"]
    origin_esr: str
    dest_esr: str
    origin_lat: float
    origin_lon: float
    dest_lat: float
    dest_lon: float
    straight_km: float
    route_factor: float
    error: str


def compute_rail_tariff_distance_debug(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str] = None,
    dest_esr: Optional[str] = None,
) -> RailDistanceDebug:
    """
    То же что compute_rail_tariff_distance_km, но возвращает источник и детали,
    чтобы можно было показать «почему так» в боте.
    """
    dbg: RailDistanceDebug = {
        "origin_lat": float(origin_lat),
        "origin_lon": float(origin_lon),
        "dest_lat": float(dest_lat),
        "dest_lon": float(dest_lon),
        "route_factor": float(DEFAULT_RAIL_ROUTE_FACTOR),
    }
    if origin_esr:
        dbg["origin_esr"] = str(origin_esr)
    if dest_esr:
        dbg["dest_esr"] = str(dest_esr)

    straight = straight_line_km(origin_lat, origin_lon, dest_lat, dest_lon)
    geo = max(1.0, float(straight) * float(DEFAULT_RAIL_ROUTE_FACTOR))
    dbg["straight_km"] = float(straight)

    def _clamp_distance(d: float) -> float:
        lo = 0.85 * geo
        hi = 2.20 * geo
        if d <= 0:
            return geo
        return max(lo, min(hi, float(d)))

    def _canon_esr6(v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        s = str(v).strip().replace(" ", "").replace(".0", "")
        if not s.isdigit():
            return None
        if len(s) == 5:
            s = s.zfill(6)
        if len(s) != 6:
            return None
        return s

    o_esr = _canon_esr6(origin_esr)
    d_esr = _canon_esr6(dest_esr)
    if o_esr:
        dbg["origin_esr"] = str(o_esr)
    if d_esr:
        dbg["dest_esr"] = str(d_esr)

    strict_tr4 = (os.getenv("TR4_STRICT", "") or "0").strip().lower() in ("1", "true", "yes", "on")
    tr4_path = os.getenv("RZD_TR4_MODULE", "").strip()
    if tr4_path and o_esr and d_esr:
        try:
            spec = importlib.util.spec_from_file_location("rzd_tr4_dynamic", tr4_path)
            mod = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(mod)  # type: ignore
            if hasattr(mod, "distance_km"):
                d = float(mod.distance_km(str(o_esr), str(d_esr)))
                if d > 0:
                    dbg["distance_km"] = _clamp_distance(d)
                    dbg["source"] = "tr4_module"
                    return dbg
            if hasattr(mod, "tariff4_distance"):
                d = float(mod.tariff4_distance(str(o_esr), str(d_esr)))
                if d > 0:
                    dbg["distance_km"] = _clamp_distance(d)
                    dbg["source"] = "tr4_module"
                    return dbg
        except Exception as exc:
            dbg["error"] = f"TR4: {exc}"

    if o_esr and d_esr:
        try:
            d = _distance_from_local_tariff_books(str(o_esr), str(d_esr))
            if d > 0:
                dbg["distance_km"] = _clamp_distance(d)
                dbg["source"] = "local_books"
                return dbg
        except Exception as exc:
            dbg["error"] = (dbg.get("error", "") + f"; books: {exc}").strip("; ")

    if strict_tr4 and o_esr and d_esr:
        raise ValueError(f"TR4_STRICT: cannot compute TR4 distance for ESR {o_esr!r}->{d_esr!r}")

    dbg["distance_km"] = max(1.0, straight * DEFAULT_RAIL_ROUTE_FACTOR)
    dbg["source"] = "geo_fallback"
    return dbg


@lru_cache(maxsize=1)
def _sqlite_db_path() -> Optional[str]:
    """
    Абсолютный путь к SQLite-файлу из DATABASE_URL.
    """
    try:
        from sqlalchemy.engine.url import make_url
    except Exception:
        return None
    raw = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///fuel_bot.db")
    try:
        u = make_url(raw)
    except Exception:
        return None
    if "sqlite" not in u.drivername or not u.database:
        return None
    p = u.database
    if not os.path.isabs(p):
        p = os.path.abspath(p)
    return p


@lru_cache(maxsize=8192)
def _station_name_by_esr(esr: str) -> Optional[str]:
    db_path = _sqlite_db_path()
    if not db_path or not os.path.isfile(db_path):
        return None
    # ESR в БД иногда попадает как "932207.0" (из Excel/CSV). Нормализуем.
    key = str(esr).strip().replace(" ", "").replace(".0", "")
    if not key:
        return None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # rail_stations.esr_code может быть и "932207", и "932207.0"
        cur.execute(
            """
            SELECT name
            FROM rail_stations
            WHERE is_active = 1
              AND (
                REPLACE(REPLACE(COALESCE(esr_code,''), '.0', ''), ' ', '') = ?
              )
            LIMIT 1
            """,
            (key,),
        )
        row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] else None
    finally:
        conn.close()


@lru_cache(maxsize=4096)
def _distance_from_local_tariff_books(origin_esr: str, dest_esr: str) -> float:
    """
    Расчёт расстояния по локальным Книгам 2/3 через railway/logistics.py.
    Нужен как второй этап до гео-fallback.
    """
    origin_name = _station_name_by_esr(origin_esr)
    dest_name = _station_name_by_esr(dest_esr)
    if not origin_name or not dest_name:
        raise ValueError("Не удалось сопоставить ESR со станциями")

    module_path = Path(__file__).resolve().parent / "railway" / "logistics.py"
    if not module_path.is_file():
        raise FileNotFoundError(f"Нет модуля логистики: {module_path}")

    railway_dir = str(module_path.parent)
    if railway_dir not in sys.path:
        sys.path.insert(0, railway_dir)
    mod_name = "railway_logistics_dynamic"
    spec = importlib.util.spec_from_file_location(mod_name, str(module_path))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)  # type: ignore

    if not hasattr(mod, "logistic_distance_km"):
        raise AttributeError("В railway/logistics.py нет logistic_distance_km")

    data_root = str((Path(__file__).resolve().parent / "railway" / "data"))
    d = float(mod.logistic_distance_km(origin_name, dest_name, data_root=data_root))
    if d <= 0:
        raise ValueError("Локальный тарифный расчёт вернул неположительное расстояние")
    return d


def get_rail_rate(distance_km: float) -> float:
    """Ставка руб/(т·км) для ж/д по дистанции (как в utils для rail)."""
    return float(get_delivery_rate_sync(float(distance_km), "rail"))


def calculate_delivery_cost(
    distance_km: float,
    volume_tonns: float,
    product_name: str | None = None,
) -> dict:
    """
    Полная стоимость доставки Ж/Д и метаданные для сообщения пользователю.

    Возвращает ключи, которые ожидает bot/handlers.calculate_final_result:
    total_cost, rate_per_ton_km, wagons_needed, tons_per_wagon
    """
    d = float(distance_km)
    v = float(volume_tonns)
    if v <= 0:
        raise ValueError("Объём должен быть положительным")

    profile = _rail_fuel_profile(product_name)
    tons_w = _tons_per_wagon_for_profile(profile)

    mode = DEFAULT_RAIL_DELIVERY_MODE
    if mode == "full":
        rows = _load_benchmark_rows_for_profile(profile)
        if rows:
            rs = sorted(rows, key=lambda x: x["distance_km"])
            rate_per_ton = _segmented_rate_per_ton_rub(d, rs, RAIL_SHORT_HAUL_KM)
            total_cost = rate_per_ton * v
            rate = max(0.0001, rate_per_ton / max(1.0, d))

            transportation = _interpolate_by_distance(d, rs, "transportation_rub") * (v / tons_w)
            security = _interpolate_by_distance(d, rs, "security_rub") * (v / tons_w)
            wagon_provision = _interpolate_by_distance(d, rs, "wagon_provision_rub") * (v / tons_w)
        else:
            rate = get_rail_rate(d)
            total_cost = d * v * rate
            transportation = total_cost
            security = 0.0
            wagon_provision = 0.0
    else:
        # base: историческая формула руб/(т*км)
        rate = get_rail_rate(d)
        total_cost = d * v * rate
        transportation = total_cost
        security = 0.0
        wagon_provision = 0.0
    wagons = max(1, math.ceil(v / tons_w))

    return {
        "distance_km": round(d, 1),
        "volume_tonns": v,
        "rate_per_ton_km": round(rate, 4),
        "total_cost": round(total_cost, 2),
        "cost_per_ton": round(total_cost / v, 2),
        "wagons_needed": wagons,
        "tons_per_wagon": int(tons_w),
        "delivery_mode": mode,
        "components": {
            "transportation_rub": round(float(transportation), 2),
            "security_rub": round(float(security), 2),
            "wagon_provision_rub": round(float(wagon_provision), 2),
        },
    }


__all__ = [
    "calculate_delivery_cost",
    "compute_rail_tariff_distance_km",
    "compute_rail_tariff_distance_km_cached",
    "get_rail_rate",
    "straight_line_km",
    "DEFAULT_TONS_PER_WAGON",
    "DEFAULT_RAIL_ROUTE_FACTOR",
]
