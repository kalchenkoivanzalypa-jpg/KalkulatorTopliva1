#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обновление рыночных цен из бюллетеня СПбМТСБ (PDF).

Пайплайн (как в бизнес-логике):
  1) Из PDF (секция «Метрическая тонна», много страниц) извлекаются пары: код инструмента → «Рыночная»
     (колонка под «Цена», не «Изменение рыночной цены»).
  2) В каталоге уже есть коды из «Базисы поставок» (product_basis_prices.instrument_code).
  3) Цена из PDF записывается только для строк каталога с совпадающим кодом; остальные коды PDF игнорируются.

Правила:
  • По умолчанию цена: «Рыночная»; если «–»/пусто — по очереди: средневзвешенная, максимальная,
    минимальная, лучшее предложение, лучший спрос (как в таблице бюллетеня).
  • Флаг --strict-market-column — только колонка «Рыночная» (старое поведение).
  • В каждой колонке смотрим её ячейки и несколько вправо (PDF режет строку).
  • До 8 следующих строк, если в строке с кодом цен нет.
  • Нет ни одной подходящей цены → строка не даёт обновления.

Извлечение:
  • Страницы с первого вхождения «Метрическая тонна» (нефтепродукты в тоннах).
  • Все повторы шапки «Код инструмента» внутри таблиц pdfplumber.
  • Страницы без шапки: те же номера колонок, что на последней странице с распознанной шапкой.
  • Несколько стратегий разметки таблиц (линии / по умолчанию), объединение результатов.
  • По умолчанию — все коды, подходящие под формат СПбМТСБ (A…, DE…, DT…, JET…, M…, PC… и т.д.).
  • Флаг --only-a-prefix — только бензины (коды с буквы A).
  • Несколько кодов в одной ячейке + несколько цен подряд (или в соседних ячейках) — zip по порядку.

Пример:
  python3 import_spimex_prices_from_pdf.py --pdf "./oil_20260306162000.pdf"
  python3 import_spimex_prices_from_pdf.py --bulletins-dir "./data/bulletins"
  python3 import_spimex_prices_from_pdf.py --bulletins-dir "./data/bulletins" --last-n 5
  python3 import_spimex_prices_from_pdf.py --pdf "./oil.pdf" --only-a-prefix
  python3 import_spimex_prices_from_pdf.py --pdf "./oil.pdf" --strict-market-column
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, date, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# col_code, упорядоченные индексы колонок цены (сначала «Рыночная», затем запасные)
FallbackColumnMap = Tuple[int, Tuple[int, ...]]

import pdfplumber
from sqlalchemy import select

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Типичный код СПбМТСБ по нефтепродуктам: A001CUU025A, A10KZLY060W, JET-ANK065F.
# В конце кода обычно стоит буква (A/F/W/J и т.п.). Это помогает не "захватывать"
# хвосты вроде "-82 243 3", когда код в PDF может припечься к числам.
INSTRUMENT_CODE_RE = re.compile(r"^[A-Z][0-9A-Z-]{9,14}[A-Z]$")
INSTRUMENT_CODE_TOKEN_RE = re.compile(r"[A-Z][0-9A-Z-]{9,14}[A-Z]")

# Рыночная цена ₽/т: отсекаем объёмы (60 т), кол-во договоров (22) и суммы в руб. (миллионы)
# Минимальный порог “ценоподобного” значения ₽/т.
# Меньшие числа в некоторых бюллетенях чаще оказываются объёмами/кол-вом,
# когда из-за сдвига сетки парсер промахивается с индексом колонки цены.
MARKET_PRICE_MIN = 10_000.0
MARKET_PRICE_MAX = 250_000.0

# Нефтяной бюллетень: oil_YYYYMMDDHHMMSS.pdf (14 цифр даты-времени в имени)
BULLETIN_DATETIME14_RE = re.compile(r"(\d{14})")


def bulletin_trade_date(path: Path) -> date:
    """
    Торговая дата из имени бюллетеня: oil_YYYYMMDDHHMMSS.pdf → YYYY-MM-DD.
    Если метки нет — по mtime файла.
    """
    m = BULLETIN_DATETIME14_RE.search(path.name)
    if m:
        s = m.group(1)[:8]
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    return datetime.fromtimestamp(path.stat().st_mtime).date()


@dataclass(frozen=True)
class MarketQuote:
    instrument_code: str
    market_price: float
    volume_tons: Optional[float] = None



def default_bulletins_directory() -> Path:
    """Каталог для хранения PDF бюллетеней (см. data/bulletins/README.md)."""
    return Path(__file__).resolve().parent / "data" / "bulletins"


def bulletin_sort_key(path: Path) -> Tuple[int, int, float]:
    """
    Сортировка: сначала файлы с 14 цифрами в имени (YYYYMMDDHHMMSS), затем по mtime.
    Больше ключ — новее бюллетень.
    """
    m = BULLETIN_DATETIME14_RE.search(path.name)
    ts = int(m.group(1)) if m else 0
    ranked = 1 if ts > 0 else 0
    mtime = path.stat().st_mtime
    return (ranked, ts, mtime)


def pick_latest_bulletin_pdf(directory: Path) -> Path:
    """
    Самый «свежий» PDF в каталоге по метке даты в имени (oil_20260320162000.pdf → 2026-03-20 …).
    Если в имени нет 14 цифр — по дате модификации файла.
    """
    d = directory.expanduser().resolve()
    if not d.is_dir():
        raise FileNotFoundError(f"Нет каталога бюллетеней: {d}")
    pdfs = list(d.glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"В каталоге нет *.pdf: {d}")
    chosen = max(pdfs, key=bulletin_sort_key)
    rk, ts, _ = bulletin_sort_key(chosen)
    if rk == 0:
        logger.warning(
            "В именах PDF нет фрагмента из 14 цифр (YYYYMMDDHHMMSS) — выбран файл по дате на диске: %s",
            chosen.name,
        )
    else:
        logger.info(
            "Из каталога выбран самый новый бюллетень по дате в имени: %s (ключ %s)",
            chosen.name,
            ts,
        )
    return chosen


def pick_latest_n_bulletin_pdfs(directory: Path, n: int) -> List[Path]:
    """
    До n самых новых PDF в каталоге (по тому же ключу, что pick_latest_bulletin_pdf).
    Порядок в списке: от нового к старому (первый элемент — самый свежий бюллетень).
    """
    if n < 1:
        raise ValueError("n должен быть >= 1")
    d = directory.expanduser().resolve()
    if not d.is_dir():
        raise FileNotFoundError(f"Нет каталога бюллетеней: {d}")
    pdfs = list(d.glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"В каталоге нет *.pdf: {d}")
    ranked = sorted(pdfs, key=bulletin_sort_key, reverse=True)
    if len(ranked) < n:
        logger.warning(
            "В каталоге только %s PDF, запрошено последних %s — обработаю все.",
            len(ranked),
            n,
        )
    return ranked[:n]


TABLE_SETTINGS_VARIANTS: Tuple[Optional[Dict[str, Any]], ...] = (
    None,
    {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_x_tolerance": 5,
        "intersection_y_tolerance": 5,
        "snap_tolerance": 4,
        "join_tolerance": 3,
    },
    {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_x_tolerance": 8,
        "intersection_y_tolerance": 8,
        "snap_tolerance": 6,
        "join_tolerance": 4,
    },
    {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "snap_tolerance": 5,
        "join_tolerance": 3,
    },
)


def _cell(c: Any) -> str:
    if c is None:
        return ""
    return str(c).replace("\n", " ").replace("\xa0", " ").strip()


def _parse_money(cell: str) -> Optional[float]:
    if not cell or cell in ("-", "—", "–"):
        return None
    s = re.sub(r"[^\d,\.]", "", cell)
    s = s.replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_volume_tons(cell: str) -> Optional[float]:
    """
    Объём торгов (тонны) — в бюллетене это отдельная колонка (обычно 4-я после кода/описания/базиса).
    Здесь НЕ пытаемся угадывать по всей строке, а парсим конкретную ячейку.
    """
    if not cell or cell in ("-", "—", "–"):
        return None
    t = cell.replace("\xa0", " ").strip()
    # оставляем цифры/пробелы/разделитель
    t = re.sub(r"[^\d,\. ]", "", t)
    t = t.replace(" ", "").replace(",", ".")
    if not t:
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    # фильтр здравого смысла: не отрицательное, не “суммы в рублях”
    if v < 0 or v > 50_000_000:
        return None
    return v


def _header_depth(table: List[List[Any]], start: int, max_depth: int = 6) -> int:
    depth = 0
    for i in range(start, min(start + max_depth, len(table))):
        row = table[i] or []
        texts = [_cell(x) for x in row if _cell(x)]
        if not texts:
            depth += 1
            continue
        if any(
            INSTRUMENT_CODE_RE.fullmatch(t.replace(" ", "")) for t in texts
        ):
            break
        depth += 1
    return max(1, depth)


def _col_label(table: List[List[Any]], header_start: int, depth: int, j: int) -> str:
    parts: List[str] = []
    for r in range(header_start, min(header_start + depth, len(table))):
        row = table[r] or []
        if j < len(row):
            t = _cell(row[j])
            if t:
                parts.append(t)
    return " ".join(parts).lower()


def _is_market_price_column_header(label: str) -> bool:
    """
    Колонка «Рыночная» в блоке «Цена (за единицу измерения), руб.».
    Не путать с «Изменение рыночной цены к цене предыдущего дня» — там тоже есть «рыночн».
    """
    if "рыночн" not in label:
        return False
    # группа «изменение … рыночной цены»
    if "изменен" in label:
        return False
    if "предыдущ" in label:
        return False
    return True


def _header_price_column_indices(
    table: List[List[Any]], header_start: int, depth: int, width: int
) -> Dict[str, int]:
    """Индексы колонок по подписи шапки (нижний уровень + склейка с родителем)."""
    out: Dict[str, int] = {
        "market": -1,
        "weighted": -1,
        "max": -1,
        "min": -1,
        "best_offer": -1,
        "best_bid": -1,
    }
    for j in range(width):
        label = _col_label(table, header_start, depth, j)
        if "код" in label and "инструмент" in label:
            continue
        if _is_market_price_column_header(label):
            out["market"] = j
        if "средневзвеш" in label or (
            "средн" in label and "взвеш" in label
        ):
            out["weighted"] = j
        if "максимальн" in label:
            out["max"] = j
        if "минимальн" in label and "изменен" not in label:
            out["min"] = j
        if "предложен" in label and (
            "лучш" in label or "лучшее" in label
        ):
            out["best_offer"] = j
        if "спрос" in label and ("лучш" in label or "лучшее" in label):
            out["best_bid"] = j
    if out["market"] < 0:
        for j in range(width):
            label = _col_label(table, header_start, depth, j)
            if "рыночн" in label and "изменен" not in label:
                out["market"] = j
                break
    return out


def _price_column_chain(
    idx: Dict[str, int], *, strict_market_only: bool
) -> Tuple[int, ...]:
    """
    Порядок опроса колонок. Сначала рыночная, затем наиболее близкие по смыслу замены.
    """
    if strict_market_only:
        m = idx.get("market", -1)
        return (m,) if m >= 0 else tuple()
    order = (
        "market",
        "weighted",
        "max",
        "min",
        "best_offer",
        "best_bid",
    )
    seen: set[int] = set()
    chain: List[int] = []
    for key in order:
        j = idx.get(key, -1)
        if j >= 0 and j not in seen:
            seen.add(j)
            chain.append(j)
    return tuple(chain)


def _map_columns(
    table: List[List[Any]],
    header_start: int,
    *,
    strict_market_only: bool,
) -> Optional[Tuple[int, int, Tuple[int, ...]]]:
    depth = _header_depth(table, header_start)
    width = max(
        (len(row or []) for row in table[header_start : header_start + depth]),
        default=0,
    )
    col_code = -1
    for j in range(width):
        label = _col_label(table, header_start, depth, j)
        if "код" in label and "инструмент" in label:
            col_code = j
    idx = _header_price_column_indices(
        table, header_start, depth, width
    )
    chain = _price_column_chain(idx, strict_market_only=strict_market_only)
    if col_code < 0 or not chain:
        return None
    return header_start + depth, col_code, chain


def _find_header_row_indices(table: List[List[Any]]) -> List[int]:
    """Все строки, где начинается шапка (в т.ч. повтор на следующей странице в одной таблице)."""
    out: List[int] = []
    for i, row in enumerate(table):
        if not row:
            continue
        joined = " ".join(_cell(c) for c in row).lower()
        if "код" in joined and "инструмент" in joined:
            out.append(i)
    return out


def _accept_code(code: str, only_a_prefix: bool) -> bool:
    if not INSTRUMENT_CODE_RE.fullmatch(code):
        return False
    if only_a_prefix and not code.startswith("A"):
        return False
    return True


def _extract_codes_from_cell(text: str, only_a_prefix: bool) -> List[str]:
    """
    В бюллетене часто несколько кодов в одной ячейке: «A001CUU025A A001KRU060F …».
    Ищем подстроки по шаблону и отдельные токены (пробелы — разделитель; в Excel код без пробелов).
    """
    if not text:
        return []
    u = text.upper().replace("\xa0", " ").strip()
    seen: set[str] = set()
    ordered: List[str] = []

    def _take(cand: str) -> None:
        if not INSTRUMENT_CODE_RE.fullmatch(cand):
            return
        if not _accept_code(cand, only_a_prefix):
            return
        if cand not in seen:
            seen.add(cand)
            ordered.append(cand)

    # 1) Главная эвристика: удаляем пробелы/переносы внутри кода.
    # В PDF коды могут быть разорваны, например: "DSC5AN K065F" -> "DSC5ANK065F".
    compact_ws = re.sub(r"\s+", "", u)
    for m in re.finditer(INSTRUMENT_CODE_TOKEN_RE, compact_ws):
        _take(m.group(0))

    # 2) В редких случаях код "прилипает" к числу после дефиса/разделителя:
    # "DSC5ANK065F-82 243 ..." — тогда удаление дефисов может помочь собрать "чистый"
    # код. Но для JET- (внутренний дефис обязателен) это лучше не применять.
    if not ordered and "JET-" not in u:
        compact_no_hyphen = compact_ws.replace("-", "")
        for m in re.finditer(r"[A-Z][0-9A-Z]{9,14}[A-Z]", compact_no_hyphen):
            _take(m.group(0))

    # 3) Фоллбек: пробуем по оригиналу (на случай "чистых" кодов без разрывов)
    if not ordered:
        for m in re.finditer(INSTRUMENT_CODE_TOKEN_RE, u):
            _take(m.group(0))
        # «A100UF M060F» в PDF → токены после сплита
        for tok in re.split(r"\s+", u):
            tok = tok.strip()
            if len(tok) >= 11:
                _take(tok)
    return ordered


def _money_candidates_in_string(s: str) -> List[float]:
    """
    Все числа, похожие на цену ₽/т: «94000», «94 000», «92 460,5».
    Отсекаем миллионы (объём в руб.) и мелкие числа (тонны, шт.).
    """
    if not s:
        return []
    t = s.replace("\xa0", " ")
    out: List[float] = []
    for m in re.finditer(
        # Важно: не выцеплять подстроки из длинных чисел (например, 184405000 → “184405”).
        r"(?<!\d)(\d{1,3}(?:\s+\d{3})+(?:[.,]\d+)?|\d{5,6}(?:[.,]\d+)?|\d{4}(?:[.,]\d+)?)(?!\d)",
        t,
    ):
        chunk = m.group(1).replace(" ", "").replace(",", ".")
        try:
            v = float(chunk)
        except ValueError:
            continue
        if MARKET_PRICE_MIN <= v <= MARKET_PRICE_MAX:
            out.append(v)
    return out


def _ordered_money_candidates_from_row(row: List[Any]) -> List[float]:
    """Все «ценоподобные» числа слева направо по ячейкам (для запасной эвристики)."""
    out: List[float] = []
    for j in range(len(row or [])):
        out.extend(_money_candidates_in_string(_cell(row[j])))
    return out


def _guess_market_price_for_single_code_row(nums: List[float]) -> List[float]:
    """
    Если сдвинулась сетка колонок: в блоке «Цена» часто порядок Мин → Средн → Макс → Рыночная.
    Берём 4-е число в диапазоне; иначе — единственное или крайнее справа.
    """
    if not nums:
        return []
    if len(nums) == 1:
        return [nums[0]]
    if len(nums) >= 4:
        return [nums[3]]
    return [nums[-1]]


def _prices_from_row_start_col(
    table: List[List[Any]],
    r: int,
    col_start: int,
    max_span: int = 8,
) -> List[float]:
    row = table[r] or []
    prices: List[float] = []
    if col_start < len(row):
        for j in range(col_start, min(len(row), col_start + max_span)):
            prices.extend(_money_candidates_in_string(_cell(row[j])))
    return prices


def _row_prices_chain(
    table: List[List[Any]],
    r: int,
    col_chain: Tuple[int, ...],
    row_end: int,
    max_lookahead: int = 8,
    span: int = 8,
    allow_row_lookahead: bool = True,
) -> List[float]:
    """
    По очереди колонки цены (Рыночная → запасные). Первая непустая даёт список чисел для zip.
    """
    for c in col_chain:
        acc = _prices_from_row_start_col(table, r, c, span)
        if acc:
            return acc
    # Важно: если коды найдены ровно один раз в текущей строке, lookahead
    # по соседним строкам может сломать соответствие код -> цена.
    if allow_row_lookahead:
        for rr in range(r + 1, min(r + 1 + max_lookahead, row_end, len(table))):
            prow = table[rr] or []
            if not prow:
                continue
            for c in col_chain:
                j0 = c if c < len(prow) else 0
                acc: List[float] = []
                for j in range(j0, min(len(prow), j0 + span)):
                    acc.extend(_money_candidates_in_string(_cell(prow[j])))
                if acc:
                    return acc
    return []


def _fallback_market_prices_single_code(
    table: List[List[Any]],
    r: int,
    row_end: int,
    max_lookahead: int = 8,
) -> List[float]:
    """Когда индекс колонки «Рыночная» промахнулся — ищем по всей строке (один код в строке)."""
    row = table[r] or []
    nums = _ordered_money_candidates_from_row(row)
    g = _guess_market_price_for_single_code_row(nums)
    if g:
        return g
    for rr in range(r + 1, min(r + 1 + max_lookahead, row_end, len(table))):
        prow = table[rr] or []
        nums2 = _ordered_money_candidates_from_row(prow)
        g2 = _guess_market_price_for_single_code_row(nums2)
        if g2:
            return g2
    return []


def _pair_codes_and_prices(
    codes: List[str], prices: List[float]
) -> List[Tuple[str, float]]:
    if not codes or not prices:
        return []
    if len(codes) == len(prices):
        return list(zip(codes, prices))
    if len(codes) == 1:
        return [(codes[0], prices[0])]
    if len(prices) >= len(codes):
        return list(zip(codes, prices[: len(codes)]))
    if len(prices) == 1 and len(codes) > 1:
        return [(c, prices[0]) for c in codes]
    # Несколько кодов в ячейке, цен меньше — сохраняем первые пары (лучше частично, чем 0)
    if 1 < len(prices) < len(codes):
        return list(zip(codes[: len(prices)], prices))
    return []


def _codes_from_row_cells(
    row: List[Any], col_code: int, only_a_prefix: bool
) -> List[str]:
    """Код в основной колонке или в соседней (PDF иногда сдвигает на +1)."""
    if col_code < len(row):
        found = _extract_codes_from_cell(_cell(row[col_code]), only_a_prefix)
        if found:
            return found
    if col_code + 1 < len(row):
        return _extract_codes_from_cell(_cell(row[col_code + 1]), only_a_prefix)
    return []


def _rows_data_loop(
    table: List[List[Any]],
    data_start: int,
    row_end: int,
    col_code: int,
    price_col_chain: Tuple[int, ...],
    only_a_prefix: bool,
    *,
    prefer_single_code_row_guess: bool = False,
) -> List[Tuple[str, float]]:
    chunk: List[Tuple[str, float]] = []
    for r in range(data_start, min(row_end, len(table))):
        row = table[r] or []
        if col_code >= len(row) and col_code + 1 >= len(row):
            continue
        codes = _codes_from_row_cells(row, col_code, only_a_prefix)
        if not codes:
            continue
        # Для строк с одним кодом часто надёжнее брать «рыночную» как 4-е число
        # в ряду (min→средн→max→рыночная), если такие числа присутствуют в строке.
        # Это защищает от редких смещений колонок/шапки и не ломает случаи,
        # когда в строке действительно есть 4 ценовые колонки.
        if len(codes) == 1:
            nums = _ordered_money_candidates_from_row(row)
            g = _guess_market_price_for_single_code_row(nums)
            if g:
                prices = g
            else:
                prices = _row_prices_chain(
                    table,
                    r,
                    price_col_chain,
                    row_end,
                    allow_row_lookahead=False,
                )
                if not prices:
                    prices = _fallback_market_prices_single_code(table, r, row_end)
        else:
            prices = _row_prices_chain(
                table,
                r,
                price_col_chain,
                row_end,
                allow_row_lookahead=(len(codes) > 1),
            )
            if not prices and len(codes) == 1:
                prices = _fallback_market_prices_single_code(table, r, row_end)
        pairs = _pair_codes_and_prices(codes, prices)
        chunk.extend(pairs)
    return chunk


def _rows_quotes_loop(
    table: List[List[Any]],
    data_start: int,
    row_end: int,
    col_code: int,
    price_col_chain: Tuple[int, ...],
    only_a_prefix: bool,
) -> List[MarketQuote]:
    """
    Как _rows_data_loop, но возвращает MarketQuote (цена + объём).
    Объём берём из ячейки row[3] (как в бюллетенях, где 4-я колонка — «Объем ... в единицах измерения»).
    """
    out: List[MarketQuote] = []
    for r in range(data_start, min(row_end, len(table))):
        row = table[r] or []
        if col_code >= len(row) and col_code + 1 >= len(row):
            continue
        codes = _codes_from_row_cells(row, col_code, only_a_prefix)
        if not codes:
            continue

        # цена: используем ту же безопасную логику, что и в _rows_data_loop
        if len(codes) == 1:
            nums = _ordered_money_candidates_from_row(row)
            g = _guess_market_price_for_single_code_row(nums)
            if g:
                prices = g
            else:
                prices = _row_prices_chain(
                    table,
                    r,
                    price_col_chain,
                    row_end,
                    allow_row_lookahead=False,
                )
                if not prices:
                    prices = _fallback_market_prices_single_code(table, r, row_end)
        else:
            prices = _row_prices_chain(
                table,
                r,
                price_col_chain,
                row_end,
                allow_row_lookahead=True,
            )

        pairs = _pair_codes_and_prices(codes, prices)
        if not pairs:
            continue

        vol = None
        if len(codes) == 1 and len(row) > 3:
            vol = _parse_volume_tons(_cell(row[3]))

        for code, price in pairs:
            out.append(MarketQuote(instrument_code=code, market_price=float(price), volume_tons=vol))
    return out


def _rows_from_segment(
    table: List[List[Any]],
    header_start: int,
    row_end: int,
    only_a_prefix: bool,
    *,
    strict_market_only: bool,
) -> Tuple[List[Tuple[str, float]], Optional[FallbackColumnMap]]:
    """Пары и карта колонок для страниц без повторной шапки."""
    mapped = _map_columns(
        table, header_start, strict_market_only=strict_market_only
    )
    if not mapped:
        return [], None
    data_start, col_code, chain = mapped
    chunk = _rows_data_loop(
        table, data_start, row_end, col_code, chain, only_a_prefix
    )
    return chunk, (col_code, chain)


def _quotes_from_segment(
    table: List[List[Any]],
    header_start: int,
    row_end: int,
    only_a_prefix: bool,
    *,
    strict_market_only: bool,
) -> Tuple[List[MarketQuote], Optional[FallbackColumnMap]]:
    mapped = _map_columns(table, header_start, strict_market_only=strict_market_only)
    if not mapped:
        return [], None
    data_start, col_code, chain = mapped
    chunk = _rows_quotes_loop(table, data_start, row_end, col_code, chain, only_a_prefix)
    return chunk, (col_code, chain)


def _table_has_data_like_rows(
    table: List[List[Any]], only_a_prefix: bool, min_hits: int = 2
) -> bool:
    """Строки с кодом в первых ячейках — типичное тело таблицы бюллетеня."""
    hits = 0
    for row in table[:80]:
        if not row:
            continue
        scan = row[:6] if len(row) >= 6 else row
        for cell in scan:
            if _extract_codes_from_cell(_cell(cell), only_a_prefix):
                hits += 1
                break
        if hits >= min_hits:
            return True
    return False


def _continuation_table_has_multicode_code_cells(
    table: List[List[Any]],
    col_code: int,
    only_a_prefix: bool,
    *,
    scan_rows: int = 80,
    max_ratio: float = 0.15,
) -> bool:
    """
    На страницах «продолжение без шапки» некоторые настройки extract_tables
    дают “склеенную” таблицу: в одной ячейке сразу несколько instrument_code
    (часто разделены переводами строк). Для таких таблиц соответствие код→цена
    становится хрупким, особенно когда карта колонок (fallback_cols) пришла
    с предыдущей страницы и могла сместиться.

    Возвращает True, если доля multi-code ячеек в предполагаемой колонке кода
    слишком высокая.
    """
    multi = 0
    hits = 0
    for row in (table or [])[:scan_rows]:
        if not row:
            continue
        cells: List[Any] = []
        if col_code < len(row):
            cells.append(row[col_code])
        if col_code + 1 < len(row):
            cells.append(row[col_code + 1])
        best = 0
        for cell in cells:
            c = _cell(cell)
            if not c:
                continue
            n = len(_extract_codes_from_cell(c, only_a_prefix))
            if n > best:
                best = n
        if best <= 0:
            continue
        hits += 1
        if best >= 2:
            multi += 1
    if hits == 0:
        return False
    return (multi / hits) > max_ratio


def _extract_from_table(
    table: List[List[Any]],
    only_a_prefix: bool,
    fallback_cols: Optional[FallbackColumnMap],
    *,
    strict_market_only: bool,
) -> Tuple[List[Tuple[str, float]], Optional[FallbackColumnMap], bool]:
    """
    Если шапки нет (часто на 2–14-й странице), используем номера колонок
    с последней таблицы, где шапка распозналась.
    Третий элемент — True, если сработал режим «продолжение без шапки».
    """
    if not table:
        return [], fallback_cols, False

    out: List[Tuple[str, float]] = []
    new_fallback = fallback_cols

    headers = _find_header_row_indices(table)
    if headers:
        for hi, hstart in enumerate(headers):
            row_end = headers[hi + 1] if hi + 1 < len(headers) else len(table)
            seg, cols = _rows_from_segment(
                table,
                hstart,
                row_end,
                only_a_prefix,
                strict_market_only=strict_market_only,
            )
            out.extend(seg)
            if cols is not None:
                new_fallback = cols
        return out, new_fallback, False

    if fallback_cols is not None and _table_has_data_like_rows(
        table, only_a_prefix
    ):
        cc, chain = fallback_cols
        # Пропускаем «склеенные» таблицы с несколькими кодами в одной ячейке
        # (иначе они могут перетирать корректные значения из “нормальной”
        # развернутой таблицы, полученной другим table_settings вариантом).
        if _continuation_table_has_multicode_code_cells(
            table, cc, only_a_prefix
        ):
            return [], new_fallback, True
        out.extend(
            _rows_data_loop(
                table,
                0,
                len(table),
                cc,
                chain,
                only_a_prefix,
                prefer_single_code_row_guess=True,
            )
        )
        logger.debug(
            "Таблица без шапки: ~%s строк, col_code=%s chain=%s, пар=%s",
            len(table),
            cc,
            chain,
            len(out),
        )
        return out, new_fallback, True
    return out, new_fallback, False


def extract_market_quotes_from_pdf(
    path: Path,
    *,
    only_a_prefix: bool = False,
    strict_market_only: bool = False,
    log_extracted_codes: bool = False,
) -> List[MarketQuote]:
    """
    Возвращает котировки: instrument_code → (рыночная цена, объём).
    Объём доступен не всегда (зависит от разметки таблицы), тогда volume_tons=None.
    """
    merged: Dict[str, MarketQuote] = {}
    seen_blocks = 0
    continuation_blocks = 0
    fallback_cols: Optional[FallbackColumnMap] = None

    with pdfplumber.open(str(path)) as pdf:
        pages = list(pdf.pages)
        start = _first_metric_ton_page_index(pages)
        page_slice = pages if start is None else pages[start:]
        if start is None:
            logger.warning("В PDF не найдена секция «Метрическая тонна» — разбор всех страниц (шумнее).")
        else:
            logger.info(
                "Секция «Метрическая тонна»: страницы с %s (1-based: %s) из %s",
                start,
                start + 1,
                len(pages),
            )

        for page in page_slice:
            for ts in TABLE_SETTINGS_VARIANTS:
                kwargs = {} if ts is None else {"table_settings": ts}
                try:
                    tables = page.extract_tables(**kwargs) or []
                except Exception as exc:
                    logger.debug("extract_tables settings=%s: %s", ts, exc)
                    continue
                for table in tables:
                    if not table:
                        continue

                    headers = _find_header_row_indices(table)
                    if headers:
                        for hi, hstart in enumerate(headers):
                            row_end = headers[hi + 1] if hi + 1 < len(headers) else len(table)
                            seg, cols = _quotes_from_segment(
                                table,
                                hstart,
                                row_end,
                                only_a_prefix,
                                strict_market_only=strict_market_only,
                            )
                            if seg:
                                seen_blocks += 1
                            for q in seg:
                                merged[q.instrument_code] = q
                            if cols is not None:
                                fallback_cols = cols
                        continue

                    # продолжение без шапки: используем fallback_cols (если есть)
                    if fallback_cols is not None and _table_has_data_like_rows(table, only_a_prefix):
                        cc, chain = fallback_cols
                        seg = _rows_quotes_loop(table, 0, len(table), cc, chain, only_a_prefix)
                        if seg:
                            seen_blocks += 1
                            continuation_blocks += 1
                        for q in seg:
                            merged[q.instrument_code] = q

    final = list(merged.values())
    src = "только «Рыночная»" if strict_market_only else "Рыночная+запасные колонки"
    logger.info(
        "PDF %s: блоков таблиц с данными=%s, продолжений без шапки=%s, "
        "уникальных кодов (котировка)=%s, источник=%s, только_A*=%s",
        path.name,
        seen_blocks,
        continuation_blocks,
        len(final),
        src,
        only_a_prefix,
    )
    if log_extracted_codes and final:
        first_codes = sorted(q.instrument_code for q in final)[:20]
        logger.info("Отладка кодов: первые 20 instrument_code: %s", first_codes)
        if any(q.instrument_code == "DSC5ANK065F" for q in final):
            q = next(x for x in final if x.instrument_code == "DSC5ANK065F")
            logger.info("Отладка DSC5ANK065F найдена. Цена=%s, объем=%s", q.market_price, q.volume_tons)
    return final


def _first_metric_ton_page_index(pages: List[Any]) -> Optional[int]:
    for i, page in enumerate(pages):
        text = (page.extract_text() or "").lower()
        if "метрическая тонна" in text or "метрическая  тонна" in text:
            return i
    return None


def extract_market_prices_from_pdf(
    path: Path,
    *,
    only_a_prefix: bool = False,
    strict_market_only: bool = False,
    log_extracted_codes: bool = False,
) -> List[Tuple[str, float]]:
    quotes = extract_market_quotes_from_pdf(
        path,
        only_a_prefix=only_a_prefix,
        strict_market_only=strict_market_only,
        log_extracted_codes=log_extracted_codes,
    )
    return [(q.instrument_code, q.market_price) for q in quotes]


async def apply_spimex_history(
    *,
    bulletin_path: Path,
    trade_dt: date,
    quotes: List[MarketQuote],
) -> int:
    """
    Записывает историю в таблицу spimex_prices (для аналитики).
    Формат: одна запись на (дата бюллетеня, instrument_code).
    """
    from db.database import AsyncSessionLocal, SpimexPrice, ProductBasisPrice, Product, Basis

    dt = datetime.combine(trade_dt, time.min)
    inserted = 0
    async with AsyncSessionLocal() as session:
        # Чтобы избежать дублей при повторном импорте одного и того же бюллетеня:
        # удаляем все строки по этой дате для тех кодов, которые будем писать.
        codes = [q.instrument_code for q in quotes]
        if codes:
            await session.execute(
                SpimexPrice.__table__.delete().where(
                    SpimexPrice.exchange_product_id.in_(codes),
                    SpimexPrice.date == dt,
                )
            )

        for q in quotes:
            pbp = (
                await session.execute(
                    select(ProductBasisPrice, Product, Basis)
                    .join(Product, Product.id == ProductBasisPrice.product_id)
                    .join(Basis, Basis.id == ProductBasisPrice.basis_id)
                    .where(ProductBasisPrice.instrument_code == q.instrument_code)
                    .limit(1)
                )
            ).first()
            if not pbp:
                continue
            row, pr, bs = pbp
            session.add(
                SpimexPrice(
                    exchange_product_id=q.instrument_code,
                    fuel=pr.name,
                    basis=bs.name,
                    price=float(q.market_price),
                    volume=float(q.volume_tons) if q.volume_tons is not None else None,
                    date=dt,
                )
            )
            inserted += 1
        await session.commit()
    logger.info(
        "✅ История (spimex_prices): %s → записано строк: %s (дата %s)",
        bulletin_path.name,
        inserted,
        trade_dt.isoformat(),
    )
    return inserted


async def apply_prices(pairs: List[Tuple[str, float]]) -> Tuple[int, int, List[str]]:
    from db.database import AsyncSessionLocal
    from db.database import ProductBasisPrice

    updated = 0
    missing_codes: List[str] = []
    async with AsyncSessionLocal() as session:
        for code, price in pairs:
            q = await session.execute(
                select(ProductBasisPrice).where(ProductBasisPrice.instrument_code == code)
            )
            row = q.scalar_one_or_none()
            if not row:
                missing_codes.append(code)
                logger.debug("Код %s нет в product_basis_prices", code)
                continue
            row.current_price = float(price)
            updated += 1
        await session.commit()
    return updated, len(missing_codes), missing_codes


async def report_db_coverage(
    pairs: List[Tuple[str, float]],
) -> Tuple[int, int, int]:
    """Сколько кодов из БД получили цену из PDF (по пересечению)."""
    from db.database import AsyncSessionLocal
    from db.database import ProductBasisPrice

    pdf_codes = {c for c, _ in pairs}
    async with AsyncSessionLocal() as session:
        q = await session.execute(
            select(ProductBasisPrice.instrument_code).where(
                ProductBasisPrice.instrument_code.isnot(None),
                ProductBasisPrice.is_active.is_(True),
            )
        )
        db_codes = {str(x[0]).strip().upper() for x in q.all() if x[0]}

    in_both = len(db_codes & pdf_codes)
    in_db_only = len(db_codes - pdf_codes)
    return len(db_codes), in_both, in_db_only


def _print_import_summary(
    pairs: List[Tuple[str, float]],
    up: int,
    miss: int,
    miss_codes: List[str],
    total_db: int,
    matched: int,
    db_no_price: int,
    strict_market_only: bool,
) -> None:
    print(f"✅ Обновлено цен в БД: {up}")
    price_note = (
        "только колонка «Рыночная»"
        if strict_market_only
        else "«Рыночная» или запасные (средневзвеш., макс., мин., лучш. предл./спрос)"
    )
    print(f"   Кодов из PDF с ценой ({price_note}): {len(pairs)}")
    print(f"   Кодов в БД (активные instrument_code): {total_db}")
    print(f"   Пересечение PDF ∩ БД: {matched} (из каталога есть цена в этом PDF)")
    print(f"   В каталоге без рыночной в этом PDF: {db_no_price} (нормально, если ячейка пустая)")
    if miss:
        print(f"   Кодов из PDF, которых нет в БД: {miss} (пример: {', '.join(miss_codes[:8])})")

    if matched < total_db and db_no_price > 0:
        print(
            "\nℹ️ Оставшиеся коды каталога: в PDF нет числа ни в «Рыночной», ни в запасных колонках "
            "(или строка не распозналась). Тогда — другой бюллетень/API или «последняя дата торгов»."
        )
        if strict_market_only:
            print(
                "   Подсказка: без --strict-market-column подставляются средневзвешенная и др. колонки."
            )


async def main_async(
    pdf_path: Path,
    only_a_prefix: bool,
    strict_market_only: bool,
    log_extracted_codes: bool = False,
) -> None:
    logger.info("Импорт цен из файла: %s", pdf_path.resolve())
    quotes = extract_market_quotes_from_pdf(
        pdf_path,
        only_a_prefix=only_a_prefix,
        strict_market_only=strict_market_only,
        log_extracted_codes=log_extracted_codes,
    )
    pairs = [(q.instrument_code, q.market_price) for q in quotes]
    if not pairs:
        print(
            "⚠️ Не извлечено ни одной пары (код + цена). "
            "Проверьте PDF и секцию «Метрическая тонна»."
        )
        return

    total_db, matched, db_no_price = await report_db_coverage(pairs)
    up, miss, miss_codes = await apply_prices(pairs)
    await apply_spimex_history(
        bulletin_path=pdf_path,
        trade_dt=bulletin_trade_date(pdf_path),
        quotes=quotes,
    )
    _print_import_summary(
        pairs, up, miss, miss_codes, total_db, matched, db_no_price, strict_market_only
    )


async def main_async_last_n(
    directory: Path,
    n: int,
    only_a_prefix: bool,
    strict_market_only: bool,
    log_extracted_codes: bool = False,
) -> None:
    newest_first = pick_latest_n_bulletin_pdfs(directory, n)
    # Сначала старые, потом новые — в БД останутся цены с последнего бюллетеня
    chrono = list(reversed(newest_first))
    logger.info(
        "Импорт последних %s бюллетеней (по дате в имени), порядок от старого к новому: %s",
        len(chrono),
        ", ".join(p.name for p in chrono),
    )
    print(
        f"Файлы ({len(chrono)}): сначала обрабатываются более старые, финальные цены — с последнего в списке."
    )
    for i, pdf_path in enumerate(chrono, start=1):
        print(f"\n--- [{i}/{len(chrono)}] {pdf_path.name} ---")
        quotes = extract_market_quotes_from_pdf(
            pdf_path,
            only_a_prefix=only_a_prefix,
            strict_market_only=strict_market_only,
            log_extracted_codes=log_extracted_codes,
        )
        pairs = [(q.instrument_code, q.market_price) for q in quotes]
        if not pairs:
            print("⚠️ Нет пар (код + цена), пропуск.")
            continue
        total_db, matched, db_no_price = await report_db_coverage(pairs)
        up, miss, miss_codes = await apply_prices(pairs)
        await apply_spimex_history(
            bulletin_path=pdf_path,
            trade_dt=bulletin_trade_date(pdf_path),
            quotes=quotes,
        )
        _print_import_summary(
            pairs, up, miss, miss_codes, total_db, matched, db_no_price, strict_market_only
        )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Импорт цен СПбМТСБ из PDF. Один файл (--pdf) или каталог (--bulletins-dir): по умолчанию один самый новый PDF; с --last-n — несколько последних по дате в имени."
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--pdf", help="Путь к одному PDF бюллетеня")
    src.add_argument(
        "--bulletins-dir",
        metavar="DIR",
        help="Каталог с PDF; по умолчанию — один файл с наибольшей меткой YYYYMMDDHHMMSS в имени",
    )
    p.add_argument(
        "--last-n",
        type=int,
        metavar="N",
        default=0,
        help="Вместе с --bulletins-dir: обработать N самых новых PDF (по дате в имени), от старого к новому; в БД останутся цены с самого свежего",
    )
    p.add_argument(
        "--only-a-prefix",
        action="store_true",
        help="Учитывать только коды, начинающиеся с A (бензины). По умолчанию — все коды формата СПбМТСБ",
    )
    p.add_argument(
        "--no-a-prefix-filter",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--strict-market-column",
        action="store_true",
        help="Брать только колонку «Рыночная» (без средневзвешенной, лучшего предложения и т.д.)",
    )
    p.add_argument(
        "--log-extracted-codes",
        action="store_true",
        help="Логировать первые извлеченные instrument_code (для отладки распознавания кодов).",
    )
    args = p.parse_args()
    if args.last_n and not args.bulletins_dir:
        raise SystemExit("--last-n допустим только вместе с --bulletins-dir")
    if args.pdf and args.last_n:
        raise SystemExit("--last-n нельзя использовать с --pdf")

    only_a = bool(args.only_a_prefix)
    log_codes = bool(getattr(args, "log_extracted_codes", False))

    if args.pdf:
        path = Path(args.pdf).expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f"Файл не найден: {path}")
        asyncio.run(
            main_async(
                path,
                only_a_prefix=only_a,
                strict_market_only=args.strict_market_column,
                log_extracted_codes=log_codes,
            )
        )
        return

    bdir = Path(args.bulletins_dir).expanduser().resolve()
    try:
        if args.last_n and args.last_n > 0:
            asyncio.run(
                main_async_last_n(
                    bdir,
                    args.last_n,
                    only_a_prefix=only_a,
                    strict_market_only=args.strict_market_column,
                    log_extracted_codes=log_codes,
                )
            )
        else:
            path = pick_latest_bulletin_pdf(bdir)
            asyncio.run(
                main_async(
                    path,
                    only_a_prefix=only_a,
                    strict_market_only=args.strict_market_column,
                    log_extracted_codes=log_codes,
                )
            )
    except FileNotFoundError as e:
        raise SystemExit(str(e)) from e


if __name__ == "__main__":
    main()
