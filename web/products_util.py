"""Список продуктов для веб (канонические коды как в боте)."""
from __future__ import annotations

import re

from sqlalchemy import func, select

from db.database import Product, ProductBasisPrice
from utils import canonical_fuel_display_name


async def list_products_for_calc(session) -> list[dict[str, object]]:
    """Список для форм: элементы вида {'id': int, 'name': str}."""
    q = await session.execute(
        select(Product).where(Product.is_active.is_(True)).order_by(Product.name)
    )
    products = q.scalars().all()
    if not products:
        return []

    product_ids = [p.id for p in products]
    price_count_map: dict[int, int] = {pid: 0 for pid in product_ids}
    rows = await session.execute(
        select(ProductBasisPrice.product_id, func.count(ProductBasisPrice.id))
        .where(ProductBasisPrice.product_id.in_(product_ids))
        .where(ProductBasisPrice.is_active.is_(True))
        .group_by(ProductBasisPrice.product_id)
    )
    for pid, cnt in rows.all():
        price_count_map[int(pid)] = int(cnt)

    canonical_to_best: dict[str, tuple[int, int]] = {}
    for product in products:
        canonical = canonical_fuel_display_name(product.name)
        pid = product.id
        cnt = price_count_map.get(pid, 0)
        best = canonical_to_best.get(canonical)
        if best is None:
            canonical_to_best[canonical] = (pid, cnt)
        else:
            best_pid, best_cnt = best
            if cnt > best_cnt or (cnt == best_cnt and pid < best_pid):
                canonical_to_best[canonical] = (pid, cnt)

    allowed_re = r"^(АИ-(92|95|100)-К5|ДТ-(А|Е|З|Л)-К5|ТС-1|Мазут топочный М100)$"
    out: list[dict[str, object]] = []
    for canonical, (pid, _cnt) in sorted(canonical_to_best.items(), key=lambda x: x[0]):
        if not re.match(allowed_re, canonical):
            continue
        out.append({"id": int(pid), "name": canonical})
    return out


async def list_products_for_basis(session, *, basis_id: int) -> list[dict[str, object]]:
    """
    Топлива на конкретном базисе (для аналитики «тренд»), в каноническом виде,
    чтобы не показывать пользователю сотни вариантов строк.
    """
    q = await session.execute(
        select(Product)
        .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
        .where(ProductBasisPrice.basis_id == int(basis_id))
        .where(ProductBasisPrice.is_active.is_(True))
        .where(Product.is_active.is_(True))
        .group_by(Product.id)
        .order_by(Product.name)
    )
    products = q.scalars().all()
    if not products:
        return []

    # Считаем, у какого product_id больше активных цен на этом базисе — его и берём как representative
    product_ids = [p.id for p in products]
    price_count_map: dict[int, int] = {pid: 0 for pid in product_ids}
    rows = await session.execute(
        select(ProductBasisPrice.product_id, func.count(ProductBasisPrice.id))
        .where(ProductBasisPrice.product_id.in_(product_ids))
        .where(ProductBasisPrice.basis_id == int(basis_id))
        .where(ProductBasisPrice.is_active.is_(True))
        .group_by(ProductBasisPrice.product_id)
    )
    for pid, cnt in rows.all():
        price_count_map[int(pid)] = int(cnt)

    canonical_to_best: dict[str, tuple[int, int]] = {}
    for product in products:
        canonical = canonical_fuel_display_name(product.name)
        pid = product.id
        cnt = price_count_map.get(pid, 0)
        best = canonical_to_best.get(canonical)
        if best is None:
            canonical_to_best[canonical] = (pid, cnt)
        else:
            best_pid, best_cnt = best
            if cnt > best_cnt or (cnt == best_cnt and pid < best_pid):
                canonical_to_best[canonical] = (pid, cnt)

    allowed_re = r"^(АИ-(92|95|100)-К5|ДТ-(А|Е|З|Л)-К5|ТС-1|Мазут топочный М100)$"
    out: list[dict[str, object]] = []
    for canonical, (pid, _cnt) in sorted(canonical_to_best.items(), key=lambda x: x[0]):
        if not re.match(allowed_re, canonical):
            continue
        out.append({"id": int(pid), "name": canonical})
    return out
