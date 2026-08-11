#!/bin/bash
# Приём бюллетеня из inbox (заливка с Mac / GHA) → data/bulletins → импорт/рестарт/рассылка.
# Запуск: sudo /opt/fuel_bot/scripts/spimex_vps_ingest.sh
set -euo pipefail

INBOX="${SPIMEX_INBOX:-/home/ops/spimex_inbox}"
ROOT="${FUEL_BOT_ROOT:-/opt/fuel_bot}"
DEST="${ROOT}/data/bulletins"
PY="${ROOT}/venv/bin/python3"
PIPELINE="${ROOT}/scripts/run_spimex_daily_pipeline.py"

mkdir -p "$INBOX" "$DEST"
shopt -s nullglob
files=("$INBOX"/oil_*.pdf "$INBOX"/oil_xls_*.xls "$INBOX"/oil_xls_*.xlsx "$INBOX"/oil_*.xls)

if ((${#files[@]} == 0)); then
  echo "spimex_vps_ingest: inbox пуст ($INBOX)"
  exit 0
fi

echo "spimex_vps_ingest: файлы → $DEST"
moved=()
for f in "${files[@]}"; do
  base="$(basename "$f")"
  mv -f "$f" "$DEST/$base"
  echo "  moved $base"
  moved+=("$DEST/$base")
done

chown -R fuel:fuel "$DEST"
chown fuel:fuel "$ROOT/fuel_bot.db" "$ROOT/fuel_bot.db-wal" "$ROOT/fuel_bot.db-shm" 2>/dev/null || true

# Дата торгов из имени файла (не «сегодня» — иначе VPS полезет на биржу зря)
latest="$(ls -1t "${moved[@]}" | head -1)"
stamp="$(basename "$latest" | grep -oE '[0-9]{14}' | head -1 || true)"
if [[ -z "$stamp" || ${#stamp} -ne 14 ]]; then
  echo "spimex_vps_ingest: не разобрал дату из $latest"
  exit 1
fi
trade_date="${stamp:0:4}-${stamp:4:2}-${stamp:6:2}"
echo "spimex_vps_ingest: trade_date=$trade_date file=$(basename "$latest")"

cd "$ROOT"
# Файл уже локально — без сети; --trade-date обязателен
sudo -u fuel -H "$PY" "$PIPELINE" --once --force --allow-weekend --trade-date "$trade_date"
echo "spimex_vps_ingest: OK"
