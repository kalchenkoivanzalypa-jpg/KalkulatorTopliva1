#!/bin/bash
# Приём бюллетеня из inbox (заливка с GitHub Actions) → data/bulletins → импорт/рестарт/рассылка.
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
for f in "${files[@]}"; do
  base="$(basename "$f")"
  mv -f "$f" "$DEST/$base"
  echo "  moved $base"
done

chown -R fuel:fuel "$DEST"
chown fuel:fuel "$ROOT/fuel_bot.db" "$ROOT/fuel_bot.db-wal" "$ROOT/fuel_bot.db-shm" 2>/dev/null || true

cd "$ROOT"
# Файл уже локально — пайплайн не ходит на биржу
sudo -u fuel -H "$PY" "$PIPELINE" --once --force
echo "spimex_vps_ingest: OK"
