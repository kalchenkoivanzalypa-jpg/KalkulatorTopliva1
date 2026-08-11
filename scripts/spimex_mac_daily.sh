#!/bin/bash
# Mac: скачать бюллетень СПбМТСБ → SCP на VPS → ingest (импорт/рестарт/рассылка).
# GitHub Actions и VPS до spimex.com не достучатся; Mac — да.
#
# Ручной тест:
#   bash scripts/spimex_mac_daily.sh --once --trade-date 2026-08-10
# Обычный день (ретраи до 18:00 МСК):
#   bash scripts/spimex_mac_daily.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${SPIMEX_VPS_HOST:-82.22.38.34}"
USER="${SPIMEX_VPS_USER:-ops}"
KEY="${SPIMEX_SSH_KEY:-$HOME/.ssh/fuel_spimex_gha}"
OUT="${SPIMEX_OUT_DIR:-$ROOT/_spimex_mac_bulletin}"
LOG_DIR="${SPIMEX_LOG_DIR:-$ROOT/logs}"
mkdir -p "$OUT" "$LOG_DIR"
LOG="$LOG_DIR/spimex_mac_daily.log"

exec >>"$LOG" 2>&1
echo "==== $(date '+%Y-%m-%d %H:%M:%S %z') start ===="

if [[ ! -f "$KEY" ]]; then
  echo "ERROR: нет SSH-ключа $KEY"
  exit 1
fi

SSH=(ssh -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20)
SCP=(scp -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20)

# До 13:50 МСК в будни ждём (ручной --once не ждёт старта сессии)
if [[ " $* " != *" --once "* ]] && [[ "${SPIMEX_SKIP_WAIT:-0}" != "1" ]]; then
  python3 - <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
msk = ZoneInfo("Europe/Moscow")
now = datetime.now(msk)
if now.weekday() >= 5:
    raise SystemExit(0)
start = now.replace(hour=13, minute=50, second=0, microsecond=0)
if now < start:
    time.sleep((start - now).total_seconds())
PY
fi

set +e
python3 "$ROOT/scripts/fetch_spimex_bulletin_only.py" --out-dir "$OUT" "$@"
rc=$?
set -e
if [[ "$rc" -ne 0 ]]; then
  echo "fetch exit $rc — ingest не запускаем"
  exit "$rc"
fi

shopt -s nullglob
files=("$OUT"/oil_*.pdf "$OUT"/oil_xls_*.xls "$OUT"/oil_xls_*.xlsx)
if ((${#files[@]} == 0)); then
  echo "нет файлов в $OUT"
  exit 0
fi
# самый свежий
latest="$(ls -1t "${files[@]}" | head -1)"
echo "upload $latest → ${USER}@${HOST}:/home/ops/spimex_inbox/"
"${SCP[@]}" "$latest" "${USER}@${HOST}:/home/ops/spimex_inbox/"
"${SSH[@]}" "${USER}@${HOST}" 'sudo -n /opt/fuel_bot/scripts/spimex_vps_ingest.sh'
echo "==== done ===="
