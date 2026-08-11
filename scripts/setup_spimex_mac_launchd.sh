#!/bin/bash
# Ставит launchd: будни ~13:50 МСК Mac сам качает бюллетень и шлёт на VPS.
# Запуск: bash scripts/setup_spimex_mac_launchd.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.fuelbot.spimex-daily"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
SCRIPT="$ROOT/scripts/spimex_mac_daily.sh"
KEY="${HOME}/.ssh/fuel_spimex_gha"

chmod +x "$SCRIPT" "$ROOT/scripts/fetch_spimex_bulletin_only.py"

if [[ ! -f "$KEY" ]]; then
  echo "Сначала нужен ключ $KEY (уже создавался setup_spimex_gha_once.sh)."
  exit 1
fi

# Локальное время старта = 13:45 МСК (скрипт дождёт до 13:50)
read -r HOUR MINUTE < <(python3 - <<'PY'
from datetime import datetime
from zoneinfo import ZoneInfo
msk = datetime.now(ZoneInfo("Europe/Moscow")).replace(hour=13, minute=45, second=0, microsecond=0)
local = msk.astimezone()
print(local.hour, local.minute)
PY
)

mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/logs"

# Weekday 1=Mon … 5=Fri в launchd
cat >"$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${SCRIPT}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>StartCalendarInterval</key>
  <array>
    <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>${HOUR}</integer><key>Minute</key><integer>${MINUTE}</integer></dict>
    <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>${HOUR}</integer><key>Minute</key><integer>${MINUTE}</integer></dict>
    <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>${HOUR}</integer><key>Minute</key><integer>${MINUTE}</integer></dict>
    <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>${HOUR}</integer><key>Minute</key><integer>${MINUTE}</integer></dict>
    <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>${HOUR}</integer><key>Minute</key><integer>${MINUTE}</integer></dict>
  </array>
  <key>StandardOutPath</key>
  <string>${ROOT}/logs/spimex_launchd.out</string>
  <key>StandardErrorPath</key>
  <string>${ROOT}/logs/spimex_launchd.err</string>
  <key>RunAtLoad</key>
  <false/>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/${LABEL}" 2>/dev/null || true

echo "OK: launchd ${LABEL}"
echo "Старт будни в ${HOUR}:$(printf '%02d' "$MINUTE") (локальное = 13:45 МСК)"
echo "Лог: $ROOT/logs/spimex_mac_daily.log"
echo ""
echo "Проверка сейчас:"
echo "  bash scripts/spimex_mac_daily.sh --once --trade-date 2026-08-10"
