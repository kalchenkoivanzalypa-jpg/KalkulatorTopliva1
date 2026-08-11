#!/bin/bash
# Одноразовая настройка: ключ SSH + подсказка secrets + заливка ingest на VPS.
# Запуск на Mac: bash scripts/setup_spimex_gha_once.sh
set -euo pipefail

HOST="${SPIMEX_VPS_HOST:-82.22.38.34}"
USER="${SPIMEX_VPS_USER:-ops}"
KEY="${HOME}/.ssh/fuel_spimex_gha"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ ! -f "$KEY" ]]; then
  ssh-keygen -t ed25519 -f "$KEY" -N "" -C "gha-spimex"
  echo "Ключ создан: $KEY"
else
  echo "Ключ уже есть: $KEY"
fi

echo ""
echo "=== 1) Публичный ключ на VPS (введи пароль ops если спросит) ==="
ssh-copy-id -i "${KEY}.pub" "${USER}@${HOST}"

echo ""
echo "=== 2) Inbox + sudoers + скрипты пайплайна на VPS ==="
scp -i "$KEY" \
  "$ROOT/scripts/spimex_vps_ingest.sh" \
  "$ROOT/scripts/run_spimex_daily_pipeline.py" \
  "$ROOT/utils/spimex_bulletin_fetch.py" \
  "$ROOT/price_checker.py" \
  "${USER}@${HOST}:/home/ops/"
ssh -i "$KEY" "${USER}@${HOST}" bash -s <<'REMOTE'
set -euo pipefail
sudo mkdir -p /opt/fuel_bot/scripts /opt/fuel_bot/utils /home/ops/spimex_inbox
sudo mv -f /home/ops/spimex_vps_ingest.sh /opt/fuel_bot/scripts/spimex_vps_ingest.sh
sudo mv -f /home/ops/run_spimex_daily_pipeline.py /opt/fuel_bot/scripts/run_spimex_daily_pipeline.py
sudo mv -f /home/ops/spimex_bulletin_fetch.py /opt/fuel_bot/utils/spimex_bulletin_fetch.py
sudo mv -f /home/ops/price_checker.py /opt/fuel_bot/price_checker.py
sudo chmod +x /opt/fuel_bot/scripts/spimex_vps_ingest.sh
sudo chown -R fuel:fuel /opt/fuel_bot/scripts/spimex_vps_ingest.sh \
  /opt/fuel_bot/scripts/run_spimex_daily_pipeline.py \
  /opt/fuel_bot/utils/spimex_bulletin_fetch.py \
  /opt/fuel_bot/price_checker.py 2>/dev/null || true
sudo chown ops:ops /home/ops/spimex_inbox
echo 'ops ALL=(root) NOPASSWD: /opt/fuel_bot/scripts/spimex_vps_ingest.sh' | sudo tee /etc/sudoers.d/fuel-spimex-ingest >/dev/null
sudo chmod 440 /etc/sudoers.d/fuel-spimex-ingest
sudo systemctl disable --now fuel-spimex-daily.timer 2>/dev/null || true
echo "VPS OK"
REMOTE

echo ""
echo "=== 3) Добавь GitHub Secrets (Settings → Secrets and variables → Actions) ==="
echo "SPIMEX_VPS_HOST = ${HOST}"
echo "SPIMEX_VPS_USER = ${USER}"
echo "SPIMEX_VPS_PORT = 22"
echo "SPIMEX_VPS_SSH_KEY = (весь private key ниже)"
echo "-----BEGIN-----"
cat "$KEY"
echo "-----END-----"
echo ""
echo "Потом: git push + Actions → spimex-daily → Run workflow"
echo "Готово. Дальше будни 13:50 МСК сами."
