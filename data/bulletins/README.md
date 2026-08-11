# PDF/XLS бюллетени СПбМТСБ (нефть)

Сюда попадают файлы вида **`oil_YYYYMMDDHHMMSS.pdf`** (основной формат) и редко `oil_xls_…xls`.

Источник на сайте биржи: [итоги торгов, нефтепродукты](https://spimex.com/markets/oil_products/trades/results/).

## Ручной импорт

```bash
cd /path/to/fuel_bot
python3 import_spimex_prices_from_pdf.py --bulletins-dir "./data/bulletins"
```

## Автоматика (рекомендуется)

VPS часто **не может** открыть `spimex.com` (`Connection refused`).  
Рабочая схема: **GitHub Actions скачивает PDF → SCP на VPS → импорт/рассылка**.

См. подробный сетап: [`docs/spimex_auto_fetch.md`](../../docs/spimex_auto_fetch.md).

Кратко на VPS после деплоя скриптов:

```bash
sudo chmod +x /opt/fuel_bot/scripts/spimex_vps_ingest.sh
sudo mkdir -p /home/ops/spimex_inbox && sudo chown ops:ops /home/ops/spimex_inbox
echo 'ops ALL=(root) NOPASSWD: /opt/fuel_bot/scripts/spimex_vps_ingest.sh' | sudo tee /etc/sudoers.d/fuel-spimex-ingest
sudo chmod 440 /etc/sudoers.d/fuel-spimex-ingest
sudo systemctl disable --now fuel-spimex-daily.timer || true
```
