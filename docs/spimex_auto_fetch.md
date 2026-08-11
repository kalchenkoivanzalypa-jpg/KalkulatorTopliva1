# Автоскачивание бюллетеней СПбМТСБ (когда VPS не достучится до биржи)

VPS (`82.x`) часто получает `Connection refused` на `spimex.com:443`.
Поэтому **скачивание идёт с GitHub Actions**, файл заливается на сервер, там импорт и рассылка.

## Один раз настроить (Mac)

Прокси **не нужен**. Скачивает GitHub Actions (не VPS и не твой Mac как cron).

```bash
cd ~/Desktop/fuel_bot
bash scripts/setup_spimex_gha_once.sh
```

Скрипт: создаст SSH-ключ, положит публичный на VPS, зальёт `spimex_vps_ingest.sh`, настроит inbox/sudoers, выключит бесполезный VPS-таймер, **напечатает private key** — его вставить в GitHub Secrets.

### Secrets в GitHub (Settings → Secrets and variables → Actions)

| Secret | Значение |
|--------|----------|
| `SPIMEX_VPS_HOST` | `82.22.38.34` |
| `SPIMEX_VPS_USER` | `ops` |
| `SPIMEX_VPS_SSH_KEY` | весь private key из вывода скрипта |
| `SPIMEX_VPS_PORT` | `22` (опционально) |

Потом пуш workflow (если ещё не в `main`) и **Actions → spimex-daily → Run workflow**.

Расписание: будни **13:50 МСК**, ретраи каждые 2 мин до **18:00 МСК**, потом SCP + ingest.

## Проверка ingest на VPS вручную

```bash
# положить pdf в inbox и:
sudo /opt/fuel_bot/scripts/spimex_vps_ingest.sh
```
