# Автоскачивание бюллетеней СПбМТСБ

`spimex.com` **блокирует** VPS и GitHub Actions (`Connection refused`).  
С **твоего Mac** сайт открывается — автоматика идёт через Mac.

## Один раз

VPS (inbox + sudoers + ingest) уже настроены на прошлых шагах.

На Mac:

```bash
cd ~/Desktop/fuel_bot
bash scripts/setup_spimex_mac_launchd.sh
```

Тест прямо сейчас:

```bash
bash scripts/spimex_mac_daily.sh --once --trade-date 2026-08-10
```

Дальше: будни с **13:50 МСК** Mac сам скачает PDF → зальёт на VPS → импорт/рестарт/рассылка.  
Mac должен быть **включён** (не sleep наглухо) в это время.

Лог: `logs/spimex_mac_daily.log`

## GitHub Actions

Оставлен только ручной `workflow_dispatch` (для отладки). По расписанию не гоняем — до биржи с runners нет доступа.
