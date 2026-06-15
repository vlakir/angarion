# Деплой

Гайд по боевому развёртыванию: роли процессов, автоперезапуск через
systemd, reverse proxy с TLS, бэкапы и обращение с секретами (§17.6/§17.9 ТЗ).

## Роли процессов

`angarion run` запускается в одной из ролей (§12.9):

| Роль | Флаг | Что внутри |
|---|---|---|
| pipeline | `--role pipeline` (по умолчанию) | ingest + worker + адаптер + consumer командного outbox; **без** Web |
| api | `--role api` | web-процесс: API + UI + producer командного outbox; **без** конвейера |
| combined | `--role combined` или `--with-api` | конвейер + uvicorn в одном процессе |

Два режима развёртывания:

- **Встроенный (combined).** Один процесс — проще всего. Минус: команда
  `restart_pipeline` из админки гасит **весь** процесс (вместе с API) —
  by design (graceful shutdown), поднимает [супервизор](#systemd). UI
  предупреждает об этом.
- **Раздельный (pipeline + api).** Два процесса, два systemd-юнита.
  `restart_pipeline` перезапускает только конвейер (через командный
  outbox), API остаётся живым. Рекомендуется для прод-нагрузки.

!!! note "Два писателя в `app.db`"
    В раздельном режиме api-процесс пишет users/settings/audit, а
    pipeline — analytics/registry/статусы outbox. SQLite допускает одного
    писателя; развязка — `PRAGMA busy_timeout` поверх WAL (ADR §3.1 в
    [DECISIONS.md](https://github.com/vlakir/angarion/blob/main/DECISIONS.md)).
    Записи api редкие (админ-действия), конкуренция минимальна.

## systemd

`restart` и graceful shutdown рассчитаны на супервизор, который поднимает
процесс заново. Юнит для combined-режима:

```ini
# /etc/systemd/system/angarion.service
[Unit]
Description=angarion pipeline
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=angarion
WorkingDirectory=/opt/angarion
Environment=ANGARION_SESSION_KEY=/run/secrets/angarion_session_key
EnvironmentFile=/etc/angarion/angarion.env
ExecStart=/opt/angarion/.venv/bin/angarion run --config /etc/angarion/app.toml --with-api
Restart=always
RestartSec=5
# graceful shutdown: стоп приёма → дообработка → курсоры → закрытие БД
TimeoutStopSec=60
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

Для раздельного режима — два юнита (`angarion-pipeline.service` с
`--role pipeline` и `angarion-api.service` с `--role api`), указывающие
на один `app.toml`.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now angarion.service
journalctl -u angarion -f          # логи (structlog, JSON)
```

## Reverse proxy + TLS

Web-процесс (uvicorn) слушает `[api].host`/`[api].port` — биндите его на
loopback и публикуйте через reverse proxy с TLS:

```nginx
server {
    listen 443 ssl;
    server_name angarion.example.com;
    ssl_certificate     /etc/letsencrypt/live/angarion.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/angarion.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

!!! danger "auth и bind"
    При `[api].auth = "none"` все роутеры открыты, а `CurrentUser` /
    `AdminUser` резолвятся в синтетического локального админа — это режим
    для dev/localhost. При bind не на `127.0.0.1` приложение пишет громкое
    предупреждение в лог при старте. Для публичного доступа используйте
    `auth = "users"` (требует `[api].secret` и user store) — см.
    [Web API и UI → аутентификация](web-api.md#аутентификация).

## Бэкапы

Гайд §17.6 ТЗ с поправкой на хранение сессий в БД (ADR 2026-06-13).
Бэкапить:

- **`app.db`** — горячая копия через SQLite backup API / `VACUUM INTO`,
  **не** файловым копированием под нагрузкой (для непрерывной репликации —
  [Litestream](https://litestream.io/)). Содержит реестр сообщений,
  курсоры, аналитику, командный outbox, пользователей/настройки **и
  зашифрованные сессии аккаунтов** (`StringSession`).
- **`ANGARION_SESSION_KEY`** — Fernet-ключ шифрования сессий. Хранить и
  бэкапить **отдельно** от `app.db`: ключ рядом с БД обесценивает
  шифрование, потеря ключа = бэкап сессий бесполезен (потребуется
  повторный `angarion login` для каждого аккаунта).
- **TOML-конфиг** — параметры пайплайнов и аккаунтов (без секретов).
- **`queue.db`** — по выбору: потеря очереди при живом catch-up
  восстановима для сообщений в окне реестра (`registry_window_days`).

Боевой адаптер не держит файловых session-файлов — сессия живёт в
`app.db`. `sessions/*.session` остаётся только у интеграционного контура
тестов и в прод-бэкап не входит.

## Секреты — только через env

`api_id` / `api_hash` / `ANGARION_SESSION_KEY`, JWT-секрет `[api].secret`,
ключи LLM (по имени `api_key_env`) подаются переменными окружения,
**никогда** не пишутся в TOML (§17.7). structlog маскирует секреты в логах
(цепочка `mask_secrets`). Доступ к `EnvironmentFile` и файлу ключа — права
`0600`, владелец — сервисный пользователь.

## Миграции

```bash
uv run angarion migrate --config app.toml
```

`auto_migrate=false` по умолчанию — миграции применяются явно, отдельным
шагом деплоя (до старта сервиса). Ревизии Alembic линейны; обновление
версии = `migrate` перед перезапуском юнита.
