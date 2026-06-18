# Matrix-стенд для интеграционного контура (M7 B4, T010)

Интеграционный тест `tests/integration/test_matrix_access.py` (маркер
`integration`, default-skip) гоняет весь Matrix-конвейер
(приём → пайплайн → отправка) против **локального homeserver'а**:
new/edited/deleted сквозь пайплайн, доставка из E2EE-комнаты, транзит медиа.

Тест homeserver-агностичен — берёт адрес и реквизиты из env/`.secrets`:

| Переменная | Пример | Назначение |
|---|---|---|
| `MATRIX_HOMESERVER` | `http://localhost:8008` | базовый URL homeserver'а |
| `MATRIX_USER` | `@bot:localhost` | MXID тестового аккаунта |
| `MATRIX_PASSWORD` | `botpass123` | пароль аккаунта |

Если переменных нет или homeserver недоступен — тесты пропускаются.

### Конфиг homeserver'а (регистрация + rate-limits)

После генерации конфига в `data/homeserver.yaml` дописать (регистрация для
создания тестового аккаунта + **поднятые rate-limits** — иначе серия
логинов/комнат/сообщений контура ловит `429`, и `nio` уходит в
многосекундный сон, контур виснет):

```yaml
enable_registration: true
enable_registration_without_verification: true
registration_shared_secret: "angarion-it-secret"
rc_message: { per_second: 1000, burst_count: 1000 }
rc_registration: { per_second: 1000, burst_count: 1000 }
rc_login:
  address: { per_second: 1000, burst_count: 1000 }
  account: { per_second: 1000, burst_count: 1000 }       # ← без него 6 логинов контура → 429
  failed_attempts: { per_second: 1000, burst_count: 1000 }
rc_joins:
  local: { per_second: 1000, burst_count: 1000 }
  remote: { per_second: 1000, burst_count: 1000 }
rc_invites:
  per_room: { per_second: 1000, burst_count: 1000 }
  per_user: { per_second: 1000, burst_count: 1000 }
rc_admin_redaction: { per_second: 1000, burst_count: 1000 }
```

## Вариант A — Docker (рекомендуется)

`docker-compose.yml` рядом поднимает Synapse (полный E2EE). Шаги — в
шапке compose-файла: `run --rm homeserver generate` → дописать в
`data/homeserver.yaml` блок выше → `up -d` → `register_new_matrix_user`.
Затем:

```bash
MATRIX_HOMESERVER=http://localhost:8008 MATRIX_USER=@bot:localhost \
  MATRIX_PASSWORD=botpass123 uv run pytest -m integration \
  tests/integration/test_matrix_access.py
```

Том данных (`data/`) git-ignored. Образ тянется с `ghcr.io` (Docker Hub
в ряде сетей недоступен).

## Вариант B — Synapse из PyPI (без Docker)

Если Docker-реестры недоступны, тот же homeserver ставится из PyPI
(этим путём контур и валидировался):

```bash
uv venv --python 3.12 /tmp/synapse-stand
uv pip install --python /tmp/synapse-stand/bin/python matrix-synapse
cd /tmp/synapse-stand && bin/python -m synapse.app.homeserver \
  --server-name localhost --config-path data/homeserver.yaml \
  --generate-config --report-stats=no --data-directory data
# в data/homeserver.yaml дописать блок «регистрация + rate-limits» (см. выше)
bin/python -m synapse.app.homeserver --config-path data/homeserver.yaml &
bin/register_new_matrix_user -u bot -p botpass123 --admin \
  -k angarion-it-secret http://localhost:8008
```

Затем — тот же `pytest -m integration` (см. выше).

## Что проверяет контур

- **new/edited/deleted** — драйвер (второе устройство аккаунта) публикует/
  правит (`m.replace`)/удаляет (redaction) в комнате-источнике, listener
  принимает через live-sync, пайплайн зеркалит в цель.
- **E2EE** — источник зашифрован; listener расшифровывает входящее и
  доставляет в (незашифрованную) цель.
- **media** — вложение из источника транзитом (`passthrough`) доезжает в цель.

Самоочистка не нужна: стенд эфемерный, между прогонами пересоздаётся
(`docker compose down -v` / новый каталог Synapse).
