# Пример: зеркало между Matrix-комнатами

Минимальный конвейер angarion на **Matrix**: новые сообщения (текст и
вложения) из одной комнаты ретранслируются в другую процессором
`passthrough`. Зашифрованные (E2EE) комнаты-источники поддержаны —
listener расшифровывает входящее.

Это аналог [`examples/forward`](../forward/) (Telegram), но на втором
боевом адаптере — демонстрирует тезис «новый мессенджер = новый адаптер
без изменения ядра».

## Что нужно

1. **Аккаунт на Matrix-homeserver'е** (свой Synapse/Conduit или публичный
   matrix.org). Впиши в [`app.toml`](app.toml):
   - `homeserver` — URL (например `https://matrix.org`);
   - `user_id` — полный MXID (`@bot:matrix.org`);
   - в `[pipelines.mirror]` — `chat_id` источника и цели (`!room:server`
     или `#alias:server`). Аккаунт должен состоять в обеих комнатах.
2. **Extra с E2EE:** `uv sync --extra matrix` — тянет `matrix-nio[e2e]`
   (→ `python-olm` → системная **libolm**, см. README репозитория, раздел
   «Установка»: `apt install libolm-dev` и аналоги).
3. **Пароль аккаунта** в окружении (секрет, не в репозиторий):

   ```bash
   export ANGARION_MATRIX_PASSWORD='…'
   ```

## Запуск

```bash
examples/matrix/run.sh
```

Скрипт сгенерит ключ шифрования сессии (`angarion-data/session.key`,
храни отдельно от `app.db`), применит миграции, при первом запуске
выполнит парольный `angarion login` (пароль → `access_token` + `device_id`
в зашифрованную сессию `app.db`), затем поднимет конвейер. Пиши в
комнату-источник — сообщения прилетят в цель.

Рантайм (`app.db`, ключ сессии, **E2EE key-store** `matrix-e2e/`) — в
git-ignored `angarion-data/`. Секреты в репозиторий не попадают.

## Заметки

- **E2EE key-store** — отдельный sqlite на ФС (`[matrix].store_dir`); токен
  и `device_id` живут в `app.db`. Бэкап обоих — см. README репозитория,
  раздел «Бэкапы».
- **Историческая E2EE-история** комнаты **до** первого входа этого
  устройства недоступна (UTD) — фундаментальное свойство Matrix E2EE, не
  баг адаптера: такие события пропускаются с пометкой `matrix_undecryptable`
  в аналитике (см. «Ограничения платформ» в README репозитория).
- **Кросс-платформа** (Telegram↔Matrix) — добавь второй аккаунт
  `[accounts.*]` другого `messenger` и укажи его в `sources`/`targets`
  пайплайна; ядро не меняется.
