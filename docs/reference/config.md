# Конфигурация

Pydantic-модель настроек `AngarionSettings` — типизированное
представление TOML-конфига (`[accounts.*]` / `[storage]` / `[queue]` /
`[pipelines.*]` / `[api]` / `[worker]`). Валидируется fail-fast на старте.

::: angarion.config
