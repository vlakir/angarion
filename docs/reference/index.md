# Справочник API

Автодокументация публичных модулей `angarion` (генерируется из docstring'ов
исходников через [mkdocstrings](https://mkdocstrings.github.io/)). Стабильность
API гарантируется с версии 1.0 (§12.11 ТЗ).

Что входит в публичный контракт:

| Раздел | Модуль | Кому нужен |
|---|---|---|
| [Доменные модели](models.md) | `angarion.domain.models` | всем — это форма события `InboundEvent` и результат `ProcessingResult` |
| [Порты](ports.md) | `angarion.domain.ports` | авторам адаптеров/хранилищ/процессоров |
| [Контракт плагина](plugin.md) | `angarion.domain.plugin` | авторам адаптеров, очередей, хранилищ |
| [Конфигурация](config.md) | `angarion.config` | при кастомной сборке/валидации настроек |
| [Web-адаптер](web.md) | `angarion.adapters.http` | при своих ручках и страницах |
| [Контрактные тесты](testing.md) | `angarion.testing` | для сертификации своей реализации порта |
