# Web-адаптер

FastAPI как второй driving-адаптер (§12.5/§12.6 ТЗ). Публичный API:
фабрика `create_app`, контейнер портов `AngarionDeps`, дескриптор
страницы `Page`, сборка контейнера из composition root
`build_web_deps` / `build_settings_notifier`.

Пакет живёт в extra `angarion[web]` — ядро остаётся fastapi-free (§14.9).
Практика — в [гайде Web API и UI](../guides/web-api.md).

## Фабрика и контейнер

::: angarion.adapters.http

## Типизированные DI-зависимости

Провайдеры портов поверх `AngarionDeps` для пользовательских ручек:
аннотируйте параметр ручки `AnalyticsDep`, `RegistryDep` и т. п. —
FastAPI подставит нужный порт из `app.state`.

::: angarion.adapters.http.deps
