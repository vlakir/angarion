# Автору плагина

angarion расширяется через **entry points** — той же механикой, что
регистрирует встроенные адаптеры и процессоры (§12.11 ТЗ, FR-14:
InMemory-плагин — «нулевой пациент» этого механизма). Свой пакет ставит
зависимость на `angarion`, объявляет entry points — и ядро подхватывает
его без изменений.

Что можно добавить:

| Группа entry point | Что регистрирует | Контракт |
|---|---|---|
| `angarion.processors` | процессор пайплайна | [`ProcessorPort`](../reference/ports.md) |
| `angarion.adapters` | driving-адаптер платформы | [`AdapterPlugin`](../reference/plugin.md) |
| `angarion.queues` | бэкенд очереди | [`QueueBackend`](../reference/plugin.md) |
| `angarion.storages` | бэкенд хранилища | [`StorageBackend`](../reference/plugin.md) |

Расширение Web (свои ручки/страницы) — отдельный механизм через DI поверх
портов, см. [Web API и UI](web-api.md).

## Свой процессор

Процессор — async-функция `(event, ctx, svc) -> ProcessingResult`
(§10.1). Он получает [`InboundEvent`](../reference/models.md),
контекст пайплайна (цели доставки) и сервисы (идемпотентные ключи,
персистентное состояние). Обязан переживать `event.text is None`
(удаление, не восстановленное реестром).

```python
# myplugin/processor.py
from angarion.application.processors import processor
from angarion.domain.models import (
    InboundEvent,
    OutboundMessage,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Verdict,
)


@processor("shout")
async def shout(
    event: InboundEvent,
    ctx: PipelineContextData,
    svc: ProcessorServices,
) -> ProcessingResult:
    """Ретранслирует текст в ВЕРХНЕМ РЕГИСТРЕ; без текста — DROP."""
    if event.text is None:
        return ProcessingResult(verdict=Verdict.DROP, note="shout: нет текста")
    outbound = [
        OutboundMessage(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=event.text.upper(),
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


# Значение entry point — объект ProcessorPort:
from angarion.application.processors import get_processor

SHOUT = get_processor("shout")
```

!!! tip "Идемпотентность выхода"
    Всегда стройте `idempotency_key` через `svc.make_idempotency_key(...)`.
    Под at-least-once процессор может вызваться повторно (рестарт,
    повторный catch-up) — одинаковый ключ гасит дублирующую доставку.

### Конфигурация процессора

Если процессору нужны параметры из `[pipelines.*.processor_config]`,
реализуйте объект `ProcessorPort` с методом `config_model()`, возвращающим
pydantic-схему. Конфиг валидируется fail-fast на старте (невалидный →
`ConfigError` при запуске, а не на первом событии). См. встроенный
`template` как образец.

### Регистрация

В `pyproject.toml` своего пакета:

```toml
[project.entry-points."angarion.processors"]
shout = "myplugin.processor:SHOUT"
```

После установки пакета процессор доступен по имени:

```toml
[pipelines.loud]
processor = "shout"
```

`load_processors` подхватит entry point при старте. Если процессор тянет
optional-зависимость, отсутствие пакета даёт `skip + warning` (как у
встроенного `llm`), а не падение.

## Свой адаптер / бэкенд

Адаптер платформы отдаёт в entry point `angarion.adapters` объект
[`AdapterPlugin`](../reference/plugin.md): имя, capabilities, pydantic-
схему секции `[accounts.*]`, фабрики listener'а и sender'а. Бэкенды
очереди и хранилища — `QueueBackend` / `StorageBackend` (именованные
фабрики `EventQueuePort` / `StorageBundle`).

Listener формализован протоколом [`Listener`](../reference/plugin.md)
(`start` / `stop` / `catchup`): получает `IngestService` через deps
фабрики и эмитит `InboundEvent` ключами из публичных хелперов (§7.2).

## Сертификация контрактными тестами

Реализовали порт — **докажите это** публичными контрактными наборами
[`angarion.testing`](../reference/testing.md) (§12.11). Это не «тесты на
всякий случай», а исполняемая спецификация порта: ваша реализация обязана
пройти их зелёными.

```bash
uv add --dev "angarion[testing]"
```

Параметризуйте контрактный класс своей реализацией через fixture-override:

```python
# tests/test_my_queue_contract.py
import pytest
from angarion.testing import EventQueueContract
from myplugin.queue import MyQueue


class TestMyQueue(EventQueueContract):
    @pytest.fixture
    def queue(self):
        return MyQueue(...)
```

Доступные контракты: `EventQueueContract`, `MessageSinkContract`,
`DedupStoreContract`, `OutboxContract`, `MessageRegistryContract`,
`CursorStoreContract`, `SessionStoreContract`, `StateStoreContract`,
`AnalyticsContract`, `DeadLetterContract`, `RuntimeConfigContract`,
`CommandOutboxContract` — по одному на порт. Фабрики тестовых данных
(`make_event`, `make_envelope`, …) — там же.

Зелёный прогон контрактов = ваш адаптер совместим с ядром и со всеми
гарантиями (at-least-once, идемпотентность, catch-up) без правок ядра.
