# Контракт плагина

Объекты, которые плагин отдаёт в entry points `angarion.adapters` /
`angarion.queues` / `angarion.storages` (§12.11 ТЗ): дескриптор адаптера
`AdapterPlugin`, именованные фабрики бэкендов очереди и хранилища,
комплект портов хранения `StorageBundle`, контракт listener'а.

Практический walkthrough — в [гайде автору плагина](../guides/plugin-authoring.md).

::: angarion.domain.plugin
