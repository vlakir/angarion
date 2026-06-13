"""
Telegram-адаптер на Telethon (MTProto, пользовательские аккаунты) —
первый реальный адаптер платформы (M3, T005, §12.1 ТЗ).

Реализован по фазам (спека ``specs/T005-m3-telegram/spec.md``, Q7):
контракт плагина и шифрование сессии (фаза 1), граница Telethon +
резолвер + mapping + live-listener + буфер (фаза 2), catch-up §9.3
(фаза 3), sender с троттлингом (фаза 4), сборка ``AdapterPlugin``
(``PLUGIN``, entry point ``angarion.adapters:telegram``), общий
``ClientRegistry`` и проводка конфига (фаза 5). CLI и composition root —
в ``angarion.cli`` / ``angarion.bootstrap``. Acceptance и суточный
прогон — фаза 6. Зависимости — extra ``angarion[telegram]``.
"""
