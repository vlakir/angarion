"""
Внутренний транспорт ``internal`` (T037): прямой провод цепочек пайплайнов.

Sink-only синтетический адаптер: его sink вместо доставки наружу преобразует
``OutboundRecord`` обратно в ``Record(kind=new)`` и подаёт его в
``IngestService`` (re-ingestion). Совпадение ``(transport=internal, address)``
у ``target`` одного пайплайна и ``source`` другого — ребро цепочки. Listener'а
нет (``make_listener=None``): «вход» приёмника наполняет сам sink.
"""
