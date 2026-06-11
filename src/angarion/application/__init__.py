"""
Application-слой angarion (§6, §10 ТЗ): ingest → очередь → worker →
доставка, маршрутизация и реестр процессоров.

Слой зависит только от домена; конкретные адаптеры приходят портами
через composition root (правило зависимостей §3.1).
"""

from __future__ import annotations
