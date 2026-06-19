"""
Лаунчер примера ``web``: конвейер + Web API/UI в одном процессе (combined,
аналог ``angarion run --with-api``), но с **кастомной ручкой и страницей**.

Встроенный CLI-раннер (``serve_combined``) собирает приложение как
``create_app(deps)``. UI-страницы (``pages``) теперь подхватываются
чистым CLI через entry-point-группу ``angarion.pages`` (T029), но
пользовательские JSON-``routers`` передать через CLI по-прежнему нельзя.
Этот пример показывает **и кастомную ручку (router), и страницу** одним
лаунчером, поэтому собирает собственный composition root поверх публичных
кирпичей библиотеки:

1. ``build_app`` — конвейер (ingest + worker + delivery + consumer outbox);
2. ``build_web_deps`` — проекция портов хранилища/очереди в ``AngarionDeps``;
3. ``create_app(deps, routers=[ext_router], pages=[EXT_PAGE])`` — ASGI-
   приложение со встроенными ручками/UI **и** расширением примера
   (``ext.py``); каталог шаблонов страница несёт сама в
   ``Page.template_dirs`` (T036), отдельный ``template_dirs`` лаунчеру
   не нужен;
4. uvicorn рядом с конвейером; стоп по сигналу (uvicorn ловит SIGINT/
   SIGTERM) ИЛИ по команде ``restart_pipeline`` (``app.restart_event``) —
   гасит весь процесс, супервизор поднимает (§3.2).

Каталог скрипта Python кладёт в ``sys.path`` сам, поэтому ``ext``
импортируется напрямую (запускать из каталога примера — это делает
``run.sh``). Пример идёт с ``[api].auth = "none"`` (локальный режим): без
user store и JWT-секрета, все роутеры открыты под синтетическим админом.
"""

import asyncio
import contextlib

import uvicorn
from ext import EXT_PAGE, ext_router

from angarion.adapters.http import build_settings_notifier, build_web_deps, create_app
from angarion.bootstrap import build_app
from angarion.config import load_settings


async def serve() -> None:
    """Поднять конвейер и uvicorn с расширением; держать до сигнала/restart."""
    settings = load_settings('app.toml')
    app = build_app(settings)
    deps = build_web_deps(
        settings, app.storage, app.queue, notifier=build_settings_notifier()
    )
    asgi = create_app(
        deps,
        routers=[ext_router],
        pages=[EXT_PAGE],
        title='angarion web example',
    )
    api = settings.api
    server = uvicorn.Server(
        uvicorn.Config(
            asgi, host=api.host, port=api.port, log_level='info', access_log=False
        )
    )

    await app.start()
    serve_task = asyncio.create_task(server.serve(), name='uvicorn')
    restart_task = asyncio.create_task(app.restart_event.wait(), name='restart')
    try:
        await asyncio.wait(
            {serve_task, restart_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        server.should_exit = True
        await serve_task
        restart_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await restart_task
        await app.stop()


def main() -> None:
    """Запустить combined-процесс примера по ``app.toml``."""
    asyncio.run(serve())


if __name__ == '__main__':
    main()
