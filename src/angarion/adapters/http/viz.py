"""
Сборка и раскладка трёхдольного графа топологии для ``/ui/pipelines``
(§12.6, T025 фаза 1): «источники → пайплайны → получатели».

Граф собирается **только из портов и конфига** (топология —
``settings.pipelines``; статус — ``runtime_config`` + ``analytics``;
аннотации — ``analytics`` + ``queue``); ORM/Telethon сюда не протекают
(§12.5/§12.6). Раскладка (координаты прямоугольников и рёбер)
вычисляется здесь, а SVG рисуется Jinja-шаблоном из готовой модели —
графический слой остаётся серверным, без JS-библиотек.

Без ``from __future__ import annotations`` не обойтись для строковых
аннотаций ``AngarionDeps`` в сигнатуре — модели ниже самодостаточны.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, Literal

from pydantic import BaseModel, ConfigDict

from angarion.domain.keys import make_source_key

if TYPE_CHECKING:
    from angarion.adapters.http.deps import AngarionDeps
    from angarion.config import AngarionSettings, EndpointConfig

NodeStatus = Literal['active', 'paused', 'failed', 'endpoint']

_FAIL_WINDOW: Final = timedelta(hours=1)
"""Окно «свежести» сбоя для окраски узла (§12.6: failed за час)."""

_DELIVERED_WINDOW: Final = timedelta(hours=24)
"""Окно подсчёта доставленных для аннотации (как на дашборде)."""

_FAIL_KINDS: Final = ('failed', 'delivery_failed')
"""Виды событий, означающие сбой пайплайна (worker + доставка)."""

# --- геометрия раскладки (единицы — пользовательские координаты SVG) ---
_MARGIN: Final = 20
_NODE_W: Final = 190
_NODE_H: Final = 48
_V_GAP: Final = 18
_COL_GAP: Final = 90

_STATUS_FILL: Final[dict[NodeStatus, str]] = {
    'active': '#2e7d32',
    'paused': '#b58900',
    'failed': '#c62828',
    'endpoint': '#37474f',
}


class GraphNode(BaseModel):
    """Узел графа: прямоугольник с подписью, статусом и аннотацией."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    key: str
    label: str
    status: NodeStatus
    fill: str
    annotation: str
    x: int
    y: int
    width: int
    height: int


class GraphEdge(BaseModel):
    """Ребро графа: прямая от правого края левого узла к левому краю правого."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    x1: int
    y1: int
    x2: int
    y2: int


class PipelineGraph(BaseModel):
    """Готовая к рендеру модель графа (координаты в пользовательских ед.)."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    width: int
    height: int
    sources: tuple[GraphNode, ...]
    pipelines: tuple[GraphNode, ...]
    targets: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    depth_pending: int
    depth_unacked: int


def _endpoint_key(settings: AngarionSettings, ep: EndpointConfig) -> str | None:
    """Ключ эндпоинта (источник/цель) или ``None``, если аккаунт не описан."""
    account = settings.accounts.get(ep.account)
    if account is None:  # ссылочная целостность — забота bootstrap
        return None
    return make_source_key(account.messenger, ep.account, ep.chat_id, ep.thread_id)


def _status(*, paused: bool, failed: bool) -> NodeStatus:
    """Пауза приоритетнее свежего сбоя — это осознанное состояние оператора."""
    if paused:
        return 'paused'
    if failed:
        return 'failed'
    return 'active'


def _column_top(content_h: int, count: int) -> int:
    """Верх колонки для вертикального центрирования внутри полотна."""
    col_h = count * _NODE_H + max(count - 1, 0) * _V_GAP
    return _MARGIN + (content_h - col_h) // 2


def _cy(top: int, index: int) -> int:
    """Центр узла по вертикали для ``index``-й строки колонки."""
    return top + index * (_NODE_H + _V_GAP) + _NODE_H // 2


async def build_pipeline_graph(deps: AngarionDeps) -> PipelineGraph:
    """
    Собрать модель графа из портов: топология + статус + аннотации.

    Узлы-эндпоинты дедуплицируются по ключу (стабильный порядок —
    первое появление при обходе ``sorted(pipelines)``). Статус узла
    пайплайна: ``paused`` (из ``runtime_config``) → ``failed`` (есть
    ``failed``/``delivery_failed`` за час) → ``active``.
    """
    settings = deps.settings
    overrides = await deps.runtime_config.load()
    paused = overrides.paused_pipelines or frozenset()
    now = datetime.now(UTC)
    fail_since = now - _FAIL_WINDOW
    delivered_since = now - _DELIVERED_WINDOW

    names = sorted(settings.pipelines)
    # дедуп эндпоинтов с сохранением порядка появления
    source_order: list[str] = []
    target_order: list[str] = []
    pipeline_sources: dict[str, list[str]] = {}
    pipeline_targets: dict[str, list[str]] = {}
    for name in names:
        cfg = settings.pipelines[name]
        srcs = [k for ep in cfg.sources if (k := _endpoint_key(settings, ep))]
        tgts = [k for ep in cfg.targets if (k := _endpoint_key(settings, ep))]
        pipeline_sources[name] = srcs
        pipeline_targets[name] = tgts
        for k in srcs:
            if k not in source_order:
                source_order.append(k)
        for k in tgts:
            if k not in target_order:
                target_order.append(k)

    rows = max(len(source_order), len(names), len(target_order), 1)
    content_h = rows * _NODE_H + (rows - 1) * _V_GAP
    height = content_h + 2 * _MARGIN
    col0_x = _MARGIN
    col1_x = _MARGIN + _NODE_W + _COL_GAP
    col2_x = _MARGIN + 2 * (_NODE_W + _COL_GAP)
    width = col2_x + _NODE_W + _MARGIN

    src_top = _column_top(content_h, len(source_order))
    pipe_top = _column_top(content_h, len(names))
    tgt_top = _column_top(content_h, len(target_order))

    src_cy = {k: _cy(src_top, i) for i, k in enumerate(source_order)}
    pipe_cy = {n: _cy(pipe_top, i) for i, n in enumerate(names)}
    tgt_cy = {k: _cy(tgt_top, i) for i, k in enumerate(target_order)}

    sources = tuple(_endpoint_node(k, col0_x, src_cy[k]) for k in source_order)
    targets = tuple(_endpoint_node(k, col2_x, tgt_cy[k]) for k in target_order)

    pipelines: list[GraphNode] = []
    for name in names:
        fail_counts = await deps.analytics.counts_by_kind(
            since=fail_since, pipeline=name
        )
        failed = any(fail_counts.get(k, 0) for k in _FAIL_KINDS)
        day_counts = await deps.analytics.counts_by_kind(
            since=delivered_since, pipeline=name
        )
        delivered = day_counts.get('delivered', 0)
        status = _status(paused=name in paused, failed=failed)
        cy = pipe_cy[name]
        pipelines.append(
            GraphNode(
                key=name,
                label=name,
                status=status,
                fill=_STATUS_FILL[status],
                annotation=f'delivered: {delivered}',
                x=col1_x,
                y=cy - _NODE_H // 2,
                width=_NODE_W,
                height=_NODE_H,
            )
        )

    edges: list[GraphEdge] = []
    for name in names:
        py = pipe_cy[name]
        edges.extend(
            GraphEdge(x1=col0_x + _NODE_W, y1=src_cy[k], x2=col1_x, y2=py)
            for k in pipeline_sources[name]
        )
        edges.extend(
            GraphEdge(x1=col1_x + _NODE_W, y1=py, x2=col2_x, y2=tgt_cy[k])
            for k in pipeline_targets[name]
        )

    depth = await deps.queue.depth()
    return PipelineGraph(
        width=width,
        height=height,
        sources=sources,
        pipelines=tuple(pipelines),
        targets=tuple(targets),
        edges=tuple(edges),
        depth_pending=depth.pending,
        depth_unacked=depth.unacked,
    )


def _endpoint_node(key: str, x: int, cy: int) -> GraphNode:
    """Нейтральный узел-эндпоинт (источник/получатель) без статуса."""
    return GraphNode(
        key=key,
        label=key,
        status='endpoint',
        fill=_STATUS_FILL['endpoint'],
        annotation='',
        x=x,
        y=cy - _NODE_H // 2,
        width=_NODE_W,
        height=_NODE_H,
    )
