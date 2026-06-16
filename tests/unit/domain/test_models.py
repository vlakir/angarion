"""Инварианты доменных DTO (§4): frozen, extra=forbid, JSON-roundtrip, UTC."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import BaseModel, ValidationError

from angarion.domain.models import (
    AccountRef,
    Address,
    AnalyticsEvent,
    DeadLetter,
    DeliveryReceipt,
    EventKind,
    InboundEvent,
    MediaRef,
    OutboundMessage,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    QueueDepth,
    QueueEnvelope,
    QueueItem,
    RegistryDelta,
    RegistryOutcome,
    RegistryRecord,
    RegistryVersion,
    SourceCursor,
    TargetSpec,
    Verdict,
)

if TYPE_CHECKING:
    from angarion.domain.models import ScopedState

NOW = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
UID = UUID('00000000-0000-0000-0000-000000000001')


def make_address(**overrides: object) -> Address:
    fields: dict[str, object] = {'messenger': 'telegram', 'chat_id': '-100123'}
    fields.update(overrides)
    return Address.model_validate(fields)


def make_event(**overrides: object) -> InboundEvent:
    fields: dict[str, object] = {
        'uid': UID,
        'kind': EventKind.MESSAGE_NEW,
        'dedup_key': 'telegram:acc1:-100123:42:new',
        'origin': 'live',
        'source': make_address(),
        'received_by': AccountRef(messenger='telegram', account_id='acc1'),
        'external_id': '42',
        'text': 'hello',
        'event_at': NOW,
        'received_at': NOW,
    }
    fields.update(overrides)
    return InboundEvent.model_validate(fields)


def make_media(**overrides: object) -> MediaRef:
    fields: dict[str, object] = {'kind': 'photo'}
    fields.update(overrides)
    return MediaRef.model_validate(fields)


def make_outbound(**overrides: object) -> OutboundMessage:
    fields: dict[str, object] = {
        'idempotency_key': 'k->p:c:0',
        'target': make_address(chat_id='-100999'),
        'send_via': AccountRef(messenger='telegram', account_id='acc1'),
        'text': 'hi',
    }
    fields.update(overrides)
    return OutboundMessage.model_validate(fields)


def representative_dtos() -> list[BaseModel]:
    """По экземпляру каждого DTO — для общих инвариантов."""
    return [
        make_address(thread_id='55', title='angarion 1'),
        AccountRef(messenger='telegram', account_id='acc1'),
        make_event(previous_text='old', content_hash='abc', raw={'id': 42}),
        make_media(
            ref='AgACfile_id',
            mime_type='image/jpeg',
            file_name='photo.jpg',
            size=1024,
            width=800,
            height=600,
        ),
        make_event(media=[make_media(), make_media(kind='document')]),
        make_outbound(extra={'parse_mode': 'html'}),
        make_outbound(media=[make_media()]),
        ProcessingResult(verdict=Verdict.DELIVER, outbound=[make_outbound()]),
        AnalyticsEvent(uid=UID, kind='ingested', event_uid=UID, at=NOW),
        PipelineContextData(
            pipeline='digest',
            targets=[
                TargetSpec(
                    target=make_address(chat_id='-100999'),
                    send_via=AccountRef(messenger='telegram', account_id='acc1'),
                )
            ],
            settings={'window': 10},
        ),
        QueueEnvelope(pipeline='digest', event=make_event()),
        QueueDepth(pending=1, unacked=0),
        DeadLetter(
            uid=UID,
            envelope=QueueEnvelope(pipeline='digest', event=make_event(), attempt=5),
            error='ProcessingError: boom',
            failed_at=NOW,
        ),
        DeliveryReceipt(external_id='777', delivered_at=NOW),
        RegistryRecord(
            source_key='telegram:acc1:-100123',
            external_id='42',
            text='hello',
            content_hash='abc',
            event_at=NOW,
        ),
        RegistryVersion(text='old', content_hash='def', recorded_at=NOW),
        RegistryDelta(outcome=RegistryOutcome.TEXT_CHANGED, previous_text='old'),
        SourceCursor(source_key='telegram:acc1:-100123', payload={'last': 42}, updated_at=NOW),
        TargetSpec(
            target=make_address(chat_id='-100999'),
            send_via=AccountRef(messenger='telegram', account_id='acc1'),
        ),
    ]


class TestDTOInvariants:
    @pytest.mark.parametrize(
        'dto', representative_dtos(), ids=lambda d: type(d).__name__
    )
    def test_frozen(self, dto: BaseModel) -> None:
        field = next(iter(type(dto).model_fields))
        with pytest.raises(ValidationError):
            setattr(dto, field, 'mutated')

    @pytest.mark.parametrize(
        'dto', representative_dtos(), ids=lambda d: type(d).__name__
    )
    def test_extra_forbidden(self, dto: BaseModel) -> None:
        payload = dto.model_dump()
        payload['unexpected_field'] = 1
        with pytest.raises(ValidationError):
            type(dto).model_validate(payload)

    @pytest.mark.parametrize(
        'dto', representative_dtos(), ids=lambda d: type(d).__name__
    )
    def test_json_roundtrip(self, dto: BaseModel) -> None:
        restored = type(dto).model_validate_json(dto.model_dump_json())
        assert restored == dto


class TestAddress:
    def test_thread_id_defaults_to_none(self) -> None:
        assert make_address().thread_id is None

    @pytest.mark.parametrize(
        'bad_messenger',
        ['Telegram', '1telegram', 't', 'a' * 33, 'tele-gram', ''],
    )
    def test_messenger_pattern_enforced(self, bad_messenger: str) -> None:
        with pytest.raises(ValidationError):
            make_address(messenger=bad_messenger)

    @pytest.mark.parametrize('good_messenger', ['telegram', 'max', 'my_imap2'])
    def test_messenger_pattern_accepts(self, good_messenger: str) -> None:
        assert make_address(messenger=good_messenger).messenger == good_messenger


class TestEventKind:
    def test_closed_set(self) -> None:
        assert {k.value for k in EventKind} == {
            'message_new',
            'message_edited',
            'message_deleted',
        }


class TestInboundEvent:
    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValidationError):
            make_event(event_at=datetime(2026, 6, 11, 12, 0))

    def test_origin_is_closed(self) -> None:
        with pytest.raises(ValidationError):
            make_event(origin='replay')

    def test_optional_enrichment_defaults(self) -> None:
        event = make_event()
        assert event.previous_text is None
        assert event.content_hash is None
        assert event.reply_to_external_id is None
        assert event.has_media is False
        assert event.media == []
        assert event.raw == {}

    def test_media_carried(self) -> None:
        event = make_event(media=[make_media(kind='video')])
        assert [m.kind for m in event.media] == ['video']

    def test_has_media_derives_from_media(self) -> None:
        assert make_event().has_media is False
        assert make_event(media=[make_media()]).has_media is True

    def test_has_media_not_an_input_field(self) -> None:
        """``has_media`` — производное свойство, не поле ввода (M7 A2)."""
        with pytest.raises(ValidationError):
            make_event(has_media=True)


class TestMediaRef:
    def test_only_kind_required(self) -> None:
        media = MediaRef(kind='photo')
        assert media.kind == 'photo'
        assert media.ref is None
        assert media.mime_type is None
        assert media.file_name is None
        assert media.size is None
        assert media.width is None
        assert media.height is None
        assert media.duration is None
        assert media.local_path is None

    def test_kind_is_open_string(self) -> None:
        """``kind`` — открытая строка (как ``Messenger``): новые платформы
        регистрируют свои виды вложений без правки домена."""
        assert MediaRef(kind='lottie_sticker').kind == 'lottie_sticker'


class TestQueueEnvelope:
    def test_defaults(self) -> None:
        envelope = QueueEnvelope(pipeline='digest', event=make_event())
        assert envelope.attempt == 0
        assert envelope.not_before is None

    def test_retry_copy(self) -> None:
        """C-8: retry — копия с attempt+1 и not_before в будущем."""
        envelope = QueueEnvelope(pipeline='digest', event=make_event())
        retry = envelope.model_copy(
            update={'attempt': envelope.attempt + 1, 'not_before': NOW}
        )
        assert retry.attempt == 1
        assert retry.not_before == NOW
        assert retry.event == envelope.event
        assert envelope.attempt == 0

    def test_not_before_requires_aware_datetime(self) -> None:
        with pytest.raises(ValidationError):
            QueueEnvelope(
                pipeline='digest',
                event=make_event(),
                not_before=datetime(2026, 6, 11),
            )


class TestQueueItem:
    def test_receipt_is_opaque(self) -> None:
        envelope = QueueEnvelope(pipeline='digest', event=make_event())
        item = QueueItem(envelope=envelope, receipt=7)
        assert item.receipt == 7
        assert QueueItem(envelope=envelope, receipt={'tag': 'x'}).receipt == {
            'tag': 'x'
        }


class TestRegistryDelta:
    def test_outcomes_include_stale(self) -> None:
        """A-3: четыре исхода, включая stale (staleness-guard §6.1)."""
        assert {o.value for o in RegistryOutcome} == {
            'is_new',
            'text_changed',
            'unchanged',
            'stale',
        }

    def test_previous_text_optional(self) -> None:
        delta = RegistryDelta(outcome=RegistryOutcome.IS_NEW)
        assert delta.previous_text is None


class TestProcessingResult:
    def test_defaults(self) -> None:
        result = ProcessingResult(verdict=Verdict.DROP)
        assert result.outbound == []
        assert result.events == []
        assert result.note is None


class FakeState:
    async def get(self, key: str) -> str | None:
        return None

    async def set(self, key: str, value: str) -> None:
        return None

    async def delete(self, key: str) -> None:
        return None

    async def keys(self, prefix: str = '') -> list[str]:
        return []


def fake_factory(event: InboundEvent, target: Address, n: int) -> str:
    return f'{event.dedup_key}->{target.chat_id}:{n}'


class TestProcessorServices:
    def test_composition_construct(self) -> None:
        """A-2: ProcessorServices — не DTO; frozen-контейнер сервисов.

        log не валидируется (SkipValidation) — прокси structlog не
        проходит isinstance протокола.
        """
        state: ScopedState = FakeState()
        services = ProcessorServices(
            log=object(), state=state, make_idempotency_key=fake_factory
        )
        assert services.state is state
        with pytest.raises(ValidationError):
            services.log = object()

    def test_state_must_satisfy_scoped_state(self) -> None:
        with pytest.raises(ValidationError):
            ProcessorServices(
                log=object(), state=object(), make_idempotency_key=fake_factory
            )

    def test_factory_must_be_callable(self) -> None:
        with pytest.raises(ValidationError):
            ProcessorServices(
                log=object(), state=FakeState(), make_idempotency_key='nope'
            )
