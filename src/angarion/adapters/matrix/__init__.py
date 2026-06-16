"""
Matrix-адаптер (M7 часть B, T010): второй боевой адаптер платформы,
доказывающий переносимость портов (§12.10). Профиль аналогичен Telegram
(личный аккаунт, правки ``m.replace``, удаления-redactions, история
через sync, E2EE-комнаты).

Объём фазы B1 — каркас: матрица возможностей, схема ``[accounts.*]``,
парольный ``angarion login`` (homeserver/пароль → ``access_token`` +
``device_id`` в зашифрованную сессию). Listener/sender/маппинг и
расшифровка E2EE — фазы B2/B3.

Optional extra ``angarion[matrix]`` (matrix-nio); E2EE (``nio[e2e]``) —
с фазы B2.
"""
