"""Create a Daily room and an Akapulu live session from the caller's server.

DAILY_API_KEY and AKAPULU_API_KEY come from the environment. A missing or
blank value raises. This posts room_url and the worker token to
POST /api/live-sessions/. It returns the room URL, the bot token, the
browser token, and session_id. The worker token is not returned.

The browser token is named You. The worker token is named Avatar.
A bot that already has a Daily room mints the worker token and posts
/api/live-sessions/ itself.
"""

import json
import os
import time
import uuid

import aiohttp
from pipecat.transports.daily.utils import (
    DailyMeetingTokenParams,
    DailyMeetingTokenProperties,
    DailyRESTHelper,
    DailyRoomParams,
    DailyRoomProperties,
)

from pipecat_akapulu.avatar import _django_origin

BROWSER_NAME = "You"
AVATAR_NAME = "Avatar"
_DAILY_API = "https://api.daily.co/v1"
ROOM_TTL_SECS = 2 * 60 * 60


def _required_text(value: object, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise RuntimeError(f"akapulu call {label} is missing")
    return value.strip()


def _env_secret(name: str) -> str:
    value = os.environ.get(name)
    if type(value) is not str or not value.strip():
        raise RuntimeError(f"{name} is missing")
    return value.strip()


def _daily_api_url() -> str:
    raw = os.environ.get("DAILY_API_URL")
    if raw is None:
        return _DAILY_API
    if type(raw) is not str or not raw.strip():
        raise RuntimeError("DAILY_API_URL is missing")
    url = raw.strip().rstrip("/")
    if not (url.startswith("https://") or url.startswith("http://")):
        raise RuntimeError("DAILY_API_URL must be an http(s) URL")
    return url


def _room_exp_secs(value: object) -> float:
    if type(value) not in (int, float) or value <= 0:
        raise RuntimeError(f"akapulu call room_exp_secs {value!r} is invalid")
    return float(value)


async def _token(
    rest: DailyRESTHelper,
    room_url: str,
    *,
    owner: bool,
    properties: DailyMeetingTokenProperties,
    room_exp_secs: float,
) -> str:
    token = await rest.get_token(
        room_url,
        expiry_time=room_exp_secs,
        owner=owner,
        params=DailyMeetingTokenParams(properties=properties),
    )
    if type(token) is not str or not token:
        raise RuntimeError("akapulu call daily meeting token is missing")
    return token


async def create_akapulu_call(
    *,
    avatar_id: str,
    show_ai_label: bool,
    session: aiohttp.ClientSession,
    room_exp_secs: float = ROOM_TTL_SECS,
) -> tuple[str, str, str, str]:
    """Return room_url, bot token, browser token, and session_id.

    Reads DAILY_API_KEY and AKAPULU_API_KEY from the environment. Creates a
    private Daily room for three participants. The room lasts room_exp_secs,
    which defaults to two hours. Mints the browser, voice-bot, and worker
    tokens for that same lifetime, and posts the live session. The worker
    token is sent to Akapulu Labs and is not returned.
    """
    daily_key = _env_secret("DAILY_API_KEY")
    akapulu_key = _env_secret("AKAPULU_API_KEY")
    avatar = _required_text(avatar_id, "avatar_id")
    try:
        avatar = str(uuid.UUID(avatar))
    except ValueError as exc:
        raise RuntimeError("akapulu call avatar_id is not a UUID") from exc
    if type(show_ai_label) is not bool:
        raise RuntimeError(f"akapulu call show_ai_label {show_ai_label!r} is invalid")
    if type(session) is not aiohttp.ClientSession:
        raise RuntimeError("akapulu call session is missing")
    exp_secs = _room_exp_secs(room_exp_secs)
    daily_url = _daily_api_url()
    origin = _django_origin()

    rest = DailyRESTHelper(
        daily_api_key=daily_key,
        daily_api_url=daily_url,
        aiohttp_session=session,
    )
    room = await rest.create_room(
        DailyRoomParams(
            privacy="private",
            properties=DailyRoomProperties(
                exp=time.time() + exp_secs,
                eject_at_room_exp=True,
                enable_prejoin_ui=False,
                enable_chat=False,
                max_participants=3,
            ),
        )
    )
    if type(room.url) is not str or not room.url.startswith("https://"):
        raise RuntimeError("akapulu call daily room url is missing")
    room_url = room.url
    bot_token = await _token(
        rest,
        room_url,
        owner=True,
        properties=DailyMeetingTokenProperties(is_owner=True),
        room_exp_secs=exp_secs,
    )
    browser_token = await _token(
        rest,
        room_url,
        owner=False,
        properties=DailyMeetingTokenProperties(is_owner=False, user_name=BROWSER_NAME),
        room_exp_secs=exp_secs,
    )
    avatar_token = await _token(
        rest,
        room_url,
        owner=False,
        properties=DailyMeetingTokenProperties(
            is_owner=False,
            user_name=AVATAR_NAME,
            enable_screenshare=False,
            start_video_off=False,
            start_audio_off=False,
            permissions={
                "hasPresence": True,
                "canSend": ["video", "audio"],
                "canAdmin": False,
            },
        ),
        room_exp_secs=exp_secs,
    )
    url = f"{origin}/api/live-sessions/"
    async with session.post(
        url,
        headers={
            "Authorization": f"Bearer {akapulu_key}",
            "Content-Type": "application/json",
        },
        json={
            "avatar_id": avatar,
            "show_ai_label": show_ai_label,
            "room_url": room_url,
            "avatar_token": avatar_token,
        },
    ) as response:
        raw = await response.text()
        if response.status != 200:
            raise RuntimeError(f"POST /api/live-sessions/ returned {response.status} {raw}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("POST /api/live-sessions/ is not JSON") from exc
    if type(payload) is not dict or "session_id" not in payload:
        raise RuntimeError("POST /api/live-sessions/ is missing session_id")
    session_id = payload["session_id"]
    try:
        session_id = str(uuid.UUID(str(session_id)))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("POST /api/live-sessions/ session_id must be a UUID") from exc
    return room_url, bot_token, browser_token, session_id
