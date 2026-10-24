"""Daily bot that publishes an Akapulu avatar from assistant audio.

This file creates a private Daily room, mints the browser, voice-bot, and
Akapulu Labs worker tokens, and POSTs /api/live-sessions/ on the Akapulu
Labs API. It prints a join URL. Open that URL. Audio out and video out stay
off. The Akapulu Labs worker publishes the face and the played audio.

Avatar, This is AI, and the Akapulu Labs API origin are fixed for this
example. Set DAILY_API_KEY, AKAPULU_API_KEY, DEEPGRAM_API_KEY, GOOGLE_API_KEY,
and CARTESIA_API_KEY.

Tested with Pipecat 1.14.0.
"""

import asyncio
import json
import os
import time
import uuid

import aiohttp
from dotenv import load_dotenv
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame, UserAudioRawFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.google.llm import GoogleLLMService
from pipecat.transports.daily.transport import DailyParams, DailyTransport
from pipecat.transports.daily.utils import (
    DailyMeetingTokenParams,
    DailyMeetingTokenProperties,
    DailyRESTHelper,
    DailyRoomParams,
    DailyRoomProperties,
)
from pipecat.workers.runner import WorkerRunner
from pipecat_akapulu import AkapuluAvatar

SAMPLE_RATE = 24000
SETUP_TIMEOUT_SECS = 180.0
USER_JOIN_TIMEOUT_SECS = 180.0
ROOM_TTL_SECS = 2 * 60 * 60
USER_NAME = "You"
AVATAR_NAME = "Avatar"
# Clara (public catalog). This is AI off.
AVATAR_ID = "1f777f64-3758-4a7d-9cbc-c64ae654f7d1"
SHOW_AI_LABEL = False
AKAPULU_API_ORIGIN = "https://akapulu.com"
_DAILY_API = "https://api.daily.co/v1"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} is missing")
    return value.strip()


def api_origin() -> str:
    """Production by default. AKAPULU_API_URL overrides for harnesses."""
    raw = os.environ.get("AKAPULU_API_URL")
    if raw is None:
        return AKAPULU_API_ORIGIN
    origin = raw.strip().rstrip("/")
    if not (origin.startswith("https://") or origin.startswith("http://")):
        raise RuntimeError("AKAPULU_API_URL must be an http(s) origin")
    return origin


def daily_api_url(raw: str | None) -> str:
    if raw is None:
        return _DAILY_API
    url = raw.strip().rstrip("/")
    if not url.startswith("https://"):
        raise RuntimeError("DAILY_API_URL must be an https URL")
    return url


def join_url(room_url: str, user_token: str) -> str:
    if not isinstance(room_url, str) or not room_url.startswith("https://") or "?" in room_url:
        raise RuntimeError("Daily room url is missing")
    if not isinstance(user_token, str) or not user_token:
        raise RuntimeError("Daily browser token is missing")
    return f"{room_url}?t={user_token}"


def live_session_body(
    *,
    room_url: str,
    avatar_token: str,
) -> dict:
    if not isinstance(room_url, str) or not room_url.startswith("https://"):
        raise RuntimeError("Daily room url is missing")
    if not isinstance(avatar_token, str) or not avatar_token:
        raise RuntimeError("Daily avatar token is missing")
    return {
        "avatar_id": AVATAR_ID,
        "show_ai_label": SHOW_AI_LABEL,
        "room_url": room_url,
        "avatar_token": avatar_token,
    }


def _daily_user_name(participant: dict) -> str:
    info = participant.get("info")
    if not isinstance(info, dict):
        raise RuntimeError("Daily participant has no info")
    name = info.get("userName")
    if not isinstance(name, str) or not name:
        raise RuntimeError("Daily participant has no userName")
    return name


def microphone_subscriptions(participants: dict) -> dict:
    """Subscribe the browser mic. Unsubscribe every other participant, including the avatar."""
    settings = {}
    for participant_id, participant in participants.items():
        if not isinstance(participant, dict):
            raise RuntimeError("Daily participant is not an object")
        info = participant.get("info")
        if isinstance(info, dict) and info.get("isLocal") is True:
            continue
        name = _daily_user_name(participant)
        settings[participant_id] = {
            "media": {
                "microphone": "subscribed" if name == USER_NAME else "unsubscribed",
            }
        }
    return settings


class BrowserMic(FrameProcessor):
    """Pass only the browser microphone into the model."""

    def __init__(self):
        super().__init__()
        self._browser_id: str | None = None

    def allow(self, participant_id: str) -> None:
        if self._browser_id is not None and self._browser_id != participant_id:
            raise RuntimeError("Daily browser microphone changed")
        self._browser_id = participant_id

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, UserAudioRawFrame):
            if self._browser_id is None or frame.user_id != self._browser_id:
                return
        await self.push_frame(frame, direction)


async def _token(rest: DailyRESTHelper, room_url: str, *, owner: bool, properties) -> str:
    token = await rest.get_token(
        room_url,
        expiry_time=ROOM_TTL_SECS,
        owner=owner,
        params=DailyMeetingTokenParams(properties=properties),
    )
    if not isinstance(token, str) or not token:
        raise RuntimeError("Daily meeting token was empty")
    return token


async def create_call(session: aiohttp.ClientSession) -> tuple[str, str, str, str]:
    """Return room_url, bot token, browser token, and Akapulu session id."""
    daily_key = _required("DAILY_API_KEY")
    akapulu_key = _required("AKAPULU_API_KEY")
    origin = api_origin()
    rest = DailyRESTHelper(
        daily_api_key=daily_key,
        daily_api_url=daily_api_url(os.environ.get("DAILY_API_URL")),
        aiohttp_session=session,
    )
    room = await rest.create_room(
        DailyRoomParams(
            privacy="private",
            properties=DailyRoomProperties(
                exp=time.time() + ROOM_TTL_SECS,
                eject_at_room_exp=True,
                enable_prejoin_ui=False,
                enable_chat=False,
                max_participants=3,
            ),
        )
    )
    if not isinstance(room.url, str) or not room.url.startswith("https://"):
        raise RuntimeError("Daily room has no url")
    room_url = room.url
    bot_token = await _token(
        rest,
        room_url,
        owner=True,
        properties=DailyMeetingTokenProperties(is_owner=True),
    )
    user_token = await _token(
        rest,
        room_url,
        owner=False,
        properties=DailyMeetingTokenProperties(is_owner=False, user_name=USER_NAME),
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
    )
    body = live_session_body(
        room_url=room_url,
        avatar_token=avatar_token,
    )
    url = f"{origin}/api/live-sessions/"
    async with session.post(
        url,
        headers={
            "Authorization": f"Bearer {akapulu_key}",
            "Content-Type": "application/json",
        },
        json=body,
    ) as response:
        raw = await response.text()
        if response.status != 200:
            raise RuntimeError(f"POST /api/live-sessions/ returned {response.status} {raw}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("POST /api/live-sessions/ is not JSON") from exc
    if not isinstance(payload, dict) or "session_id" not in payload:
        raise RuntimeError("POST /api/live-sessions/ is missing session_id")
    session_id = payload["session_id"]
    try:
        session_id = str(uuid.UUID(str(session_id)))
    except ValueError as exc:
        raise RuntimeError("POST /api/live-sessions/ session_id must be a UUID") from exc
    return room_url, bot_token, user_token, session_id


async def run_bot() -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        room_url, bot_token, user_token, session_id = await create_call(session)
    print(f"Open this URL to join:\n{join_url(room_url, user_token)}", flush=True)

    transport = DailyTransport(
        room_url,
        bot_token,
        "Assistant",
        DailyParams(
            audio_in_enabled=True,
            audio_in_user_tracks=True,
            audio_out_enabled=False,
            video_out_enabled=False,
        ),
    )
    browser_mic = BrowserMic()
    stt = DeepgramSTTService(api_key=_required("DEEPGRAM_API_KEY"))
    llm = GoogleLLMService(
        api_key=_required("GOOGLE_API_KEY"),
        settings=GoogleLLMService.Settings(
            system_instruction=(
                "You are a helpful assistant in a voice conversation. "
                "Your responses will be spoken aloud, so avoid emojis, bullet points, "
                "or other formatting that can't be spoken. Respond briefly."
            ),
        ),
    )
    tts = CartesiaTTSService(
        api_key=_required("CARTESIA_API_KEY"),
        sample_rate=SAMPLE_RATE,
        settings=CartesiaTTSService.Settings(
            voice="a167e0f3-df7e-4d52-a9c3-f949145efdab",
        ),
    )
    avatar = AkapuluAvatar(session_id=session_id)
    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )
    pipeline = Pipeline(
        [
            transport.input(),
            browser_mic,
            stt,
            user_aggregator,
            llm,
            tts,
            avatar,
            transport.output(),
            assistant_aggregator,
        ]
    )
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=16000,
            audio_out_sample_rate=SAMPLE_RATE,
            enable_metrics=True,
        ),
        setup_timeout_secs=SETUP_TIMEOUT_SECS,
    )
    runner = WorkerRunner(handle_sigint=True)
    await runner.add_workers(worker)

    joined = asyncio.Event()
    seen: set[str] = set()

    async def enforce_microphone_subscriptions() -> None:
        settings = microphone_subscriptions(dict(transport.participants()))
        if not settings:
            return
        error = await transport.update_subscriptions(participant_settings=settings)
        if error:
            raise RuntimeError(f"Daily microphone subscription failed: {error}")

    async def note_participant(participant) -> None:
        if not isinstance(participant, dict):
            raise RuntimeError("Daily participant is not an object")
        info = participant.get("info")
        if isinstance(info, dict) and info.get("isLocal") is True:
            return
        participant_id = participant.get("id")
        if not isinstance(participant_id, str) or not participant_id:
            raise RuntimeError("Daily participant has no id")
        if participant_id in seen:
            await enforce_microphone_subscriptions()
            return
        seen.add(participant_id)
        name = _daily_user_name(participant)
        logger.info("Daily participant joined {}", name)
        if name == USER_NAME:
            browser_mic.allow(participant_id)
        await enforce_microphone_subscriptions()
        if name == AVATAR_NAME:
            return
        if name != USER_NAME:
            raise RuntimeError(f"Daily participant {name} is not the browser or the avatar")
        if joined.is_set():
            raise RuntimeError("Daily browser joined twice")
        joined.set()
        context.add_message(
            {
                "role": "user",
                "content": "Start by greeting the user and ask how you can help.",
            }
        )
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_joined")
    async def on_joined(_transport, _data):
        for participant in dict(transport.participants()).values():
            await note_participant(participant)

    @transport.event_handler("on_participant_joined")
    async def on_participant_joined(_transport, participant):
        await note_participant(participant)

    @transport.event_handler("on_participant_updated")
    async def on_participant_updated(_transport, _participant):
        await enforce_microphone_subscriptions()

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(_transport, _client):
        logger.info("Daily client disconnected")
        await runner.cancel()

    async def leave_if_alone() -> None:
        try:
            await asyncio.wait_for(joined.wait(), USER_JOIN_TIMEOUT_SECS)
        except TimeoutError:
            logger.info("Browser did not join")
            await runner.cancel()

    wait_task = asyncio.create_task(leave_if_alone())
    try:
        await runner.run()
    finally:
        wait_task.cancel()
        try:
            await wait_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(run_bot())
