"""Send assistant PCM to the Akapulu avatar worker.

The worker joins the Daily room stored on the session and publishes the
face and the played audio there. This tap does not decode JPEGs and does
not push pictures or playback audio.

Open it with session_id and sample_rate. Idle becomes speech on the first
output chunk louder than the speaking threshold. tail_secs defaults to 0.6,
which is how long assistant audio stays open after TTS stops.

Worker lookup uses the production Django origin. Set AKAPULU_API_URL to an
http(s) origin to use another API. A set value that is empty or not http(s)
raises.

setup starts the connect in the background and returns. StartFrame is
forwarded immediately, after a POST of {} to the live-session status
route. That stamps "Conversation engine joined the call..." for every
pipeline that includes this tap. Loading status is an RTVI server message on
this same pipe, so holding StartFrame until the GPU joins would hide those
updates for the whole boot. call_is_ready stays false until the first
frame. Audio that arrives before the worker audio socket exists is queued.

The session socket stays open for the life of the call. The first message
is the sample rate and the session id. The worker answers with the private
audio socket. Assistant PCM, speaking, and interrupt stay on that socket.
The original TTS frame is not forwarded.

Ingest audio, bot to worker:
  A + uint32 ingest sequence + pcm
Speaking text on the audio socket:
  {"type":"speaking","who":"bot"|"user","state":"started"|"stopped"|"interrupted"}
A loud chunk while the post-TTS tail is still running keeps the gate open.
That chunk, and each bot utterance, go out as a text header and then the raw
s16 bytes:
  {"type":"bot_audio","kind":"reopen"|"utterance","turn":N,"index":N,"sample_rate":N,"nbytes":N}
"""

import asyncio
import json
import os
import struct
import time
import urllib.error
import urllib.request
import uuid

import numpy as np

from loguru import logger
from websockets.asyncio.client import connect

from pipecat.audio.utils import is_silence
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor, FrameProcessorSetup
from pipecat.processors.frameworks.rtvi import RTVIServerMessageFrame
from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    CancelFrame,
    EndFrame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSStoppedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)

CHANNELS = 1
_PROD_DJANGO_ORIGIN = "https://akapulu.com"


def _pcm_is_loud(pcm: bytes) -> bool:
    if len(pcm) < 2:
        return False
    return not is_silence(pcm)


def _pcm_max_abs(pcm: bytes) -> int:
    audio = np.frombuffer(pcm, dtype=np.int16)
    if audio.size == 0:
        raise RuntimeError("akapulu avatar audio frame is empty")
    return int(np.abs(audio.astype(np.int32)).max())


def _django_origin() -> str:
    raw = os.environ.get("AKAPULU_API_URL")
    if raw is None:
        return _PROD_DJANGO_ORIGIN
    origin = raw.strip().rstrip("/")
    if not (origin.startswith("https://") or origin.startswith("http://")):
        raise RuntimeError("AKAPULU_API_URL must be an http(s) origin")
    return origin


_SESSION_TIMEOUT_SECS = 180.0
_SPEAKING = {
    UserStartedSpeakingFrame: ("user", "started"),
    UserStoppedSpeakingFrame: ("user", "stopped"),
}


class AkapuluAvatar(FrameProcessor):
    """Forward assistant audio. The GPU worker publishes the face into Daily.

    Pass session_id and sample_rate. The room and the avatar token are
    already on that session. tail_secs defaults to 0.6.
    """

    def __init__(
        self,
        *,
        sample_rate: int,
        session_id: str,
        tail_secs: float = 0.6,
    ):
        super().__init__()
        self._bind(sample_rate, tail_secs)
        if not isinstance(session_id, str) or not session_id:
            raise RuntimeError("akapulu avatar session_id is missing")
        try:
            self._session_id = str(uuid.UUID(session_id))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("akapulu avatar session_id is missing") from exc

    def _bind(
        self,
        sample_rate: int,
        tail_secs: float,
    ) -> None:
        if sample_rate < 25 or sample_rate % 25 != 0:
            raise RuntimeError(f"akapulu avatar sample rate {sample_rate} is not a positive multiple of 25")
        if tail_secs < 0:
            raise RuntimeError(f"akapulu avatar tail wait {tail_secs} is negative")
        self._url = ""
        self._sample_rate = sample_rate
        self._tail_secs = tail_secs
        self._session = None
        self._audio = None
        self._audio_reader: asyncio.Task | None = None
        self._audio_ready = asyncio.Event()
        self._open_error: Exception | None = None
        self._pipeline_started = False
        self._queued_status: list = []
        self._status_lock = asyncio.Lock()
        self._status_percent: int | None = None
        self._last_poll_body: dict | None = None
        self._updates_task: asyncio.Task | None = None
        self._open_task: asyncio.Task | None = None
        self._pending_audio: list[bytes | str] = []
        self._audio_send_lock = asyncio.Lock()
        self._seq = 0
        self._reader_error: Exception | None = None
        self._connected_at = 0.0
        self._turn = 0
        self._in_bot_turn = False
        self._turn_started = 0.0
        self._last_stop_at = 0.0
        self._closed = False
        self._status_task = None
        self._accept_audio = False
        self._gate_bytes = 0
        self._gate_opened_at = 0.0
        self._tts_stopped_at = 0.0
        self._dropped_bytes = 0
        self._tail_task: asyncio.Task | None = None
        self._tail_bytes = 0
        self._utterance = bytearray()
        self._reopen_index = 0
        self._cutting = False
        self._cut_sent = False

    async def setup(self, setup: FrameProcessorSetup):
        await super().setup(setup)
        # The worker socket can take the whole GPU boot. StartFrame has to
        # pass this processor first or the loading messages cannot be pushed.
        self._open_task = self.create_task(self._open(), "akapulu-avatar-open")

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            await self._interrupt()
            await self.push_frame(frame, direction)
            return
        if isinstance(frame, StartFrame):
            await asyncio.to_thread(self._post_engine_joined)
            await self.push_frame(frame, direction)
            async with self._status_lock:
                queued = self._queued_status
                self._queued_status = []
                self._pipeline_started = True
            for status_frame in queued:
                await self.push_frame(status_frame)
            return
        if isinstance(frame, TTSStoppedFrame):
            self._tts_stopped_at = time.perf_counter()
            self._log_audio_end_hint("tts_stopped")
            stopped = BotStoppedSpeakingFrame()
            await self.push_frame(stopped, FrameDirection.UPSTREAM)
            await self.push_frame(stopped)
            logger.info("akapulu avatar sent bot-stopped-speaking")
            if self._cut_sent:
                logger.info("akapulu avatar stop after interrupt, no flush")
            else:
                if self._tail_task is not None and not self._tail_task.done():
                    await self.cancel_task(self._tail_task)
                self._tail_bytes = 0
                self._tail_task = self.create_task(self._lock_after_tail(), "akapulu-avatar-tail")
        elif isinstance(frame, LLMFullResponseEndFrame):
            logger.info("akapulu avatar gpt response end accept={}", int(self._accept_audio))
        speaking = _SPEAKING.get(type(frame))
        if speaking is not None:
            await self._send_speaking(*speaking)
        if isinstance(frame, (EndFrame, CancelFrame)):
            await self._close()
        elif isinstance(frame, OutputAudioRawFrame) and direction == FrameDirection.DOWNSTREAM:
            loud = _pcm_is_loud(frame.audio)
            if not self._accept_audio:
                if not loud:
                    before = self._dropped_bytes
                    self._dropped_bytes += len(frame.audio)
                    stride = self._sample_rate * 2
                    if before == 0 or before // stride != self._dropped_bytes // stride:
                        logger.info(
                            "akapulu avatar audio dropped while locked bytes={}",
                            self._dropped_bytes,
                        )
                    return
                await self._open_from_idle()
            elif loud and self._tail_task is not None and not self._tail_task.done():
                await self.cancel_task(self._tail_task)
                self._tail_task = None
                logger.info(
                    "akapulu avatar next utterance kept the gate open tail_bytes={} max_abs={}",
                    self._tail_bytes,
                    _pcm_max_abs(frame.audio),
                )
                self._tail_bytes = 0
                await self._send_bot_audio("reopen", self._turn, self._reopen_index, frame.audio)
                self._reopen_index += 1
            if self._tail_task is not None and not self._tail_task.done():
                self._tail_bytes += len(frame.audio)
            await self._ingest(frame)
            return
        await self.push_frame(frame, direction)

    async def _open(self) -> None:
        if self._session is not None:
            return
        t0 = time.perf_counter()
        logger.info("akapulu avatar connecting")
        self._url = await self._wait_worker_wss()
        self._updates_task = asyncio.create_task(self._poll_updates())
        try:
            self._session = await connect(
                self._url,
                open_timeout=_SESSION_TIMEOUT_SECS,
                max_size=None,
                compression=None,
            )
            self._status_task = asyncio.create_task(self._read_session())
            await self._session.send(
                json.dumps(
                    {
                        "type": "daily",
                        "sample_rate": self._sample_rate,
                        "session_id": self._session_id,
                    }
                )
            )
        except Exception:
            if self._updates_task is not None and not self._updates_task.done():
                self._updates_task.cancel()
            if self._status_task is not None and not self._status_task.done():
                self._status_task.cancel()
            if self._session is not None:
                await self._session.close()
                self._session = None
            raise
        self._connected_at = time.perf_counter()
        logger.info(
            "akapulu avatar session open sample_rate={} tail_secs={} session_ms={:.1f}",
            self._sample_rate,
            self._tail_secs,
            (self._connected_at - t0) * 1000,
        )

    def _status_url(self) -> str:
        origin = _django_origin()
        return f"{origin}/api/live-sessions/{self._session_id}/status/"

    def _post_engine_joined(self) -> None:
        request = urllib.request.Request(
            self._status_url(),
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"engine joined returned {response.status}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"engine joined returned {exc.code} {body}") from exc

    def _fetch_worker_poll(self) -> dict:
        request = urllib.request.Request(self._status_url(), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"worker url lookup returned {response.status}")
                raw = response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"worker url lookup returned {exc.code} {body}") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError("worker url lookup is not JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("worker url lookup is not an object")
        if "worker_wss_url" not in payload:
            raise RuntimeError("worker url lookup is missing worker_wss_url")
        return payload

    def _worker_wss_from_poll(self, payload: dict) -> str | None:
        url = payload["worker_wss_url"]
        if url is None:
            return None
        if not isinstance(url, str) or not url.startswith("wss://"):
            raise RuntimeError("worker url lookup worker_wss_url is not wss://")
        return url

    def _poll_status_body(self, payload: dict) -> dict | None:
        """Milestone text for the pipeline. Skip a poll that is behind a socket status."""
        ready = payload.get("call_is_ready")
        text = payload.get("latest_update_text")
        percent = payload.get("completion_percent")
        delayed = payload.get("assistant_startup_delayed")
        if type(ready) is not bool:
            raise RuntimeError("worker url lookup call_is_ready is missing")
        if not isinstance(text, str) or text == "":
            raise RuntimeError("worker url lookup latest_update_text is missing")
        if type(percent) is not int:
            raise RuntimeError("worker url lookup completion_percent is missing")
        if type(delayed) is not bool:
            raise RuntimeError("worker url lookup assistant_startup_delayed is missing")
        # Ten milestone columns, so this percent sits below the socket's 70/85/95.
        # Do not replace a later socket sentence with an earlier milestone sentence.
        # The watchdog delay sentence still replaces that text. The bar does not rewind.
        if (
            self._status_percent is not None
            and percent < self._status_percent
            and not ready
            and not delayed
        ):
            return None
        if self._status_percent is not None and percent < self._status_percent and not ready:
            percent = self._status_percent
        return {
            "type": "status",
            "call_is_ready": ready,
            "latest_update_text": text,
            "completion_percent": percent,
        }

    async def _emit_poll_status(self, payload: dict) -> None:
        body = self._poll_status_body(payload)
        if body is None or body == self._last_poll_body:
            return
        self._last_poll_body = body
        await self._emit_status(body)

    async def _wait_worker_wss(self) -> str:
        deadline = time.perf_counter() + _SESSION_TIMEOUT_SECS
        while True:
            payload = await asyncio.to_thread(self._fetch_worker_poll)
            await self._emit_poll_status(payload)
            worker_wss_url = self._worker_wss_from_poll(payload)
            if worker_wss_url:
                return worker_wss_url
            if time.perf_counter() >= deadline:
                raise RuntimeError("akapulu avatar worker url was not posted")
            await asyncio.sleep(0.5)

    async def _poll_updates(self) -> None:
        """Keep reading milestone text after the socket URL is posted."""
        try:
            while not self._closed:
                await asyncio.sleep(0.5)
                if self._closed:
                    return
                payload = await asyncio.to_thread(self._fetch_worker_poll)
                await self._emit_poll_status(payload)
                if payload.get("call_is_ready") is True:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_open(exc)
            if not self._closed:
                logger.error("akapulu avatar updates poll failed: {}", exc)
                raise

    async def _wait_for_audio(self) -> None:
        if self._audio is not None:
            return
        try:
            await asyncio.wait_for(self._audio_ready.wait(), _SESSION_TIMEOUT_SECS)
        except TimeoutError as exc:
            raise RuntimeError("akapulu avatar streams did not arrive") from exc
        if self._open_error is not None:
            raise self._open_error

    async def _emit_status(self, body: dict) -> None:
        percent = body.get("completion_percent")
        if type(percent) is int and (
            self._status_percent is None or percent > self._status_percent
        ):
            self._status_percent = percent
        frame = RTVIServerMessageFrame(data=body)
        async with self._status_lock:
            if not self._pipeline_started:
                self._queued_status.append(frame)
                return
        await self.push_frame(frame)

    async def _take_streams(self, body: dict) -> None:
        if self._audio is not None:
            raise RuntimeError("akapulu avatar streams arrived twice")
        if "video" in body:
            raise RuntimeError("akapulu avatar session still has a video socket")
        audio_url = body.get("audio")
        if not isinstance(audio_url, str) or not audio_url.startswith("wss://"):
            raise RuntimeError("akapulu avatar audio URL is missing")
        audio = await connect(audio_url, open_timeout=60, max_size=None, compression=None)
        async with self._audio_send_lock:
            self._audio = audio
            await self._audio.send(json.dumps({"type": "format", "sample_rate": self._sample_rate}))
            pending = self._pending_audio
            self._pending_audio = []
            for message in pending:
                await self._audio.send(message)
        self._audio_ready.set()
        if self._audio_reader is None:
            self._audio_reader = self.create_task(self._read_audio(), "akapulu-avatar-audio")

    def _fail_open(self, exc: Exception) -> None:
        if self._open_error is None:
            self._open_error = exc
        if not self._audio_ready.is_set():
            self._audio_ready.set()

    async def _open_from_idle(self) -> None:
        """Idle becomes speech. The stop and the tail flush stay on TTSStoppedFrame."""
        if self._accept_audio:
            raise RuntimeError("akapulu avatar opened from idle while the gate was open")
        self._gate_bytes = 0
        self._gate_opened_at = time.perf_counter()
        if self._utterance:
            raise RuntimeError("akapulu avatar opened a turn with an unsent utterance")
        self._accept_audio = True
        self._dropped_bytes = 0
        self._tail_bytes = 0
        self._reopen_index = 0
        self._cut_sent = False
        logger.info("akapulu avatar audio unlocked")
        if not self._in_bot_turn:
            self._begin_turn()
        await self._send_speaking("bot", "started")
        started = BotStartedSpeakingFrame()
        await self.push_frame(started, FrameDirection.UPSTREAM)
        await self.push_frame(started)
        logger.info("akapulu avatar sent bot-started-speaking")

    def _begin_turn(self) -> None:
        if self._in_bot_turn:
            raise RuntimeError("akapulu avatar bot turn started twice")
        self._turn += 1
        self._in_bot_turn = True
        self._turn_started = time.perf_counter()
        since_ms = 0.0
        if self._turn == 1 and self._connected_at:
            since_ms = (self._turn_started - self._connected_at) * 1000
        elif self._last_stop_at:
            since_ms = (self._turn_started - self._last_stop_at) * 1000
        logger.info("akapulu avatar turn={} start since_ms={:.1f}", self._turn, since_ms)

    def _end_turn(self) -> None:
        if not self._in_bot_turn:
            logger.info("akapulu avatar end turn ignored, no open turn")
            return
        dur_ms = (time.perf_counter() - self._turn_started) * 1000
        self._last_stop_at = time.perf_counter()
        self._in_bot_turn = False
        logger.info(
            "akapulu avatar turn={} end dur_ms={:.0f} audio_bytes={}",
            self._turn,
            dur_ms,
            self._gate_bytes,
        )

    async def _lock_after_tail(self) -> None:
        await asyncio.sleep(self._tail_secs)
        self._tail_task = None
        if not self._accept_audio:
            return
        self._accept_audio = False
        self._log_audio_end_hint("worker_stopped")
        logger.info(
            "akapulu avatar audio locked after tail tail_bytes={}",
            self._tail_bytes,
        )
        await self._send_speaking("bot", "stopped")
        await self._send_utterance()
        if self._in_bot_turn:
            self._end_turn()

    async def _interrupt(self) -> None:
        if self._tail_task is not None and not self._tail_task.done():
            await self.cancel_task(self._tail_task)
            self._tail_task = None
        speaking = self._accept_audio or self._in_bot_turn
        self._accept_audio = False
        if not speaking or self._cut_sent:
            return
        self._cut_sent = True
        self._cutting = True
        logger.info("akapulu avatar interrupt")
        await self._send_utterance()
        await self._send_speaking("bot", "interrupted")
        stopped = BotStoppedSpeakingFrame()
        await self.push_frame(stopped, FrameDirection.UPSTREAM)
        if self._in_bot_turn:
            self._end_turn()

    async def _take_cut(self, text: str) -> None:
        try:
            body = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("akapulu avatar cut message is not JSON") from exc
        if body.get("type") != "cut":
            raise RuntimeError(f"akapulu avatar audio socket rejected text {body.get('type')!r}")
        drained = body.get("drained")
        if drained not in (0, 1):
            raise RuntimeError(f"akapulu avatar cut drained is {drained!r}")
        if not self._cutting:
            raise RuntimeError("akapulu avatar cut arrived with no interrupt")
        if drained == 1:
            next_idx = body.get("next_idx")
            if not isinstance(next_idx, int):
                raise RuntimeError("akapulu avatar cut is missing next_idx")
            logger.info("akapulu avatar cut drained next_idx={}", next_idx)
        else:
            logger.info("akapulu avatar cut left the flush running")
        self._cutting = False

    async def _send_speaking(self, who: str, state: str) -> None:
        t0 = time.perf_counter()
        await self._send_or_queue(json.dumps({"type": "speaking", "who": who, "state": state}))
        send_ms = (time.perf_counter() - t0) * 1000
        logger.info("akapulu avatar sent speaking who={} state={} send_ms={:.1f}", who, state, send_ms)

    async def _read_audio(self) -> None:
        try:
            while not self._closed:
                message = await self._audio.recv()
                if isinstance(message, str):
                    await self._take_cut(message)
                    continue
                raise RuntimeError("akapulu avatar audio socket received playback bytes")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_reader(exc)

    def _fail_reader(self, exc: Exception) -> None:
        if self._closed:
            return
        self._reader_error = exc
        logger.exception("akapulu avatar reader failed")

    def _log_audio_end_hint(self, signal: str) -> None:
        """Where we learn the utterance is ending, against audio already ingested."""
        now = time.perf_counter()
        audio_ms = self._gate_bytes / (self._sample_rate * 2) * 1000
        since_unlock_ms = (now - self._gate_opened_at) * 1000 if self._gate_opened_at else 0.0
        since_tts_ms = (now - self._tts_stopped_at) * 1000 if self._tts_stopped_at else 0.0
        logger.info(
            "akapulu avatar audio end hint signal={} audio_ms={:.0f} "
            "since_unlock_ms={:.0f} since_tts_stopped_ms={:.0f}",
            signal,
            audio_ms,
            since_unlock_ms,
            since_tts_ms,
        )

    async def _send_utterance(self) -> None:
        pcm = bytes(self._utterance)
        if not pcm:
            return
        if self._turn < 1:
            raise RuntimeError("akapulu avatar utterance has no turn")
        await self._send_bot_audio("utterance", self._turn, 0, pcm)
        self._utterance.clear()

    async def _send_bot_audio(self, kind: str, turn: int, index: int, pcm: bytes) -> None:
        if kind not in ("reopen", "utterance"):
            raise RuntimeError(f"akapulu avatar bot audio kind {kind!r}")
        if turn < 1 or index < 0:
            raise RuntimeError(f"akapulu avatar bot audio turn={turn} index={index}")
        if len(pcm) < 2 or len(pcm) % 2:
            raise RuntimeError(f"akapulu avatar bot audio length {len(pcm)} is not s16")
        header = json.dumps(
            {
                "type": "bot_audio",
                "kind": kind,
                "turn": turn,
                "index": index,
                "sample_rate": self._sample_rate,
                "nbytes": len(pcm),
            }
        )
        async with self._audio_send_lock:
            if self._audio is None:
                raise RuntimeError("akapulu avatar bot audio socket is not open")
            await self._audio.send(header)
            await self._audio.send(pcm)
        logger.info(
            "akapulu avatar sent bot audio kind={} turn={} index={} bytes={}",
            kind,
            turn,
            index,
            len(pcm),
        )

    async def _send_or_queue(self, message: bytes | str) -> None:
        async with self._audio_send_lock:
            if self._audio is None:
                self._pending_audio.append(message)
                return
            await self._audio.send(message)

    async def _ingest(self, frame: OutputAudioRawFrame) -> None:
        if self._reader_error is not None:
            raise self._reader_error
        if frame.sample_rate != self._sample_rate or frame.num_channels != CHANNELS:
            raise RuntimeError(
                f"akapulu avatar expects {self._sample_rate} Hz mono s16, "
                f"got {frame.sample_rate} Hz {frame.num_channels} ch"
            )
        self._gate_bytes += len(frame.audio)
        self._utterance.extend(frame.audio)
        seq = self._seq
        self._seq += 1
        t_send = time.perf_counter()
        await self._send_or_queue(b"A" + struct.pack(">I", seq) + frame.audio)
        send_ms = (time.perf_counter() - t_send) * 1000
        if send_ms > 20:
            logger.warning(
                "akapulu avatar ingest slow seq={} send_ms={:.1f} bytes={}",
                seq,
                send_ms,
                len(frame.audio),
            )

    async def _read_session(self) -> None:
        """Status arrives as the worker loads. Streams opens the audio socket."""
        try:
            while not self._closed and self._session is not None:
                raw = await self._session.recv()
                if not isinstance(raw, str):
                    continue
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError("akapulu avatar session message is not JSON") from exc
                if not isinstance(body, dict):
                    raise RuntimeError("akapulu avatar session message is not an object")
                kind = body.get("type")
                if kind == "status":
                    await self._emit_status(body)
                    continue
                if kind == "streams":
                    await self._take_streams(body)
                    continue
                raise RuntimeError(f"akapulu avatar session type is {kind!r}")
        except Exception as exc:
            self._fail_open(exc)
            if not self._closed:
                logger.info("akapulu avatar session socket closed")

    async def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._send_utterance()
        if self._open_task is not None and not self._open_task.done():
            await self.cancel_task(self._open_task)
        if self._updates_task is not None and not self._updates_task.done():
            self._updates_task.cancel()
        if self._status_task is not None and not self._status_task.done():
            self._status_task.cancel()
        for sock in (self._audio, self._session):
            if sock is not None:
                await sock.close()
        if self._audio_reader is not None and not self._audio_reader.done():
            await self.cancel_task(self._audio_reader)
        if self._reader_error is not None:
            logger.error("akapulu avatar closed turn={}", self._turn)
            return
        logger.info("akapulu avatar closed turn={}", self._turn)
