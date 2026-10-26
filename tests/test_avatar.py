"""Daily handshake for AkapuluAvatar. Run from the repo root: python tests/test_avatar.py"""

import asyncio
import json
import os
import struct
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    OutputAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from loguru import logger
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from websockets.asyncio.server import serve

from pipecat_akapulu.avatar import AkapuluAvatar, _PROD_DJANGO_ORIGIN, _django_origin


SESSION = "11111111-1111-1111-1111-111111111111"


def _tap(connect_url: str | None = None) -> AkapuluAvatar:
    tap = AkapuluAvatar(
        session_id=SESSION,
    )
    tap._bind(24000)
    if connect_url is not None:
        async def _ready():
            return connect_url

        async def _no_poll():
            return

        tap._wait_worker_wss = _ready
        tap._poll_updates = _no_poll
    return tap


def _poll(**overrides) -> dict:
    body = {
        "worker_wss_url": None,
        "call_is_ready": False,
        "latest_update_text": "Creating call room...",
        "completion_percent": 40,
        "assistant_startup_delayed": False,
    }
    body.update(overrides)
    return body


class _Audio:
    def __init__(self):
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        return None


async def _serve(handler):
    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


async def test_daily_message() -> None:
    seen = {}

    async def session(ws):
        seen["daily"] = json.loads(await ws.recv())
        await ws.send(
            json.dumps(
                {
                    "type": "status",
                    "call_is_ready": False,
                    "latest_update_text": "Initializing media services...",
                    "completion_percent": 70,
                }
            )
        )
        await ws.send(json.dumps({"type": "streams", "audio": "ws://127.0.0.1/v1/audio"}))
        await asyncio.sleep(1)

    server, url = await _serve(session)
    tap = _tap(url)
    try:
        await tap._open()
        try:
            await tap._wait_for_audio()
        except RuntimeError as exc:
            if "audio URL is missing" not in str(exc):
                raise
        else:
            raise RuntimeError("ws audio url was accepted")
        body = seen.get("daily")
        if body != {
            "type": "daily",
            "sample_rate": 24000,
            "session_id": SESSION,
        }:
            raise RuntimeError(f"daily message was {body}")
        if len(tap._queued_status) != 1:
            raise RuntimeError(f"status was not queued: {tap._queued_status}")
    finally:
        if tap._status_task is not None and not tap._status_task.done():
            tap._status_task.cancel()
        server.close()
        await server.wait_closed()


async def test_video_url_is_rejected() -> None:
    async def session(ws):
        await ws.recv()
        await ws.send(
            json.dumps(
                {
                    "type": "streams",
                    "audio": "wss://example.invalid/audio",
                    "video": "wss://example.invalid/video",
                }
            )
        )
        await asyncio.sleep(1)

    server, url = await _serve(session)
    tap = _tap(url)
    try:
        await tap._open()
        try:
            await tap._wait_for_audio()
        except RuntimeError as exc:
            if "video socket" not in str(exc):
                raise
        else:
            raise RuntimeError("video url was accepted")
    finally:
        if tap._status_task is not None and not tap._status_task.done():
            tap._status_task.cancel()
        server.close()
        await server.wait_closed()


def _pcm(sample: int, count: int = 48) -> bytes:
    return struct.pack("<h", sample) * count


async def _push_audio(tap: AkapuluAvatar, pcm: bytes) -> None:
    await tap.process_frame(
        OutputAudioRawFrame(audio=pcm, sample_rate=24000, num_channels=1),
        FrameDirection.DOWNSTREAM,
    )


async def test_idle_opens_on_loud_audio_not_tts_started() -> None:
    tap = _tap("wss://example.invalid/live")
    pushed = []

    async def push(frame, direction=FrameDirection.DOWNSTREAM):
        pushed.append(type(frame))

    tap.push_frame = push
    tap._audio = _Audio()
    await tap.process_frame(TTSStartedFrame(), FrameDirection.DOWNSTREAM)
    if tap._accept_audio or tap._audio.sent:
        raise RuntimeError("TTSStarted opened the gate")
    if pushed != [TTSStartedFrame]:
        raise RuntimeError(f"TTSStarted was not forwarded: {pushed}")

    await _push_audio(tap, _pcm(0))
    if tap._accept_audio or tap._audio.sent:
        raise RuntimeError("silence opened the gate")

    loud = _pcm(1000)
    await _push_audio(tap, loud)
    if not tap._accept_audio or tap._turn != 1:
        raise RuntimeError(f"loud audio did not open turn={tap._turn} accept={tap._accept_audio}")
    speaking = [message for message in tap._audio.sent if isinstance(message, str)]
    if speaking != [json.dumps({"type": "speaking", "who": "bot", "state": "started"})]:
        raise RuntimeError(f"speaking start was {speaking}")
    packets = [message for message in tap._audio.sent if isinstance(message, bytes)]
    if len(packets) != 1 or packets[0][5:] != loud:
        raise RuntimeError("loud chunk was not ingested")
    started = [kind for kind in pushed if kind is BotStartedSpeakingFrame]
    if len(started) != 2:
        raise RuntimeError(f"bot started frames were {len(started)}")

    await _push_audio(tap, loud)
    speaking = [message for message in tap._audio.sent if isinstance(message, str)]
    if len(speaking) != 1 or tap._turn != 1:
        raise RuntimeError("a second loud chunk started another turn")


async def test_threshold_sample_stays_idle() -> None:
    tap = _tap("wss://example.invalid/live")

    async def push(frame, direction=FrameDirection.DOWNSTREAM):
        return None

    tap.push_frame = push
    tap._audio = _Audio()
    await _push_audio(tap, _pcm(20))
    if tap._accept_audio or tap._audio.sent:
        raise RuntimeError("a sample at the speaking threshold opened the gate")
    await _push_audio(tap, _pcm(21))
    if not tap._accept_audio or tap._turn != 1:
        raise RuntimeError("a sample above the speaking threshold stayed idle")


async def test_loud_audio_during_tail_restarts_the_lock() -> None:
    tap = _tap("wss://example.invalid/live")

    async def push(frame, direction=FrameDirection.DOWNSTREAM):
        return None

    tap.push_frame = push
    tap._audio = _Audio()
    await _push_audio(tap, _pcm(1000))
    hold = asyncio.Event()
    cancelled = []

    def create_task(coroutine, name=None):
        coroutine.close()
        return asyncio.get_running_loop().create_task(hold.wait(), name=name)

    async def cancel_task(task, timeout=None):
        cancelled.append(task)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    tap.create_task = create_task
    tap.cancel_task = cancel_task
    await tap.process_frame(TTSStoppedFrame(), FrameDirection.DOWNSTREAM)
    tail = tap._tail_task
    if tail is None or tail.done():
        raise RuntimeError("tail was not pending")

    quiet = _pcm(0)
    await _push_audio(tap, quiet)
    if cancelled or tap._tail_task is not tail or not tap._accept_audio:
        raise RuntimeError("quiet audio during the tail changed the lock")
    if tap._tail_bytes != len(quiet):
        raise RuntimeError(f"tail bytes were {tap._tail_bytes}")

    loud = _pcm(1000)
    await _push_audio(tap, loud)
    restarted = tap._tail_task
    if cancelled != [tail] or restarted is None or restarted is tail or restarted.done():
        raise RuntimeError(
            f"loud audio did not restart the tail cancelled={cancelled!r} restarted={restarted!r}"
        )
    if restarted.get_name() != "akapulu-avatar-tail":
        raise RuntimeError(f"restarted tail was {restarted.get_name()}")
    if tap._tail_bytes != len(loud):
        raise RuntimeError(f"tail bytes were {tap._tail_bytes}")
    if not tap._accept_audio or tap._turn != 1:
        raise RuntimeError("loud audio during the tail closed the gate or started a turn")
    speaking = [message for message in tap._audio.sent if isinstance(message, str)]
    if speaking != [json.dumps({"type": "speaking", "who": "bot", "state": "started"})]:
        raise RuntimeError(f"speaking was {speaking}")


def _close_frame(code: int, reason: str):
    from websockets.frames import Close

    return Close(code, reason)


class _ClosingAudio(_Audio):
    def __init__(self, exc: BaseException):
        super().__init__()
        self._exc = exc

    async def recv(self):
        raise self._exc


class _SendRaises(_Audio):
    def __init__(self, exc: BaseException):
        super().__init__()
        self._exc = exc

    async def send(self, message):
        self.sent.append(message)
        if isinstance(message, str) and '"stopped"' in message:
            raise self._exc


async def test_tail_normal_close_is_not_an_error() -> None:
    from websockets.exceptions import ConnectionClosedOK

    tap = _tap("wss://example.invalid/live")
    tap._audio = _SendRaises(
        ConnectionClosedOK(_close_frame(1001, "going away"), _close_frame(1001, "going away"), True)
    )
    tap._accept_audio = True
    tap._in_bot_turn = True
    tap._turn = 1
    tap._tail_secs = 0
    errors = []
    handle = logger.add(lambda message: errors.append(str(message)), level="ERROR")
    try:
        await tap._lock_after_tail()
    finally:
        logger.remove(handle)
    if errors:
        raise RuntimeError(f"tail normal close logged {errors}")
    if tap._accept_audio or tap._in_bot_turn:
        raise RuntimeError("tail normal close left the utterance open")
    stopped = [
        message
        for message in tap._audio.sent
        if isinstance(message, str) and '"stopped"' in message
    ]
    if len(stopped) != 1:
        raise RuntimeError(f"tail stopped send was {stopped}")


async def test_tail_unexpected_close_still_raises() -> None:
    from websockets.exceptions import ConnectionClosedError

    tap = _tap("wss://example.invalid/live")
    tap._audio = _SendRaises(ConnectionClosedError(_close_frame(1011, "internal"), None))
    tap._accept_audio = True
    tap._in_bot_turn = True
    tap._tail_secs = 0
    try:
        await tap._lock_after_tail()
    except ConnectionClosedError:
        pass
    else:
        raise RuntimeError("tail unexpected close was swallowed")
    if tap._in_bot_turn is not True:
        raise RuntimeError("tail unexpected close ended the turn")


async def test_close_cancels_the_tail() -> None:
    tap = _tap("wss://example.invalid/live")
    tap._audio = _Audio()
    tap._accept_audio = True
    tap._tail_secs = 30
    task = asyncio.create_task(tap._lock_after_tail())
    tap._tail_task = task
    cancelled = []

    async def cancel_task(pending, timeout=None):
        cancelled.append(pending)
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass

    tap.cancel_task = cancel_task
    await tap._close()
    if cancelled != [task] or not task.done():
        raise RuntimeError(f"close cancelled {cancelled!r} done={task.done()}")
    stopped = [
        message
        for message in tap._audio.sent
        if isinstance(message, str) and '"stopped"' in message
    ]
    if stopped:
        raise RuntimeError(f"closed tail sent {stopped}")


async def test_cleanup_closes() -> None:
    tap = _tap("wss://example.invalid/live")
    tap._audio = _Audio()
    from unittest.mock import AsyncMock, patch

    with patch.object(FrameProcessor, "cleanup", new=AsyncMock()) as super_cleanup:
        await tap.cleanup()
        if not tap._closed:
            raise RuntimeError("cleanup left the tap open")
        super_cleanup.assert_awaited_once()
        await tap.cleanup()
        if not tap._closed:
            raise RuntimeError("second cleanup reopened the tap")
        if super_cleanup.await_count != 2:
            raise RuntimeError("second cleanup skipped super")


async def test_setup_binds_pipeline_rate() -> None:
    from unittest.mock import AsyncMock, patch

    tap = AkapuluAvatar(session_id=SESSION)
    created = {}

    def create_task(coro, name=None):
        created["name"] = name
        coro.close()
        return object()

    tap.create_task = create_task
    with patch.object(FrameProcessor, "setup", new=AsyncMock()):
        await tap.setup(type("Setup", (), {"audio_out_sample_rate": 16000})())
    if tap._sample_rate != 16000:
        raise RuntimeError(f"setup bound {tap._sample_rate}")
    if created.get("name") != "akapulu-avatar-open":
        raise RuntimeError(f"setup open task was {created}")

    bad = AkapuluAvatar(session_id=SESSION)
    with patch.object(FrameProcessor, "setup", new=AsyncMock()):
        try:
            await bad.setup(type("Setup", (), {"audio_out_sample_rate": 24001})())
        except RuntimeError as exc:
            if "multiple of 25" not in str(exc):
                raise
        else:
            raise RuntimeError("setup accepted a non-multiple sample rate")


async def test_clean_audio_close_is_not_a_reader_error() -> None:
    from websockets.exceptions import ConnectionClosedOK

    tap = _tap("wss://example.invalid/live")
    tap._audio = _ClosingAudio(
        ConnectionClosedOK(_close_frame(1001, "going away"), _close_frame(1001, "going away"), True)
    )
    errors = []
    handle = logger.add(lambda message: errors.append(str(message)), level="ERROR")
    try:
        await tap._read_audio()
        if tap._reader_error is not None:
            raise RuntimeError(f"clean close was stored as {tap._reader_error!r}")
        await tap._close()
    finally:
        logger.remove(handle)
    if errors:
        raise RuntimeError(f"clean close logged {errors}")


async def test_unexpected_audio_close_is_a_reader_error() -> None:
    from websockets.exceptions import ConnectionClosedError

    exc = ConnectionClosedError(_close_frame(1011, "internal"), None)
    tap = _tap("wss://example.invalid/live")
    tap._audio = _ClosingAudio(exc)
    errors = []
    handle = logger.add(lambda message: errors.append(str(message)), level="ERROR")
    try:
        await tap._read_audio()
    finally:
        logger.remove(handle)
    if tap._reader_error is not exc:
        raise RuntimeError(f"unexpected close was stored as {tap._reader_error!r}")
    if not any("akapulu avatar reader failed" in message for message in errors):
        raise RuntimeError(f"unexpected close logged {errors}")


async def test_tts_stopped_still_schedules_the_tail() -> None:
    tap = _tap("wss://example.invalid/live")

    async def push(frame, direction=FrameDirection.DOWNSTREAM):
        return None

    tap.push_frame = push
    tap._audio = _Audio()
    await _push_audio(tap, _pcm(1000))
    scheduled = {}

    def create_task(coroutine, name=None):
        scheduled["name"] = name
        coroutine.close()
        return asyncio.get_running_loop().create_task(asyncio.sleep(0))

    tap.create_task = create_task
    await tap.process_frame(TTSStoppedFrame(), FrameDirection.DOWNSTREAM)
    if scheduled.get("name") != "akapulu-avatar-tail":
        raise RuntimeError(f"tail task was {scheduled}")
    if not tap._accept_audio:
        raise RuntimeError("TTSStopped locked the gate before the tail")


async def test_ingest_packet() -> None:
    tap = _tap("wss://example.invalid/live")
    audio = _Audio()
    tap._audio = audio
    pcm = b"\x02\x00" * 960
    await tap._ingest(OutputAudioRawFrame(audio=pcm, sample_rate=24000, num_channels=1))
    if len(audio.sent) != 1 or not isinstance(audio.sent[0], bytes):
        raise RuntimeError(f"ingest was {audio.sent!r}")
    packet = audio.sent[0]
    if packet[:1] != b"A":
        raise RuntimeError(f"ingest kind was {packet[:1]!r}")
    seq = struct.unpack(">I", packet[1:5])[0]
    if seq != 0 or packet[5:] != pcm:
        raise RuntimeError(f"ingest packet seq={seq} bytes={len(packet)}")


def _kwargs(**overrides):
    values = dict(
        session_id=SESSION,
    )
    values.update(overrides)
    return values


def test_constructor_requires_session() -> None:
    try:
        AkapuluAvatar(**_kwargs(session_id=""))
    except RuntimeError:
        pass
    else:
        raise RuntimeError("empty session id was accepted")
    try:
        AkapuluAvatar(**_kwargs(session_id="not-a-uuid"))
    except RuntimeError:
        pass
    else:
        raise RuntimeError("non-uuid session id was accepted")
    tap = AkapuluAvatar(**_kwargs())
    if tap._tail_secs != 0:
        raise RuntimeError(f"default tail was {tap._tail_secs}")
    explicit = AkapuluAvatar(**_kwargs(tail_secs=1.5))
    if explicit._tail_secs != 1.5:
        raise RuntimeError(f"explicit tail was {explicit._tail_secs}")
    try:
        AkapuluAvatar(**_kwargs(tail_secs=-0.1))
    except RuntimeError:
        pass
    else:
        raise RuntimeError("negative tail was accepted")
    try:
        AkapuluAvatar(sample_rate=24000, session_id=SESSION)
    except TypeError:
        pass
    else:
        raise RuntimeError("constructor still took sample_rate")
    if tap._debug_file is not None:
        raise RuntimeError("debug file opened while debug was false")
    try:
        AkapuluAvatar(**_kwargs(debug_path="trace"))
    except RuntimeError as exc:
        if "debug is false" not in str(exc):
            raise
    else:
        raise RuntimeError("debug_path was accepted while debug was false")
    try:
        AkapuluAvatar(**_kwargs(debug=True))
    except RuntimeError as exc:
        if "debug_path is missing" not in str(exc):
            raise
    else:
        raise RuntimeError("debug true was accepted without debug_path")
    try:
        AkapuluAvatar(**_kwargs(debug="yes", debug_path="trace"))
    except RuntimeError as exc:
        if "debug is" not in str(exc):
            raise
    else:
        raise RuntimeError("non-bool debug was accepted")


def _debug_rows(path: Path) -> list[dict]:
    text = (path / "sends.jsonl").read_text()
    if text == "":
        return []
    return [json.loads(line) for line in text.splitlines()]


def test_debug_path_must_be_a_fresh_directory() -> None:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        file_path = root / "not-a-dir"
        file_path.write_text("x")
        try:
            AkapuluAvatar(session_id=SESSION, debug=True, debug_path=str(file_path))
        except RuntimeError as exc:
            if "not a directory" not in str(exc):
                raise
        else:
            raise RuntimeError("a file debug_path was accepted")
        trace = root / "trace"
        first = AkapuluAvatar(session_id=SESSION, debug=True, debug_path=str(trace))
        first._close_debug()
        try:
            AkapuluAvatar(session_id=SESSION, debug=True, debug_path=str(trace))
        except RuntimeError as exc:
            if "sends.jsonl" not in str(exc):
                raise
        else:
            raise RuntimeError("an existing debug directory was accepted")


async def test_debug_records_each_wire_send() -> None:
    with tempfile.TemporaryDirectory() as raw:
        trace = Path(raw) / "trace"
        tap = AkapuluAvatar(session_id=SESSION, debug=True, debug_path=str(trace))
        tap._bind(24000)
        try:
            queued = json.dumps({"type": "speaking", "who": "user", "state": "started"})
            await tap._send_or_queue(queued)
            if _debug_rows(trace):
                raise RuntimeError("a queued send was written before the socket existed")
            if tap._pending_audio != [queued]:
                raise RuntimeError(f"pending audio was {tap._pending_audio!r}")
            tap._audio = _Audio()
            loud = _pcm(1000)
            await tap._ingest(
                OutputAudioRawFrame(audio=loud, sample_rate=24000, num_channels=1)
            )
            await tap._send_speaking("bot", "stopped")
            rows = _debug_rows(trace)
            if [row["kind"] for row in rows] != ["audio", "speaking"]:
                raise RuntimeError(f"debug rows were {rows}")
            if rows[0]["gap_ms"] is not None or rows[0]["seq"] != 0 or rows[0]["bytes"] != len(loud):
                raise RuntimeError(f"audio row was {rows[0]}")
            if not isinstance(rows[1]["gap_ms"], float) or rows[1]["gap_ms"] < 0:
                raise RuntimeError(f"speaking gap was {rows[1]['gap_ms']!r}")
            if rows[1]["body"] != {"type": "speaking", "who": "bot", "state": "stopped"}:
                raise RuntimeError(f"speaking body was {rows[1]['body']}")
            pcm_path = trace / rows[0]["pcm"]
            if pcm_path.read_bytes() != loud:
                raise RuntimeError("debug pcm did not match the sent frame")
            if tap._audio.sent[0][5:] != loud:
                raise RuntimeError("socket audio did not match the debug pcm")
        finally:
            await tap._close()


async def test_debug_records_the_format_and_the_flush() -> None:
    import pipecat_akapulu.avatar as avatar_mod

    class _Sock:
        def __init__(self):
            self.sent = []

        async def send(self, message):
            self.sent.append(message)

        async def close(self):
            return None

    sock = _Sock()

    async def fake_connect(url, **kwargs):
        if not url.startswith("wss://"):
            raise RuntimeError(f"connect url was {url}")
        return sock

    with tempfile.TemporaryDirectory() as raw:
        trace = Path(raw) / "trace"
        tap = AkapuluAvatar(session_id=SESSION, debug=True, debug_path=str(trace))
        tap._bind(24000)
        loud = _pcm(800)
        packet = b"A" + struct.pack(">I", 3) + loud
        tap._pending_audio.append(packet)

        def create_task(coro, name=None):
            coro.close()
            return None

        tap.create_task = create_task
        original = avatar_mod.connect
        avatar_mod.connect = fake_connect
        try:
            await tap._take_streams({"audio": "wss://example.invalid/v1/audio"})
            rows = _debug_rows(trace)
            if [row["kind"] for row in rows] != ["format", "audio"]:
                raise RuntimeError(f"flush rows were {rows}")
            if rows[0]["body"] != {"type": "format", "sample_rate": 24000}:
                raise RuntimeError(f"format body was {rows[0]['body']}")
            if rows[1]["seq"] != 3 or (trace / rows[1]["pcm"]).read_bytes() != loud:
                raise RuntimeError(f"flushed audio row was {rows[1]}")
            if sock.sent != [
                json.dumps({"type": "format", "sample_rate": 24000}),
                packet,
            ]:
                raise RuntimeError(f"socket flush was {sock.sent!r}")
        finally:
            avatar_mod.connect = original
            await tap._close()


def test_bind_requires_multiple_of_25() -> None:
    tap = AkapuluAvatar(session_id=SESSION)
    tap._bind(24000)
    if tap._sample_rate != 24000:
        raise RuntimeError(f"bound rate was {tap._sample_rate}")
    try:
        tap._bind(24001)
    except RuntimeError as exc:
        if "multiple of 25" not in str(exc):
            raise
    else:
        raise RuntimeError("non-multiple sample rate was accepted")
    try:
        tap._bind(0)
    except RuntimeError:
        pass
    else:
        raise RuntimeError("zero sample rate was accepted")


def _with_origin_env(value: str | None):
    previous = os.environ.get("AKAPULU_API_URL")
    internal = os.environ.get("INTERNAL_DJANGO_API_URL")
    environment = os.environ.get("MODAL_ENVIRONMENT")
    debug = os.environ.get("DEBUG")
    if value is None:
        os.environ.pop("AKAPULU_API_URL", None)
    else:
        os.environ["AKAPULU_API_URL"] = value
    os.environ["INTERNAL_DJANGO_API_URL"] = "https://internal.example"
    os.environ["MODAL_ENVIRONMENT"] = "dev"
    os.environ["DEBUG"] = "true"
    return previous, internal, environment, debug


def _restore_origin_env(previous, internal, environment, debug) -> None:
    if previous is None:
        os.environ.pop("AKAPULU_API_URL", None)
    else:
        os.environ["AKAPULU_API_URL"] = previous
    for key, saved in (
        ("INTERNAL_DJANGO_API_URL", internal),
        ("MODAL_ENVIRONMENT", environment),
        ("DEBUG", debug),
    ):
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved


def test_django_origin() -> None:
    saved = _with_origin_env(None)
    try:
        if _django_origin() != _PROD_DJANGO_ORIGIN:
            raise RuntimeError("unset AKAPULU_API_URL did not use the production origin")
        tap = AkapuluAvatar(session_id=SESSION)
        url = tap._status_url()
        suffix = f"/api/live-sessions/{SESSION}/status/"
        if url != _PROD_DJANGO_ORIGIN + suffix:
            raise RuntimeError("worker url did not use the production origin")
    finally:
        _restore_origin_env(*saved)

    saved = _with_origin_env("https://example.test/")
    try:
        if _django_origin() != "https://example.test":
            raise RuntimeError("AKAPULU_API_URL override was not used")
        tap = AkapuluAvatar(session_id=SESSION)
        url = tap._status_url()
        expected = f"https://example.test/api/live-sessions/{SESSION}/status/"
        if url != expected:
            raise RuntimeError("worker url did not use AKAPULU_API_URL")
    finally:
        _restore_origin_env(*saved)

    saved = _with_origin_env("http://127.0.0.1:8000/")
    try:
        if _django_origin() != "http://127.0.0.1:8000":
            raise RuntimeError("http origin was rejected")
    finally:
        _restore_origin_env(*saved)

    for bad in ("", "   ", "ftp://example.test", "example.test"):
        saved = _with_origin_env(bad)
        try:
            try:
                _django_origin()
            except RuntimeError as exc:
                if str(exc) != "AKAPULU_API_URL must be an http(s) origin":
                    raise
            else:
                raise RuntimeError("bad AKAPULU_API_URL was accepted")
        finally:
            _restore_origin_env(*saved)


async def test_poll_status_before_the_url() -> None:
    tap = _tap()
    script = [
        _poll(),
        _poll(
            latest_update_text="Your avatar's shaking off the cobwebs — taking a bit longer than usual, hang tight",
            assistant_startup_delayed=True,
        ),
        _poll(worker_wss_url="wss://example.invalid/live", completion_percent=50),
    ]

    def fetch():
        if not script:
            raise RuntimeError("worker poll was called again")
        return script.pop(0)

    tap._fetch_worker_poll = fetch
    url = await tap._wait_worker_wss()
    if url != "wss://example.invalid/live":
        raise RuntimeError(f"worker url was {url}")
    texts = [frame.data["latest_update_text"] for frame in tap._queued_status]
    if texts != [
        "Creating call room...",
        "Your avatar's shaking off the cobwebs — taking a bit longer than usual, hang tight",
        "Creating call room...",
    ]:
        raise RuntimeError(f"poll status was {texts}")
    if tap._queued_status[-1].data["completion_percent"] != 50:
        raise RuntimeError(f"last poll percent was {tap._queued_status[-1].data}")


def test_poll_does_not_rewind_socket_status() -> None:
    tap = _tap()
    tap._status_percent = 85
    behind = tap._poll_status_body(
        _poll(latest_update_text="Initializing media services...", completion_percent=50)
    )
    if behind is not None:
        raise RuntimeError(f"older milestone was pushed: {behind}")
    delayed = tap._poll_status_body(
        _poll(
            latest_update_text="Your avatar's taking a little longer to wake up — hang tight",
            completion_percent=50,
            assistant_startup_delayed=True,
        )
    )
    if delayed != {
        "type": "status",
        "call_is_ready": False,
        "latest_update_text": "Your avatar's taking a little longer to wake up — hang tight",
        "completion_percent": 85,
    }:
        raise RuntimeError(f"delay status was {delayed}")
    try:
        tap._poll_status_body({"worker_wss_url": None})
    except RuntimeError as exc:
        if "call_is_ready" not in str(exc):
            raise
    else:
        raise RuntimeError("poll status without fields was accepted")
    try:
        tap._worker_wss_from_poll(_poll(worker_wss_url="https://nope"))
    except RuntimeError as exc:
        if "wss://" not in str(exc):
            raise
    else:
        raise RuntimeError("https worker url was accepted")
    if tap._worker_wss_from_poll(_poll()) is not None:
        raise RuntimeError("null worker url was treated as ready")


async def test_poll_continues_after_the_url() -> None:
    tap = _tap()
    script = [
        _poll(worker_wss_url="wss://example.invalid/live", completion_percent=50),
        _poll(
            worker_wss_url="wss://example.invalid/live",
            latest_update_text="Your avatar's shaking off the cobwebs — taking a bit longer than usual, hang tight",
            assistant_startup_delayed=True,
        ),
        _poll(
            worker_wss_url="wss://example.invalid/live",
            call_is_ready=True,
            latest_update_text="Media stream active...",
            completion_percent=100,
        ),
    ]

    def fetch():
        if not script:
            raise RuntimeError("updates poll continued after ready")
        return script.pop(0)

    tap._fetch_worker_poll = fetch
    url = await tap._wait_worker_wss()
    if url != "wss://example.invalid/live":
        raise RuntimeError(f"worker url was {url}")
    await asyncio.wait_for(tap._poll_updates(), 3)
    texts = [frame.data["latest_update_text"] for frame in tap._queued_status]
    if texts != [
        "Creating call room...",
        "Your avatar's shaking off the cobwebs — taking a bit longer than usual, hang tight",
        "Media stream active...",
    ]:
        raise RuntimeError(f"continued poll status was {texts}")
    if tap._queued_status[1].data["completion_percent"] != 50:
        raise RuntimeError("delay percent was rewound")
    if tap._queued_status[-1].data["call_is_ready"] is not True:
        raise RuntimeError("ready poll was not pushed")
    if script:
        raise RuntimeError(f"poll stopped early: {script}")


def test_engine_joined_post() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            self.server.seen.append((self.path, body))
            code = self.server.status_code
            self.send_response(code)
            self.end_headers()
            if code != 200:
                self.wfile.write(b"nope")

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.seen = []
    server.status_code = 200
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    saved = _with_origin_env(f"http://{host}:{port}")
    try:
        tap = AkapuluAvatar(session_id=SESSION)
        tap._post_engine_joined()
        expected = (
            f"/api/live-sessions/{SESSION}/status/",
            b"{}",
        )
        if server.seen != [expected]:
            raise RuntimeError("engine joined request was wrong")
        server.status_code = 500
        server.seen.clear()
        try:
            tap._post_engine_joined()
        except RuntimeError as exc:
            if "500" not in str(exc):
                raise
        else:
            raise RuntimeError("engine joined 500 was accepted")
    finally:
        _restore_origin_env(*saved)
        server.shutdown()
        thread.join(timeout=5)


async def main() -> None:
    test_constructor_requires_session()
    test_debug_path_must_be_a_fresh_directory()
    await test_debug_records_each_wire_send()
    await test_debug_records_the_format_and_the_flush()
    test_bind_requires_multiple_of_25()
    await test_setup_binds_pipeline_rate()
    await test_cleanup_closes()
    test_django_origin()
    test_engine_joined_post()
    test_poll_does_not_rewind_socket_status()
    await test_poll_status_before_the_url()
    await test_poll_continues_after_the_url()
    await test_video_url_is_rejected()
    await test_daily_message()
    await test_idle_opens_on_loud_audio_not_tts_started()
    await test_threshold_sample_stays_idle()
    await test_loud_audio_during_tail_restarts_the_lock()
    await test_tail_normal_close_is_not_an_error()
    await test_tail_unexpected_close_still_raises()
    await test_close_cancels_the_tail()
    await test_clean_audio_close_is_not_a_reader_error()
    await test_unexpected_audio_close_is_a_reader_error()
    await test_tts_stopped_still_schedules_the_tail()
    await test_ingest_packet()
    print("AKAPULU_AVATAR_ROOM_OK")


if __name__ == "__main__":
    asyncio.run(main())
