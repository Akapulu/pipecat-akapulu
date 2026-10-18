"""Daily handshake for AkapuluAvatar. Run from the repo root: python tests/test_avatar.py"""

import asyncio
import json
import os
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    OutputAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from websockets.asyncio.server import serve

from pipecat_akapulu.avatar import AkapuluAvatar, _PROD_DJANGO_ORIGIN, _django_origin


SESSION = "11111111-1111-1111-1111-111111111111"


def _tap(connect_url: str | None = None) -> AkapuluAvatar:
    tap = AkapuluAvatar(
        sample_rate=24000,
        session_id=SESSION,
    )
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


async def test_loud_audio_during_tail_cancels_the_lock() -> None:
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

    await _push_audio(tap, _pcm(1000))
    if cancelled != [tail] or tap._tail_task is not None or tap._tail_bytes != 0:
        raise RuntimeError("loud audio did not cancel the pending lock")
    if not tap._accept_audio or tap._turn != 1:
        raise RuntimeError("loud audio during the tail closed the gate or started a turn")
    speaking = [message for message in tap._audio.sent if isinstance(message, str)]
    if speaking != [json.dumps({"type": "speaking", "who": "bot", "state": "started"})]:
        raise RuntimeError(f"speaking was {speaking}")


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
        sample_rate=24000,
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
    if tap._tail_secs != 0.6:
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
        tap = AkapuluAvatar(sample_rate=24000, session_id=SESSION)
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
        tap = AkapuluAvatar(sample_rate=24000, session_id=SESSION)
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
        tap = AkapuluAvatar(sample_rate=24000, session_id=SESSION)
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
    test_django_origin()
    test_engine_joined_post()
    test_poll_does_not_rewind_socket_status()
    await test_poll_status_before_the_url()
    await test_poll_continues_after_the_url()
    await test_video_url_is_rejected()
    await test_daily_message()
    await test_idle_opens_on_loud_audio_not_tts_started()
    await test_threshold_sample_stays_idle()
    await test_loud_audio_during_tail_cancels_the_lock()
    await test_tts_stopped_still_schedules_the_tail()
    await test_ingest_packet()
    print("AKAPULU_AVATAR_ROOM_OK")


if __name__ == "__main__":
    asyncio.run(main())
