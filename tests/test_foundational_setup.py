"""Room and session helpers in the foundational example. No network."""

import asyncio
import importlib.util
from pathlib import Path

from pipecat.frames.frames import STTUpdateSettingsFrame, TranscriptionFrame, TTSUpdateSettingsFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transcriptions.language import Language


def _load():
    path = Path(__file__).resolve().parents[1] / "examples" / "foundational.py"
    spec = importlib.util.spec_from_file_location("foundational", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("foundational example is missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_api_origin():
    example = _load()
    assert example.api_origin() == "https://akapulu.com"
    # harness override
    import os
    previous = os.environ.get("AKAPULU_API_URL")
    try:
        os.environ["AKAPULU_API_URL"] = "https://akapulu.com/"
        assert example.api_origin() == "https://akapulu.com"
        os.environ["AKAPULU_API_URL"] = "   "
        try:
            example.api_origin()
        except RuntimeError as exc:
            assert "AKAPULU_API_URL" in str(exc)
        else:
            raise AssertionError("blank AKAPULU_API_URL was accepted")
    finally:
        if previous is None:
            os.environ.pop("AKAPULU_API_URL", None)
        else:
            os.environ["AKAPULU_API_URL"] = previous


def test_live_session_body_and_join_url():
    example = _load()
    body = example.live_session_body(
        room_url="https://example.daily.co/room",
        avatar_token="avatar-token",
    )
    assert body == {
        "avatar_id": "1f777f64-3758-4a7d-9cbc-c64ae654f7d1",
        "show_ai_label": False,
        "room_url": "https://example.daily.co/room",
        "avatar_token": "avatar-token",
    }
    assert (
        example.join_url("https://example.daily.co/room", "browser-token")
        == "https://example.daily.co/room?t=browser-token"
    )
    try:
        example.join_url("https://example.daily.co/room?x=1", "browser-token")
    except RuntimeError as exc:
        assert "room url" in str(exc)
    else:
        raise AssertionError("join url accepted a query")


def test_voice_stack():
    example = _load()
    assert example.SPEAKER_MODEL == "gpt-5.4-mini"
    assert example.SONIC_MODEL == "sonic-3.6"
    assert example.SONIC_VOICE_ID == "ec1e269e-9ca0-402f-8a18-58e0e022355a"
    assert example.FLUX_MODEL == "flux-general-multi"
    assert example.EAGER_EOT_THRESHOLD == 0.5
    assert example.Language.EN in example.FLUX_LANGUAGES
    assert example.Language.ZH not in example.FLUX_LANGUAGES


def test_microphone_subscriptions():
    example = _load()
    settings = example.microphone_subscriptions(
        {
            "local": {"info": {"isLocal": True, "userName": "Assistant"}},
            "browser": {"info": {"userName": "You"}},
            "avatar": {"info": {"userName": "Avatar"}},
        }
    )
    assert settings["browser"]["media"]["microphone"] == "subscribed"
    assert settings["avatar"]["media"]["microphone"] == "unsubscribed"
    assert "local" not in settings


def test_spoken_language_locks_and_rejects_a_missing_language():
    example = _load()

    class Recording(example.SpokenLanguage):
        def __init__(self):
            super().__init__()
            self.pushed = []

        async def push_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
            self.pushed.append((frame, direction))

    processor = Recording()
    missing = TranscriptionFrame("hello", "browser", "2026-10-25T00:00:00Z", None, finalized=True)
    try:
        asyncio.run(processor.process_frame(missing, FrameDirection.DOWNSTREAM))
    except RuntimeError as exc:
        assert "no language" in str(exc)
    else:
        raise AssertionError("missing language was accepted")
    assert processor.pushed == []

    processor = Recording()
    turn = TranscriptionFrame(
        "hello", "browser", "2026-10-25T00:00:00Z", Language.JA, finalized=True
    )
    asyncio.run(processor.process_frame(turn, FrameDirection.DOWNSTREAM))
    assert isinstance(processor.pushed[0][0], STTUpdateSettingsFrame)
    assert processor.pushed[0][1] == FrameDirection.UPSTREAM
    assert processor.pushed[0][0].delta.language_hints == [Language.JA]
    assert isinstance(processor.pushed[1][0], TTSUpdateSettingsFrame)
    assert processor.pushed[1][0].delta.language == Language.JA
    assert processor.pushed[2] == (turn, FrameDirection.DOWNSTREAM)


if __name__ == "__main__":
    test_api_origin()
    test_live_session_body_and_join_url()
    test_voice_stack()
    test_microphone_subscriptions()
    test_spoken_language_locks_and_rejects_a_missing_language()
    print("ok")
