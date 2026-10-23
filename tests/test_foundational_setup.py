"""Room and session helpers in the foundational example. No network."""

import importlib.util
from pathlib import Path


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


if __name__ == "__main__":
    test_api_origin()
    test_live_session_body_and_join_url()
    test_microphone_subscriptions()
    print("ok")
