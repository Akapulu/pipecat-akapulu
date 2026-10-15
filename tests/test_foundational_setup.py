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
    assert example.api_origin(None) == "https://akapulu.com"
    assert example.api_origin("https://akapulu-dev.fly.dev/") == "https://akapulu-dev.fly.dev"
    try:
        example.api_origin("   ")
    except RuntimeError as exc:
        assert "AKAPULU_API_URL" in str(exc)
    else:
        raise AssertionError("blank AKAPULU_API_URL was accepted")


def test_show_ai_label():
    example = _load()
    assert example.show_ai_label("true") is True
    assert example.show_ai_label("false") is False
    try:
        example.show_ai_label("yes")
    except RuntimeError as exc:
        assert "AKAPULU_SHOW_AI_LABEL" in str(exc)
    else:
        raise AssertionError("show_ai_label accepted yes")


def test_live_session_body_and_join_url():
    example = _load()
    avatar = "11111111-1111-1111-1111-111111111111"
    body = example.live_session_body(
        avatar=avatar,
        label=True,
        room_url="https://example.daily.co/room",
        avatar_token="avatar-token",
    )
    assert body == {
        "avatar_id": avatar,
        "show_ai_label": True,
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
    test_show_ai_label()
    test_live_session_body_and_join_url()
    test_microphone_subscriptions()
    print("ok")
