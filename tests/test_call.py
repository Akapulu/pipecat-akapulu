"""create_akapulu_call against a local Daily and live-session stand-in."""

import asyncio
import os
import time
import uuid

import aiohttp
from aiohttp import web

from pipecat_akapulu.call import AVATAR_NAME, BROWSER_NAME, create_akapulu_call

AVATAR_ID = "22222222-2222-2222-2222-222222222222"
SESSION_ID = "11111111-1111-1111-1111-111111111111"
ROOM_URL = "https://example.daily.co/room-name"


def _state() -> dict:
    return {"rooms": [], "tokens": [], "sessions": [], "live_status": 200, "live_body": None}


async def _rooms(request: web.Request) -> web.Response:
    body = await request.json()
    request.app["state"]["rooms"].append(
        {"authorization": request.headers.get("Authorization"), "body": body}
    )
    return web.json_response(
        {
            "id": "room-id",
            "name": "room-name",
            "api_created": True,
            "privacy": "private",
            "url": ROOM_URL,
            "created_at": "2026-10-27T00:00:00.000Z",
            "config": {},
        }
    )


async def _tokens(request: web.Request) -> web.Response:
    body = await request.json()
    request.app["state"]["tokens"].append(body)
    props = body["properties"]
    if props.get("is_owner") is True:
        token = "bot-token"
    elif props.get("user_name") == BROWSER_NAME:
        token = "browser-token"
    elif props.get("user_name") == AVATAR_NAME:
        token = "avatar-token"
    else:
        raise web.HTTPBadRequest(text="unknown token")
    return web.json_response({"token": token})


async def _live(request: web.Request) -> web.Response:
    body = await request.json()
    state = request.app["state"]
    state["sessions"].append(
        {"authorization": request.headers.get("Authorization"), "body": body}
    )
    if state["live_status"] != 200:
        return web.Response(status=state["live_status"], text='{"error_code":"nope"}')
    if state["live_body"] is not None:
        return web.json_response(state["live_body"])
    return web.json_response({"session_id": SESSION_ID})


async def _serve(state: dict) -> tuple[web.AppRunner, str]:
    app = web.Application()
    app["state"] = state
    app.router.add_post("/rooms", _rooms)
    app.router.add_post("/meeting-tokens", _tokens)
    app.router.add_post("/api/live-sessions/", _live)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"


def _restore(previous: str | None) -> None:
    if previous is None:
        os.environ.pop("AKAPULU_API_URL", None)
    else:
        os.environ["AKAPULU_API_URL"] = previous


async def _call(state: dict, **overrides):
    runner, origin = await _serve(state)
    previous = os.environ.get("AKAPULU_API_URL")
    os.environ["AKAPULU_API_URL"] = origin
    kwargs = {
        "daily_api_key": "daily-key",
        "akapulu_api_key": "akapulu-key",
        "avatar_id": AVATAR_ID,
        "show_ai_label": True,
        "room_exp_secs": 7200,
        "daily_api_url": origin,
    }
    kwargs.update(overrides)
    try:
        async with aiohttp.ClientSession() as session:
            kwargs["session"] = session
            return await create_akapulu_call(**kwargs)
    finally:
        _restore(previous)
        await runner.cleanup()


async def test_creates_the_room_and_posts_the_session() -> None:
    state = _state()
    started = time.time()
    room_url, bot_token, browser_token, session_id = await _call(state)
    if (room_url, bot_token, browser_token, session_id) != (
        ROOM_URL,
        "bot-token",
        "browser-token",
        SESSION_ID,
    ):
        raise RuntimeError("create_akapulu_call returned the wrong values")
    if "avatar-token" in (room_url, bot_token, browser_token, session_id):
        raise RuntimeError("create_akapulu_call returned the worker token")
    if len(state["rooms"]) != 1 or len(state["tokens"]) != 3 or len(state["sessions"]) != 1:
        raise RuntimeError("create_akapulu_call did not make one room, three tokens, and one session")
    room = state["rooms"][0]
    if room["authorization"] != "Bearer daily-key":
        raise RuntimeError("Daily room request used the wrong key")
    if room["body"]["privacy"] != "private":
        raise RuntimeError("Daily room was not private")
    props = room["body"]["properties"]
    if props["max_participants"] != 3:
        raise RuntimeError("Daily room did not cap at three participants")
    if props["eject_at_room_exp"] is not True or props["enable_chat"] is not False:
        raise RuntimeError("Daily room properties were wrong")
    if props["enable_prejoin_ui"] is not False:
        raise RuntimeError("Daily room left the prejoin UI on")
    if abs(props["exp"] - (started + 7200)) > 30:
        raise RuntimeError("Daily room expiry was not room_exp_secs ahead")
    by_name = {}
    for body in state["tokens"]:
        token_props = body["properties"]
        by_name[token_props.get("user_name")] = token_props
    if by_name[None]["is_owner"] is not True:
        raise RuntimeError("bot token was not an owner")
    if by_name[BROWSER_NAME]["is_owner"] is not False or by_name[BROWSER_NAME]["user_name"] != "You":
        raise RuntimeError("browser token was not named You")
    avatar = by_name[AVATAR_NAME]
    if avatar["user_name"] != "Avatar" or avatar["is_owner"] is not False:
        raise RuntimeError("worker token was not named Avatar")
    if avatar["enable_screenshare"] is not False or avatar["start_audio_off"] is not False:
        raise RuntimeError("worker token muted itself")
    if avatar["permissions"] != {
        "hasPresence": True,
        "canSend": ["video", "audio"],
        "canAdmin": False,
    }:
        raise RuntimeError("worker token permissions were wrong")
    posted = state["sessions"][0]
    if posted["authorization"] != "Bearer akapulu-key":
        raise RuntimeError("live session used the wrong key")
    if posted["body"] != {
        "avatar_id": AVATAR_ID,
        "show_ai_label": True,
        "room_url": ROOM_URL,
        "avatar_token": "avatar-token",
    }:
        raise RuntimeError("live session body was wrong")


async def test_live_session_error_includes_the_body() -> None:
    state = _state()
    state["live_status"] = 402
    try:
        await _call(state)
    except RuntimeError as exc:
        text = str(exc)
        if "402" not in text or "nope" not in text:
            raise RuntimeError(f"live session error hid the response: {text}") from exc
    else:
        raise RuntimeError("live session 402 was accepted")


async def test_rejects_a_bad_session_id() -> None:
    state = _state()
    state["live_body"] = {"session_id": "not-a-uuid"}
    try:
        await _call(state)
    except RuntimeError as exc:
        if "session_id" not in str(exc):
            raise RuntimeError(f"bad session id was the wrong error: {exc}") from exc
    else:
        raise RuntimeError("a non-UUID session id was accepted")


async def test_rejects_bad_arguments() -> None:
    async with aiohttp.ClientSession() as session:
        base = {
            "daily_api_key": "daily-key",
            "akapulu_api_key": "akapulu-key",
            "avatar_id": AVATAR_ID,
            "show_ai_label": False,
            "session": session,
            "room_exp_secs": 60,
        }
        cases = [
            {"daily_api_key": "  ", "needle": "daily api key"},
            {"akapulu_api_key": "", "needle": "akapulu api key"},
            {"avatar_id": "clara", "needle": "avatar_id"},
            {"show_ai_label": "yes", "needle": "show_ai_label"},
            {"session": None, "needle": "session"},
            {"room_exp_secs": 0, "needle": "room_exp_secs"},
            {"room_exp_secs": True, "needle": "room_exp_secs"},
            {"daily_api_url": "ftp://daily.example", "needle": "daily api url"},
        ]
        for update in cases:
            kwargs = dict(base)
            needle = update["needle"]
            kwargs.update(update)
            kwargs.pop("needle")
            try:
                await create_akapulu_call(**kwargs)
            except RuntimeError as exc:
                if needle not in str(exc):
                    raise RuntimeError(f"{update} raised {exc}") from exc
            else:
                raise RuntimeError(f"{update} was accepted")


def main() -> None:
    asyncio.run(test_rejects_bad_arguments())
    asyncio.run(test_creates_the_room_and_posts_the_session())
    asyncio.run(test_live_session_error_includes_the_body())
    asyncio.run(test_rejects_a_bad_session_id())
    uuid.UUID(SESSION_ID)
    print("AKAPULU_CALL_OK")


if __name__ == "__main__":
    main()
