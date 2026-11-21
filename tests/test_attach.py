"""attach() and akapulu_daily_params. Run: python tests/test_attach.py"""

import asyncio

from pipecat.utils.base_object import BaseObject

from pipecat_akapulu import AkapuluAvatar, akapulu_daily_params


SESSION = "11111111-1111-1111-1111-111111111111"


class _FakeClient:
    def __init__(self):
        self.captured = []

    async def capture_participant_video(
        self, participant_id, callback, framerate=None, video_source="camera", color_format="RGB"
    ):
        self.captured.append((participant_id, callback, framerate, video_source, color_format))
        await callback(participant_id, object(), video_source)


class _FakeTransport(BaseObject):
    def __init__(self):
        super().__init__()
        self._register_event_handler("on_participant_joined", sync=True)
        self._client = _FakeClient()
        self.subscriptions = []

    async def update_subscriptions(self, *, participant_settings):
        self.subscriptions.append(participant_settings)
        return None


def test_akapulu_daily_params():
    params = akapulu_daily_params()
    assert params.audio_in_enabled is True
    assert params.audio_out_enabled is False
    assert params.video_in_enabled is True
    assert params.video_out_enabled is False
    params = akapulu_daily_params(audio_in_user_tracks=True)
    assert params.audio_in_user_tracks is True
    assert params.audio_out_enabled is False


def test_attach_wires_avatar_only():
    transport = _FakeTransport()
    tap = AkapuluAvatar(session_id=SESSION)
    assert tap.attach(transport) is None
    try:
        tap.attach(transport)
    except RuntimeError as exc:
        assert "already attached" in str(exc)
    else:
        raise AssertionError("second attach was accepted")

    async def run():
        await transport._call_event_handler(
            "on_participant_joined",
            {"id": "browser", "info": {"userName": "You"}},
        )
        assert transport.subscriptions == []
        assert transport._client.captured == []
        await transport._call_event_handler(
            "on_participant_joined",
            {"id": "avatar", "info": {"userName": "Avatar"}},
        )
        assert transport.subscriptions == [
            {"avatar": {"media": {"microphone": "unsubscribed"}}}
        ]
        assert len(transport._client.captured) == 1
        assert transport._client.captured[0][0] == "avatar"
        assert tap._video_frames == 1

    asyncio.run(run())


if __name__ == "__main__":
    test_akapulu_daily_params()
    test_attach_wires_avatar_only()
    print("ok")
