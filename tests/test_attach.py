"""attach() and akapulu_daily_params. Run: python tests/test_attach.py"""

import asyncio

from pipecat.frames.frames import UserImageRawFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.utils.base_object import BaseObject

from pipecat_akapulu import AkapuluAvatar, akapulu_daily_params
from pipecat_akapulu.avatar import _AvatarVideoIn


SESSION = "11111111-1111-1111-1111-111111111111"


class _FakeTransport(BaseObject):
    def __init__(self):
        super().__init__()
        self._register_event_handler("on_participant_joined", sync=True)
        self.subscriptions = []
        self.captured = []

    async def update_subscriptions(self, *, participant_settings):
        self.subscriptions.append(participant_settings)
        return None

    async def capture_participant_video(self, participant_id, framerate=None):
        self.captured.append((participant_id, framerate))


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
    video_in = tap.attach(transport)
    assert isinstance(video_in, _AvatarVideoIn)
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
        assert transport.captured == []
        await transport._call_event_handler(
            "on_participant_joined",
            {"id": "avatar", "info": {"userName": "Avatar"}},
        )
        assert transport.subscriptions == [
            {"avatar": {"media": {"microphone": "unsubscribed"}}}
        ]
        assert transport.captured == [("avatar", None)]
        await video_in.process_frame(
            UserImageRawFrame(user_id="avatar", image=b"x", size=(1, 1), format="RGB"),
            FrameDirection.DOWNSTREAM,
        )
        assert tap._video_frames == 1

    asyncio.run(run())


if __name__ == "__main__":
    test_akapulu_daily_params()
    test_attach_wires_avatar_only()
    print("ok")
