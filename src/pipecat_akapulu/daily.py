"""Daily transport defaults for an Akapulu avatar voice bot."""

from pipecat.transports.daily.transport import DailyParams


def akapulu_daily_params(**overrides) -> DailyParams:
    """DailyParams for a voice bot that does not publish Avatar audio or video.

    The Akapulu Labs worker joins as Avatar and publishes both. Voice-bot
    audio out and video out stay off. Video in stays on so ``attach`` can
    count Avatar camera frames for ``call_is_ready``. Pass explicit keyword
    overrides when a call needs different DailyParams fields.
    """
    params = {
        "audio_in_enabled": True,
        "audio_out_enabled": False,
        "video_in_enabled": True,
        "video_out_enabled": False,
    }
    params.update(overrides)
    return DailyParams(**params)
