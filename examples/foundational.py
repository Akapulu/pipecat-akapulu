"""Daily bot that publishes an Akapulu avatar from assistant audio.

Create the session first (room URL, bot token, session id). This file does
not create a room. Audio out and video out stay off. The Akapulu worker
publishes the face and the played audio.

Tested with Pipecat 1.12.0.
"""

import asyncio
import os

from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.google.llm import GoogleLLMService
from pipecat.transports.daily.transport import DailyParams, DailyTransport
from pipecat.workers.runner import WorkerRunner
from pipecat_akapulu import AkapuluAvatar

SAMPLE_RATE = 24000
SETUP_TIMEOUT_SECS = 180.0


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} is missing")
    return value.strip()


async def run_bot() -> None:
    room_url = _required("DAILY_ROOM_URL")
    if not room_url.startswith("https://"):
        raise RuntimeError("DAILY_ROOM_URL is missing")
    token = _required("DAILY_TOKEN")
    session_id = _required("AKAPULU_SESSION_ID")

    transport = DailyTransport(
        room_url,
        token,
        "Assistant",
        DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=False,
            video_out_enabled=False,
        ),
    )
    stt = DeepgramSTTService(api_key=_required("DEEPGRAM_API_KEY"))
    llm = GoogleLLMService(
        api_key=_required("GOOGLE_API_KEY"),
        settings=GoogleLLMService.Settings(
            system_instruction=(
                "You are a helpful assistant in a voice conversation. "
                "Your responses will be spoken aloud, so avoid emojis, bullet points, "
                "or other formatting that can't be spoken. Respond briefly."
            ),
        ),
    )
    tts = CartesiaTTSService(
        api_key=_required("CARTESIA_API_KEY"),
        sample_rate=SAMPLE_RATE,
        settings=CartesiaTTSService.Settings(
            voice="a167e0f3-df7e-4d52-a9c3-f949145efdab",
        ),
    )
    avatar = AkapuluAvatar(sample_rate=SAMPLE_RATE, session_id=session_id)
    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )
    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            avatar,
            transport.output(),
            assistant_aggregator,
        ]
    )
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=16000,
            audio_out_sample_rate=SAMPLE_RATE,
            enable_metrics=True,
        ),
        setup_timeout_secs=SETUP_TIMEOUT_SECS,
    )
    runner = WorkerRunner(handle_sigint=True)
    await runner.add_workers(worker)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(_transport, _client):
        logger.info("Daily client connected")
        context.add_message(
            {
                "role": "user",
                "content": "Start by greeting the user and ask how you can help.",
            }
        )
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(_transport, _client):
        logger.info("Daily client disconnected")
        await runner.cancel()

    await runner.run()


if __name__ == "__main__":
    asyncio.run(run_bot())
