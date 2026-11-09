# pipecat-akapulu

Pipecat processor for an [Akapulu Labs](https://akapulu.com) talking avatar. Maintained by Akapulu Labs.

The `AkapuluAvatar` Pipecat plugin sits after your speech model. It sends assistant audio to the Akapulu Labs worker. The worker joins the Daily room for that call and publishes the avatar video and audio. Your voice bot itself does not publish camera or microphone.

Tested with Pipecat 1.14.0. Requires Pipecat 1.12.0 or newer.

## Install

```bash
uv add pipecat-akapulu
```

## Use

`create_akapulu_call` runs on your server before the bot starts. It reads `DAILY_API_KEY` and `AKAPULU_API_KEY`. A missing or blank value raises. It creates a private Daily room for three participants, mints the browser, voice-bot, and worker tokens, and `POST /api/live-sessions/`. It returns the room URL, the bot token, the browser token, and `session_id`. The worker token is sent to Akapulu Labs and is not returned. The browser joins as `You`. The worker joins as `Avatar`. Daily shows the end user and the avatar. The voice bot is in the room and does not get its own tile. `create_akapulu_call` spawns an avatar worker. That worker can take 5 to 10 seconds to start up. The Daily room lasts two hours unless you pass `room_exp_secs`. Daily removes everyone when it expires.

```python
from pipecat_akapulu import AkapuluAvatar, create_akapulu_call

room_url, bot_token, browser_token, session_id = await create_akapulu_call(
    avatar_id=avatar_id,
    show_ai_label=True,
    session=session,
)
avatar = AkapuluAvatar(session_id=session_id)
```

The bot joins Daily with `bot_token`. The browser joins with `browser_token`. Pass `session_id` to `AkapuluAvatar`.

A bot that already has a Daily room skips `create_akapulu_call`. Mint the avatar worker token and POST `/api/live-sessions/` to Akapulu Labs with `avatar_id`, `show_ai_label`, `room_url`, and `avatar_token`. The response is `session_id`. The avatar worker joins the room as `Avatar` and publishes audio and video. Its token needs permission to send audio and video.

When you construct your voice bot and create your transport processor, leave audio out and video out off. Your voice bot will not publish audio or video into the Daily room. The Akapulu Labs avatar worker publishes the audio and video. Add `AkapuluAvatar` after your TTS processor so it forwards that audio to the avatar worker.

The avatar worker needs about 800 ms of assistant audio before it starts rendering the video. The render itself, given the audio, only takes around 150ms. You might see added latency in your bot's response even if your TTS provider's TTFA is low, due to this buffer accumulation. A TTS that trickles audio in slowly fills that buffer late, so the response is slower and the avatar gestures less. We recommend a TTS that sends audio in waves. Cartesia has been the best in our testing.

The `AkapuluAvatar` Pipecat plugin connects to the Akapulu Labs worker in the background. When the pipeline starts, it forwards `StartFrame` right away so your voice bot keeps moving. It does not wait for the first video frame.

While the Akapulu Labs worker boots, the plugin pushes **RTVI server messages** into your voice-bot pipeline. Each message body has `type` `status` and these fields:

- `call_is_ready`: whether the Akapulu Labs worker has published the first frame
- `completion_percent`: how far startup has gotten (0 to 100)
- `latest_update_text`: the latest human-readable startup line

Those are not Daily audio or video tracks. With RTVI on the call (for example `@pipecat-ai/client-js` in the browser), they arrive on the client as RTVI `ServerMessage` events over that same Daily connection. Use them for a loading spinner, percent, and copy. Keep the loading UI up until `call_is_ready` is true.

## Example

`examples/foundational.py` calls `create_akapulu_call`, then runs the voice bot. Deepgram Flux (`flux-general-multi`) hears the caller. OpenAI `gpt-5.4-mini` speaks, with reasoning off. Cartesia Sonic 3.6 speaks each finished sentence, using the Carson male voice. `AkapuluAvatar` sits after that audio. It prints a join URL. Open that URL. Daily Prebuilt shows you and the avatar. The bot greets when you join. When the participant named Avatar joins, the bot turns that microphone off. The browser stays subscribed to it. The call starts in English. A later committed turn in another language switches Flux and Sonic.

The example uses Hollis, leaves **This is AI** off, and calls the Akapulu Labs production API.

The same voice stack, a local server, and a browser page are in [akapulu-examples](https://github.com/Akapulu/akapulu-examples).

```bash
uv add "pipecat-ai[daily,deepgram,openai,cartesia]==1.14.0" pipecat-akapulu
```

Set `DAILY_API_KEY` and `AKAPULU_API_KEY`. Also set `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, and `CARTESIA_API_KEY`. Then:

```bash
python examples/foundational.py
```
