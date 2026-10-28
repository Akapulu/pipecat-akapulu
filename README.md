# pipecat-akapulu

Pipecat processor for an [Akapulu Labs](https://akapulu.com) talking avatar. Maintained by Akapulu Labs.

The `AkapuluAvatar` Pipecat plugin sits after your speech model. It sends assistant audio to the Akapulu Labs worker. The Akapulu Labs worker joins the Daily room already stored on the session and publishes the face and the played audio. It does not publish either.

Tested with Pipecat 1.14.0. Requires Pipecat 1.12.0 or newer.

## Install

```bash
uv add pipecat-akapulu
```

## Use

`create_akapulu_call` runs on your server before the bot starts. It creates a private Daily room for three participants, mints the browser, voice-bot, and worker tokens, and `POST /api/live-sessions/`. It returns the room URL, the bot token, the browser token, and `session_id`. The worker token is sent to Akapulu Labs and is not returned. The browser joins as `You`. The worker joins as `Avatar`.

```python
from pipecat_akapulu import AkapuluAvatar, create_akapulu_call

room_url, bot_token, browser_token, session_id = await create_akapulu_call(
    daily_api_key=daily_api_key,
    akapulu_api_key=akapulu_api_key,
    avatar_id=avatar_id,
    show_ai_label=True,
    session=session,
    room_exp_secs=2 * 60 * 60,
)
avatar = AkapuluAvatar(session_id=session_id)
```

The bot joins Daily with `bot_token`. The browser joins with `browser_token`. Pass `session_id` to `AkapuluAvatar`.

A bot that already has a Daily room skips `create_akapulu_call`. Mint the worker token and `POST /api/live-sessions/` with `avatar_id`, `show_ai_label`, `room_url`, and `avatar_token`. The response is `session_id`.

Assistant audio is resampled to 24000 Hz mono before it is sent. A frame that is already 24000 Hz mono is sent unchanged. The worker always receives 24000.

Leave transport audio out and video out off. The Akapulu Labs worker is the Daily publisher for the face and the played audio.

The `AkapuluAvatar` Pipecat plugin connects to the Akapulu Labs worker in the background. When the pipeline starts, it forwards `StartFrame` right away so your voice bot keeps moving. It does not wait for the first face frame.

While the Akapulu Labs worker boots, the plugin pushes **RTVI server messages** into your voice-bot pipeline. Each message body has `type` `status` and these fields:

- `call_is_ready`: whether the Akapulu Labs worker has published the first frame
- `completion_percent`: how far startup has gotten (0 to 100)
- `latest_update_text`: the latest human-readable startup line

Those are not Daily audio or video tracks. With RTVI on the call (for example `@pipecat-ai/client-js` in the browser), they arrive on the client as RTVI `ServerMessage` events over that same Daily connection. Use them for a loading spinner, percent, and copy. Keep the loading UI up until `call_is_ready` is true.

## Example

`examples/foundational.py` calls `create_akapulu_call`, then runs the voice bot. Deepgram Flux (`flux-general-multi`) hears the caller. OpenAI `gpt-5.4-mini` speaks, with reasoning off. Cartesia Sonic 3.6 speaks each finished sentence, using the Ariana voice. `AkapuluAvatar` sits after that audio. It prints a join URL. Open that URL. The bot greets when you join, and it listens only to that browser microphone. The first committed turn locks that language on Flux and on Sonic.

The example uses Clara, leaves **This is AI** off, and calls the Akapulu Labs production API.

The same voice stack, a local server, and a browser page are in [akapulu-examples](https://github.com/Akapulu/akapulu-examples).

```bash
uv add "pipecat-ai[daily,deepgram,openai,cartesia]==1.14.0" pipecat-akapulu
```

Set `DAILY_API_KEY` and `AKAPULU_API_KEY`. Also set `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, and `CARTESIA_API_KEY`. Then:

```bash
python examples/foundational.py
```
