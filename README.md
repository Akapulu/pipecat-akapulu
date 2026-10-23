# pipecat-akapulu

Pipecat processor for an [Akapulu Labs](https://akapulu.com) talking avatar. Maintained by Akapulu Labs.

`AkapuluAvatar` sits after your speech model. It sends assistant audio to the Akapulu Labs worker. The Akapulu Labs worker joins the Daily room already stored on the session and publishes the face and the played audio. This processor does not publish either.

Tested with Pipecat 1.12.0.

## Install

```bash
uv add pipecat-akapulu
```

## Use

Create the Daily room yourself, then `POST /api/live-sessions/` from your server with your Akapulu Labs API key before the bot starts. The body is `avatar_id`, `show_ai_label`, `room_url`, and `avatar_token`. The response is `session_id`.

```python
from pipecat_akapulu import AkapuluAvatar

avatar = AkapuluAvatar(sample_rate=24000, session_id=session_id)
```

`sample_rate` is a positive multiple of 25 and must match the assistant audio frames.

Leave transport audio out and video out off. The Akapulu Labs worker is the Daily publisher for the face and the played audio.

When the pipeline starts, `AkapuluAvatar` begins connecting in the background and forwards `StartFrame` right away so your voice bot keeps moving. While the Akapulu Labs worker comes up, it pushes Pipecat server messages with `type` `status`. Those messages include:

- `call_is_ready`: whether the Akapulu Labs worker has published the first frame
- `completion_percent`: how far startup has gotten (0 to 100)
- `latest_update_text`: the latest human-readable startup line

Use those fields for a loading UI. `AkapuluAvatar` handles startup status for you.

## Example

`examples/foundational.py` creates a private Daily room, mints the browser, voice-bot, and Akapulu Labs worker tokens, then calls the Akapulu Labs API (`POST /api/live-sessions/` with your Bearer API key). It then runs speech-to-text, a model, speech output, and `AkapuluAvatar`. It prints a join URL. Open that URL. The bot greets when you join, and it listens only to that browser microphone.

The example uses Clara, leaves **This is AI** off, and calls the Akapulu Labs production API.

A GPT Live bot, a local server, and a browser page are in [akapulu-examples](https://github.com/Akapulu/akapulu-examples).

```bash
uv add "pipecat-ai[daily,deepgram,cartesia,google,silero]==1.12.0" pipecat-akapulu
```

Set `DAILY_API_KEY` and `AKAPULU_API_KEY`. Also set `DEEPGRAM_API_KEY`, `GOOGLE_API_KEY`, and `CARTESIA_API_KEY`. Then:

```bash
python examples/foundational.py
```
