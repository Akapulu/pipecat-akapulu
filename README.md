# pipecat-akapulu

Pipecat processor for an [Akapulu Labs](https://akapulu.com) talking avatar. Maintained by Akapulu Labs.

`AkapuluAvatar` sits after your speech model. It sends assistant audio to the Akapulu worker. The worker joins the Daily room already stored on the session and publishes the face and the played audio. This processor does not publish either.

Tested with Pipecat 1.12.0.

## Install

```bash
uv add pipecat-akapulu
```

## Use

Create the Daily room yourself, then `POST /api/live-sessions/` from your server with your Akapulu API key before the bot starts. The body is `avatar_id`, `show_ai_label`, `room_url`, and `avatar_token`. The response is `session_id`.

```python
from pipecat_akapulu import AkapuluAvatar

avatar = AkapuluAvatar(sample_rate=24000, session_id=session_id)
```

`sample_rate` is a positive multiple of 25 and must match the assistant audio frames. `tail_secs` defaults to `0.6`, which is how long assistant audio stays open after speech stops.

Leave transport audio out and video out off. The worker is the Daily publisher for the face and the played audio.

`AKAPULU_API_URL` selects the API origin. Unset, the processor uses `https://akapulu.com`. A set value that is empty or not an `http` or `https` origin raises.

On `StartFrame` the processor posts `{}` to `POST /api/live-sessions/<session_id>/status/`. It polls `GET` on that same route for the worker socket and for loading status, and pushes each status body as an RTVI server message.

## Example

`examples/foundational.py` is a Daily bot: speech-to-text, a model, speech output, then `AkapuluAvatar`. It greets when the Daily client connects.

```bash
uv add "pipecat-ai[daily,deepgram,cartesia,google,silero]==1.12.0" pipecat-akapulu
```

Set `DAILY_ROOM_URL`, `DAILY_TOKEN`, and `AKAPULU_SESSION_ID` from the session you created. Also set `DEEPGRAM_API_KEY`, `GOOGLE_API_KEY`, and `CARTESIA_API_KEY`. Then:

```bash
python examples/foundational.py
```
