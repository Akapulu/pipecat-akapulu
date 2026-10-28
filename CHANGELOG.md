# Changelog

## [Unreleased]

## [0.1.10] - 2026-10-27

- `create_akapulu_call` reads `DAILY_API_KEY` and `AKAPULU_API_KEY`. It creates a private Daily room, mints the browser, voice-bot, and worker tokens, and posts the live session. It returns the room URL, the bot token, the browser token, and `session_id`. The worker token is not returned.

## [0.1.9] - 2026-10-27

- Assistant audio is resampled to 24000 Hz before it is sent. A frame that is already 24000 Hz mono is sent unchanged. The pipeline rate no longer has to match the frames.

## [0.1.8] - 2026-10-26

- A debug trace stamps each audio-socket message when the bot hands it over. A message queued before the socket exists keeps that time. Opening the socket does not rewrite it.

## [0.1.7] - 2026-10-26

- `tail_secs` defaults to 0. A TTS stop sends `bot stopped` immediately. Pass `tail_secs` to keep a wait.
- `examples/foundational.py` uses Deepgram Flux (`flux-general-multi`), OpenAI `gpt-5.4-mini`, and Cartesia Sonic 3.6. The first committed turn locks that language on Flux and on Sonic.

## [0.1.6] - 2026-10-24

- Sample rate comes from `FrameProcessorSetup.audio_out_sample_rate` in `setup()`. `AkapuluAvatar` no longer takes `sample_rate`.
- `cleanup()` closes the worker sockets. A cancel during setup does not send `CancelFrame`.
- Pin is `pipecat-ai>=1.12.0`. Tested with Pipecat 1.14.0.
- README: say "The AkapuluAvatar Pipecat plugin" instead of "is a Pipecat plugin".
- README: call `AkapuluAvatar` a Pipecat plugin; drop the Django poll note from loading copy.
- README: spell out loading as RTVI server messages on the Daily call (StartFrame passes through; use status until call_is_ready).
- README: say Akapulu Labs worker, describe loading status for a loading UI, drop idle/tail_secs and AKAPULU_API_URL processor notes.
- `examples/foundational.py` hardcodes Clara and This is AI off. Drop `AKAPULU_AVATAR_ID` and `AKAPULU_SHOW_AI_LABEL`.

## [0.1.5] - 2026-10-23

- The post-speech tail treats a normal audio-socket close (code 1000 or 1001) as the call ending, and close cancels that tail. Pipecat no longer logs it as an unexpected exception.

## [0.1.4] - 2026-10-21

- A clean close of the audio socket stops the reader. `ConnectionClosedOK`, including close code 1001, is not logged as an error. An unexpected close still is.
- 0.1.3 was published from outside this git history and sent `bot_audio` messages the worker rejects. This release does not send those.

## [0.1.2] - 2026-10-19

- A loud chunk during the post-TTS wait starts that wait over. Leaving the wait unset held the gate open for the rest of the call.

## [0.1.1] - 2026-10-18

- Idle becomes speech on the first output chunk above the speaking threshold. `TTSStoppedFrame` still waits `tail_secs` and then flushes.
- `examples/foundational.py` creates the Daily room and the Akapulu session, then prints a join URL.

## [0.1.0] - 2026-10-14

- `AkapuluAvatar` sends assistant audio to the Akapulu worker and forwards loading status into the pipeline.
- Tested with Pipecat 1.12.0.
