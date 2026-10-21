# Changelog

## [Unreleased]

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
