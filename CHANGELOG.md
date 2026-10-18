# Changelog

## [Unreleased]

- Idle becomes speech on the first output chunk above the speaking threshold. `TTSStoppedFrame` still waits `tail_secs` and then flushes.
- `examples/foundational.py` creates the Daily room and the Akapulu session, then prints a join URL.

## [0.1.0] - 2026-10-14

- `AkapuluAvatar` sends assistant audio to the Akapulu worker and forwards loading status into the pipeline.
- Tested with Pipecat 1.12.0.
