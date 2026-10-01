"""Apply the approved Pipecat 1.0 event migration to the pinned SEMOSS release."""
import hashlib


EXPECTED_SHA256 = "87db7f8b98eec660ff54bb92e5db4082fc92672dd3d01c78c921b8b82ee7be1f"

OLD_HANDLER = '''        @transcript.event_handler("on_transcript_update")
        async def on_transcript_update(processor, frame):
            for msg in frame.messages:
                payload = {
                    "type": "transcription",
                    "role": msg.role,
                    "text": msg.content,
                    "ts": msg.timestamp,
                    "isFinal": msg.is_final,
                }
                await transport.send_message(json.dumps(payload))
'''

NEW_HANDLER = '''        async def send_transcript(role, message):
            if message.content:
                await transport.send_message(json.dumps({
                    "type": "transcription",
                    "role": role,
                    "text": message.content,
                    "ts": message.timestamp,
                    "isFinal": True,
                }))

        @user_agg.event_handler("on_user_turn_stopped")
        async def on_user_turn_stopped(aggregator, strategy, message):
            await send_transcript("user", message)

        @assistant_agg.event_handler("on_assistant_turn_stopped")
        async def on_assistant_turn_stopped(aggregator, message):
            await send_transcript("assistant", message)
'''


def patch_audio(path):
    original = path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest != EXPECTED_SHA256:
        raise ValueError("Unexpected SEMOSS audio source hash: " + digest)
    text = original.decode("utf-8")
    replacements = [
        ("from pipecat.processors.transcript_processor import TranscriptProcessor\n", ""),
        ("        transcript = TranscriptProcessor()\n", ""),
        ("                transcript.user(),\n", ""),
        ("                transcript.assistant(),\n", ""),
        (OLD_HANDLER, NEW_HANDLER),
    ]
    for old, new in replacements:
        if text.count(old) != 1:
            raise ValueError("Audio compatibility patch target must occur exactly once")
        text = text.replace(old, new, 1)
    compile(text, "lk_to_pcat.py", "exec")
    updated = text.encode("utf-8")
    path.write_bytes(updated)
    return {
        "path": "py/audio/lk_to_pcat.py",
        "change": "Replace removed TranscriptProcessor with Pipecat 1.0 turn-stopped events",
        "original_sha256": digest,
        "patched_sha256": hashlib.sha256(updated).hexdigest(),
    }
