"""Twitch -> YouTube restreamer with concurrent topic-segmented uploads.

Setup:
    sudo apt install -y streamlink ffmpeg            # Ubuntu
    # On Windows install streamlink + ffmpeg and ensure both are on PATH.
    pip install -r requirements.txt

Configure restream/.env (copy from restream/.env.example). The minimum:
    TWITCH_CHANNEL=...
    YOUTUBE_STREAM_KEY=...                           # static fallback key
    # Recommended for title-sync + dynamic broadcast:
    TWITCH_CLIENT_ID=...
    TWITCH_CLIENT_SECRET=...

YouTube Data API one-time bootstrap (opens browser):
    python -m restream.youtube_live --auth

Run the daemon:
    python -m restream

What happens per cycle:
  - If TWITCH_CLIENT_ID/SECRET present: poll Helix /streams; skip if offline.
  - Otherwise: fall back to streamlink probe (legacy path).
  - If RESTREAM_USE_DYNAMIC_BROADCAST=1 and YouTube creds are set up: mint
    a fresh liveBroadcast + liveStream titled after the live Twitch stream
    and use that stream key for ffmpeg's RTMP output.
  - If RESTREAM_RECORD_ENABLED=1: ffmpeg also writes 30s MP4 chunks to
    restream/recordings/<session_id>/, fed into a concurrent pipeline:
        SegmentWatcher -> Transcriber (faster-whisper) ->
        TopicDetector (MiniLM cosine) -> Cutter (ffmpeg concat) ->
        Uploader (videos.insert).
  - Each detected topic boundary triggers a cut + upload while the live
    stream is still going.
  - On clean ffmpeg exit (channel ended): pipeline drains, broadcast is
    transitioned to 'complete', stream resource is deleted.

Smoke tests:
    python -m restream.transcriber path/to/seg.mp4
    python -m restream.topic_detector path/to/sentences.json
    python -m restream.post_processor --replay path/to/recordings/<session_id>
    RESTREAM_UPLOAD_ENABLED=0 python -m restream    # full dry-run
"""
from .restreamer import main

if __name__ == "__main__":
    main()
