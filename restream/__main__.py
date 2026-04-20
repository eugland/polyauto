"""Twitch -> YouTube restreamer (streamlink + ffmpeg passthrough).

Setup on Ubuntu:
    sudo apt install -y streamlink ffmpeg

Create restream/.env (this folder, NOT the repo root .env):
    TWITCH_CHANNEL=gettingajob
    YOUTUBE_STREAM_KEY=<your rotated key>
    # optional:
    # RESTREAM_QUALITY=best              # streamlink quality token: best, 1080p60, 720p, ...
    # RESTREAM_RETRY_SECONDS=30          # how long to wait when channel is offline
    # RESTREAM_HLS_LIVE_EDGE=15          # # segments behind live (~2s each); 15 ≈ 30s buffer
    # RESTREAM_BUFFER_MB=64              # streamlink in-memory ring buffer
    # RESTREAM_REENCODE_AUDIO=1          # re-encode audio to AAC (default on; kills ad-break beeps)
    # RESTREAM_REENCODE_VIDEO=0          # re-encode video to H.264 (default off; ~1 CPU core)
    # RESTREAM_VIDEO_BITRATE_KBPS=6000   # only used when RESTREAM_REENCODE_VIDEO=1

restream/.env is gitignored — secrets stay local.

Run:
    python3 -m restream

The loop:
  - streamlink tries to pull the live HLS stream from twitch.tv/<channel>
  - if live, output is piped to ffmpeg with -c copy (no re-encode, ~0 CPU)
  - ffmpeg pushes the FLV mux to YouTube's RTMP ingest
  - on offline / disconnect / any non-zero exit, sleep RESTREAM_RETRY_SECONDS and retry
  - Ctrl-C exits cleanly
"""
from .restreamer import main

if __name__ == "__main__":
    main()
