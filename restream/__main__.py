"""Twitch -> YouTube restreamer.

Setup:
    sudo apt install -y streamlink ffmpeg            # Ubuntu
    # On Windows install streamlink + ffmpeg and ensure both are on PATH.
    pip install -r requirements.txt

Configure restream/.env (copy from restream/.env.example). Required:
    TWITCH_CHANNEL=...
    TWITCH_CLIENT_ID=...
    TWITCH_CLIENT_SECRET=...

YouTube Data API one-time bootstrap (opens browser):
    python -m restream.youtube_live --auth

Run the daemon:
    python -m restream

Check current YouTube-side health of the persisted broadcast:
    python -m restream --status

End the persisted broadcast (if Ctrl-C'd earlier):
    python -m restream --shutdown

Run without re-encoding (zero CPU, but beep artifacts at Twitch ad breaks):
    python -m restream --passthrough

Force full re-encode (libx264 + AAC, max CPU, max stability at ad breaks):
    python -m restream --full-reencode

(The default is video -c:v copy + audio AAC re-encode — saves ~1 CPU core
while still killing ad-break beeps. Adjust reencode_video in config.toml's
[restream] section to change the default.)

State machine:
  - OFFLINE: poll Twitch Helix /streams every RESTREAM_RETRY_SECONDS. When
    it goes live, mint a fresh YouTube liveBroadcast + liveStream titled
    after the Twitch stream and persist its identifiers to
    restream/.session.json.
  - LIVE: pipe streamlink -> ffmpeg -> YouTube RTMP. If ffmpeg exits
    while Twitch is still live (network drop, RTMP hiccup), sleep
    RESTREAM_RECONNECT_SECONDS and reconnect RTMP to the *same* stream
    key — YouTube resumes the broadcast at the same watch URL.
    Exponential backoff kicks in when ffmpeg dies in < 30s repeatedly.
  - RESTART: on Ctrl-C the broadcast is preserved (not transitioned
    to complete). The next `python -m restream` invocation reads the
    session file, confirms the broadcast is still reusable on YouTube's
    side, and reconnects to it. If YouTube has force-ended it, a fresh
    broadcast is minted.
  - TEARDOWN: broadcast is transitioned to `complete` and the stream
    resource deleted only when (a) Twitch goes offline, (b) YouTube
    force-ended the broadcast, or (c) `--shutdown` is invoked.
"""
from .restreamer import main

if __name__ == "__main__":
    main()
