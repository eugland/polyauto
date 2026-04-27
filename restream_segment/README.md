# Restream Segment — Research Plan

Research notes and implementation plan for auto-segmenting a previously
streamed YouTube VOD (single-host, desk setup with occasional screen
share) into topic-coherent clips.

Status: not implemented. This folder only holds the design docs.

## Problem

Input: multi-hour YouTube livestream VODs. One presenter at a desk
talking; occasionally switches to screen-share (another video, a web
page, a game). Visual signal is noisy — a scene change almost always
means "screen toggled," not "topic changed."

Goal: cut the VOD into N segments, each covering one coherent topic.
Each segment should be directly re-uploadable as its own clip.

## Why visual / silence detection alone is insufficient

- Scene change fires on every screen-share toggle even when the host
  is still on the same topic.
- Silence detection catches breaths and pauses but not topic shifts —
  a host can talk continuously while moving between topics, or pause
  mid-topic to read a chat message.
- Audio-energy heuristics have the same problem.

Topic segmentation must come from the **transcript**.

## Approach A — Embedding-based TextTiling (local, free, deterministic)

Pipeline:

1. **Transcribe** with word timestamps: `faster-whisper`
   (`large-v3` for quality, `medium` for speed). Output:
   `[(word, start_s, end_s), ...]`.
2. **Chunk** into 30–60s blocks (or sentence-aligned).
3. **Embed** each block with `sentence-transformers`
   (`all-MiniLM-L6-v2` baseline; `bge-small-en-v1.5` if better
   semantic quality matters).
4. **Boundary score** via TextTiling depth:
   - Adjacent-pair cosine similarity series `s[i] = cos(emb[i], emb[i+1])`.
   - For each gap, depth = `(left_peak - valley) + (right_peak - valley)`.
   - Boundaries = local minima whose depth exceeds a threshold.
   - This is more robust than raw-cosine thresholding: it only cuts
     where similarity actually *dips* relative to its neighborhood.
5. **Smooth**: merge any segment shorter than ~90s into its neighbor.
6. **Snap** boundary timestamps to the nearest sentence end or whisper
   VAD silence — avoids mid-word cuts.
7. **Cut**: `ffmpeg -ss S -to E -c copy -i in.mp4 seg_N.mp4` (lossless,
   instant, no re-encode).

Libraries: `faster-whisper`, `sentence-transformers`, `numpy`,
`scipy.signal.find_peaks`, `ffmpeg-python` or raw `subprocess`.

Pros: fully offline, free, deterministic, reproducible.
Cons: struggles when host "stays on a topic but pulls up a video to
illustrate" — embeddings see the video narration as a new topic.

## Approach B — LLM on the full transcript

Pipeline:

1. Same whisper step.
2. Format transcript as one line per utterance: `[mm:ss] text...`.
3. Single call to a long-context LLM with a prompt like:

   > You are given a timestamped transcript of a livestream.
   > Segment it into topic-coherent chunks. Return strict JSON:
   > `[{"start":"HH:MM:SS","end":"HH:MM:SS","title":"...","summary":"..."}]`.
   > Rules: minimum segment length 2 minutes; merge brief digressions
   > (<60s) into the parent topic; do not cut mid-sentence; cover the
   > entire timeline with no gaps.

4. Parse JSON, feed timestamps to ffmpeg.

Pros: understands "same topic, different visual aid"; output ships
with human-readable titles.
Cons: non-deterministic; depends on API availability.

## Approach C — Hybrid (recommended)

Run A to produce candidate boundaries, then feed the transcript with
those candidates marked to the LLM with instructions to
confirm/reject/rename. Cheap token-wise (LLM only makes decisions at
the candidate boundaries), and gets the best of both.

## Tool stack

| Stage          | Library / service               | Notes                               |
|----------------|---------------------------------|-------------------------------------|
| Download VOD   | `yt-dlp` (Python package)       | Grab bestaudio+bestvideo            |
| Transcribe     | `faster-whisper`                | CPU-ok, GPU 5–10× faster            |
| Embeddings     | `sentence-transformers`         | MiniLM is 80MB, runs on CPU         |
| Boundaries     | `numpy`, `scipy.signal`         | Depth-score peak picking            |
| Chat signal    | `chat-downloader`               | Optional: spikes = highlight prior  |
| Cut            | `ffmpeg` via `subprocess`       | `-c copy` lossless                  |
| Upload back    | `google-api-python-client`      | YouTube Data API v3                 |

## Free LLM options for Approach B / C

Ranked by fit for long transcripts:

1. **Google AI Studio (Gemini Flash)** — 1M+ context, generous free
   daily quota. First choice.
2. **Groq** — Llama 3.3 70B / Qwen / DeepSeek, ~128k context, very
   fast. Split streams >~8h.
3. **OpenRouter `:free` endpoints** — rotating set of free Llama/Qwen/
   DeepSeek variants. OpenAI-compatible API.
4. **Local via Ollama** — `qwen2.5:14b` or `llama3.1:8b`, 128k ctx,
   fully offline, no keys. Fallback when hosted quotas run out.
5. **Cerebras** — free tier, very fast, smaller model lineup.

## Implementation order

Smallest first, each one shippable:

1. `download.py` — `yt-dlp` a URL → local `.mp4` + `.wav`.
2. `transcribe.py` — wav → word-timestamped JSON.
3. `segment_llm.py` — JSON + Gemini → segments JSON. (Approach B first;
   fastest to a useful result.)
4. `cut.py` — segments JSON + mp4 → N output files via `ffmpeg -c copy`.
5. `segment_embed.py` — local/offline alternative using Approach A.
6. Optional: `segment_hybrid.py` that combines them.
7. Optional: YouTube upload step.

## Open questions

- Desired segment length distribution? (Probably 3–15 min target.)
- Do we want vertical/short reframing like Opus Clips? If yes, this
  becomes a different project — a compositor with face-tracking. Out
  of scope for this plan.
- Chat replay as a highlight prior worth the complexity? Probably not
  for v1 — topic segmentation ≠ highlight extraction.

## References

- Hearst (1997), *TextTiling: Segmenting Text into Multi-paragraph
  Subtopic Passages* — the depth-score idea.
- `faster-whisper`: https://github.com/SYSTRAN/faster-whisper
- `sentence-transformers`: https://www.sbert.net
- `yt-dlp`: https://github.com/yt-dlp/yt-dlp
