import { pushLog } from "./stores/logs.svelte.js";
import { ensureRemoteStat, removeRemoteStat } from "./stores/stats.svelte.js";

// How far a video frame may lag the live edge (newest timestamp received)
// before we treat it as stale. Past this, a fresher frame is already waiting,
// so decoding the lagging one only accrues latency ("slow motion"); we drop
// the rest of its GOP and jump to the next keyframe instead. ~250ms ≈ 7-8
// frames of slack at 30fps — enough to ride out normal jitter without
// over-trimming, while keeping end-to-end latency bounded. Catch-up costs at
// most one GOP (KEYFRAME_EVERY frames, ~1s).
const MAX_LAG_US = 250_000;

/**
 * RemotePlayer decodes one remote participant's H.264 + Opus streams with
 * WebCodecs. Decoded video frames are drawn onto a canvas; decoded audio is
 * scheduled for playback through a shared AudioContext.
 */
export class RemotePlayer {
  /**
   * @param {string} id
   * @param {{codec: string, width: number, height: number} | null} videoConfig
   * @param {{codec: string, samplerate: number, channelConfig: string} | null} audioConfig
   * @param {AudioContext | null} audioCtx
   */
  constructor(id, videoConfig, audioConfig, audioCtx) {
    this.id = id;
    this.audioCtx = audioCtx;
    /** @type {HTMLCanvasElement | null} */
    this.canvas = null;
    this.sawKeyframe = false;
    this.nextPlayTime = 0;
    this.renderedFrame = false;
    // Codec stats for the debug panel; mutated as frames flow.
    this.stat = ensureRemoteStat(id);

    // Decode-order gating state. Each video GOP is its own MoQ group/subgroup
    // = its own QUIC stream, and the backend reads streams concurrently, so
    // chunks from adjacent GOPs can arrive out of order on a lossy link. We
    // only ever feed the decoder objects belonging to the group we're anchored
    // on, in ascending ObjectID, and jump forward to a newer keyframe instead
    // of decoding stale deltas (which is what smeared the picture against a
    // remote relay).
    /** @type {number | undefined} */
    this.curGroup = undefined;
    this.curObject = -1;
    // Newest video timestamp received (the live edge), used to skip stale
    // frames and converge playback to live.
    this.liveEdgeTs = 0;

    /** @type {VideoDecoder | null} */
    this.videoDecoder = null;
    /** @type {AudioDecoder | null} */
    this.audioDecoder = null;

    if (videoConfig && "VideoDecoder" in window) {
      this.videoDecoder = new VideoDecoder({
        output: (frame) => this.#drawFrame(frame),
        error: (e) => {
          this.stat.decodeErrors++;
          pushLog("ERROR", "video decoder error", { user: id, err: e.message ?? String(e) });
        },
      });
      try {
        this.videoDecoder.configure({
          codec: videoConfig.codec,
          codedWidth: videoConfig.width || undefined,
          codedHeight: videoConfig.height || undefined,
          optimizeForLatency: true,
        });
        pushLog("DEBUG", "video decoder configured", { user: id, codec: videoConfig.codec });
      } catch (e) {
        pushLog("ERROR", "video configure failed", { user: id, codec: videoConfig.codec, err: e.message ?? String(e) });
        this.videoDecoder = null;
      }
    }

    if (audioConfig && audioCtx && "AudioDecoder" in window) {
      this.audioDecoder = new AudioDecoder({
        output: (data) => this.#playAudio(data),
        error: (e) => pushLog("ERROR", "audio decoder error", { user: id, err: e.message ?? String(e) }),
      });
      try {
        this.audioDecoder.configure({
          codec: audioConfig.codec,
          sampleRate: audioConfig.samplerate,
          numberOfChannels: Number(audioConfig.channelConfig) || 1,
        });
        pushLog("DEBUG", "audio decoder configured", { user: id, codec: audioConfig.codec });
      } catch (e) {
        pushLog("ERROR", "audio configure failed", { user: id, err: e.message ?? String(e) });
        this.audioDecoder = null;
      }
    }
  }

  /** @param {HTMLCanvasElement} canvas */
  setCanvas(canvas) {
    this.canvas = canvas;
  }

  /**
   * @param {"video" | "audio"} kind
   * @param {string} b64
   * @param {number} timestampMicros
   * @param {boolean} keyframe
   * @param {number} groupId
   * @param {number} objectId
   */
  pushChunk(kind, b64, timestampMicros, keyframe, groupId, objectId) {
    const data = base64ToBytes(b64);
    if (kind === "video") {
      this.#pushVideo(data, timestampMicros, keyframe, groupId, objectId);
    } else {
      if (!this.audioDecoder || this.audioDecoder.state !== "configured") return;
      // Opus frames are independently decodable, so audio needs no group
      // gating; the player schedules them by timestamp (see #playAudio).
      this.audioDecoder.decode(
        new EncodedAudioChunk({ type: "key", timestamp: timestampMicros, data }),
      );
    }
  }

  /**
   * Feed one video object to the decoder in strict decode order. The rules:
   *   - A keyframe (object 0 of a group) is an IDR — independently decodable
   *     and a reference-buffer reset. Jump to it whenever it belongs to a
   *     group at least as new as the current one. This bootstraps the first
   *     frame and recovers from any loss/reorder in the previous GOP.
   *   - A delta is decoded only if it's the next object of the current group.
   *     Stragglers from an older group, or a newer group whose keyframe hasn't
   *     arrived, are dropped. A hole inside the current group abandons it and
   *     waits for the next keyframe rather than smearing forward.
   * @param {Uint8Array} data
   * @param {number} ts
   * @param {boolean} keyframe
   * @param {number} groupId
   * @param {number} objectId
   */
  #pushVideo(data, ts, keyframe, groupId, objectId) {
    if (!this.videoDecoder || this.videoDecoder.state !== "configured") return;

    // Advance the live edge. `behind` means a fresher frame already arrived
    // (reordering/backlog on the link), so this one is stale.
    if (ts > this.liveEdgeTs) this.liveEdgeTs = ts;
    const behind = this.liveEdgeTs - ts > MAX_LAG_US;

    if (keyframe) {
      // Keyframes are the catch-up points: always take the newest one. Even if
      // it lags the live edge, decoding it re-anchors us nearer to live than
      // staying on the older group would.
      if (this.curGroup === undefined || groupId >= this.curGroup) {
        this.curGroup = groupId;
        this.curObject = objectId; // 0
        this.sawKeyframe = true;
        this.#decodeVideo("key", ts, data);
      } else {
        this.stat.framesDropped++; // late keyframe from a group we're already past
      }
      return;
    }

    if (!this.sawKeyframe) return; // not yet anchored on a keyframe
    if (groupId !== this.curGroup || objectId !== this.curObject + 1) {
      // Out-of-group straggler, a future group missing its keyframe, or a hole
      // in the current GOP. Stop decoding this group; the next keyframe
      // re-anchors us cleanly.
      if (groupId === this.curGroup) this.sawKeyframe = false;
      this.stat.framesDropped++;
      return;
    }
    if (behind) {
      // We're lagging the live edge. Abandon the rest of this GOP and wait for
      // the next keyframe to jump forward, rather than playing out the backlog
      // in slow motion.
      this.sawKeyframe = false;
      this.stat.framesDropped++;
      return;
    }
    this.curObject = objectId;
    this.#decodeVideo("delta", ts, data);
  }

  /** @param {"key"|"delta"} type @param {number} ts @param {Uint8Array} data */
  #decodeVideo(type, ts, data) {
    try {
      this.videoDecoder.decode(new EncodedVideoChunk({ type, timestamp: ts, data }));
      this.stat.decodeQueue = this.videoDecoder.decodeQueueSize;
    } catch (e) {
      this.stat.decodeErrors++;
      pushLog("ERROR", "video decode threw", { user: this.id, err: e.message ?? String(e) });
    }
  }

  /** @param {VideoFrame} frame */
  #drawFrame(frame) {
    const canvas = this.canvas;
    if (!canvas) {
      frame.close();
      return;
    }
    const w = frame.displayWidth;
    const h = frame.displayHeight;
    if (canvas.width !== w) canvas.width = w;
    if (canvas.height !== h) canvas.height = h;
    const ctx = canvas.getContext("2d");
    if (ctx) ctx.drawImage(frame, 0, 0, w, h);
    frame.close();
    this.stat.framesDecoded++;
    this.stat.width = w;
    this.stat.height = h;
    this.stat.decodeQueue = this.videoDecoder?.decodeQueueSize ?? 0;
    if (!this.renderedFrame) {
      this.renderedFrame = true;
      pushLog("INFO", "rendering remote video", { user: this.id, w, h });
    }
  }

  /** @param {AudioData} data */
  #playAudio(data) {
    const ctx = this.audioCtx;
    if (!ctx) {
      data.close();
      return;
    }
    const channels = data.numberOfChannels;
    const frames = data.numberOfFrames;
    const buffer = ctx.createBuffer(channels, frames, data.sampleRate);
    for (let c = 0; c < channels; c++) {
      const plane = new Float32Array(frames);
      data.copyTo(plane, { planeIndex: c, format: "f32-planar" });
      buffer.copyToChannel(plane, c);
    }
    data.close();

    const src = ctx.createBufferSource();
    src.buffer = buffer;
    src.connect(ctx.destination);
    // Schedule back-to-back; if we've fallen behind, resync to now.
    const start = Math.max(ctx.currentTime, this.nextPlayTime);
    src.start(start);
    this.nextPlayTime = start + buffer.duration;
  }

  close() {
    try {
      this.videoDecoder?.close();
    } catch {
      // already closed
    }
    try {
      this.audioDecoder?.close();
    } catch {
      // already closed
    }
    this.videoDecoder = null;
    this.audioDecoder = null;
    this.canvas = null;
    removeRemoteStat(this.id);
  }
}

/** @param {string} b64 */
function base64ToBytes(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    bytes[i] = bin.charCodeAt(i);
  }
  return bytes;
}
