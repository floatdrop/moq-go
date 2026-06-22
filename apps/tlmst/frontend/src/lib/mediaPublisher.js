import { SessionService } from "../../bindings/github.com/floatdrop/moq-go/apps/tlmst/index.js";
import {
  publishStarted,
  publishStopped,
  recordEncodedFrame,
  recordEncodeDrop,
} from "./stores/stats.svelte.js";

// Tunables for the local encode.
const VIDEO_CODEC = "avc1.42E01F"; // H.264 Baseline 3.1
const AUDIO_CODEC = "opus";
const VIDEO_BITRATE = 1_500_000;
const AUDIO_BITRATE = 64_000;
const KEYFRAME_EVERY = 30; // force a keyframe ~every 1s at 30fps

/**
 * MediaPublisher encodes a local MediaStream with WebCodecs (H.264 + Opus) and
 * streams the encoded chunks to the Go backend, which packages them as LOC
 * objects and publishes them over MoQ.
 *
 * Frame delivery uses an HTMLVideoElement + requestVideoFrameCallback because
 * MediaStreamTrackProcessor is not available in WebKit/WKWebView. Audio capture
 * still relies on MediaStreamTrackProcessor; when it's missing we publish video
 * only and report it via onStatus.
 */
export class MediaPublisher {
  /**
   * @param {MediaStream} stream
   * @param {(msg: string) => void} [onStatus]
   */
  constructor(stream, onStatus = () => {}) {
    this.stream = stream;
    this.onStatus = onStatus;
    this.running = false;
    this.frameCount = 0;
    /** @type {VideoEncoder | null} */
    this.videoEncoder = null;
    /** @type {AudioEncoder | null} */
    this.audioEncoder = null;
    /** @type {HTMLVideoElement | null} */
    this.videoEl = null;
    /** @type {ReadableStreamDefaultReader | null} */
    this.audioReader = null;
    /** @type {AudioContext | null} */
    this.audioContext = null;
    /** @type {AudioWorkletNode | null} */
    this.audioNode = null;
    /** @type {MediaStreamAudioSourceNode | null} */
    this.audioSource = null;
    // Running frame count for AudioData timestamps on the worklet path.
    this.audioSampleCursor = 0;
    // Encoder config, captured at start and kept stable across device switches
    // so re-captured audio keeps matching the published catalog.
    this.sampleRate = 48000;
    this.channels = 1;
    // Per-track promise chains keep backend calls ordered without blocking the
    // encoder output callbacks.
    this.videoChain = Promise.resolve();
    this.audioChain = Promise.resolve();
  }

  async start() {
    if (!("VideoEncoder" in window)) {
      throw new Error("WebCodecs VideoEncoder is not available in this webview");
    }

    const videoTrack = this.stream.getVideoTracks()[0];
    const audioTrack = this.stream.getAudioTracks()[0];
    if (!videoTrack) {
      throw new Error("no video track to publish");
    }

    const vs = videoTrack.getSettings();
    const width = vs.width ?? 1280;
    const height = vs.height ?? 720;
    const framerate = vs.frameRate ?? 30;

    const as = audioTrack?.getSettings() ?? {};
    this.sampleRate = as.sampleRate ?? 48000;
    this.channels = as.channelCount ?? 1;
    const sampleRate = this.sampleRate;
    const channels = this.channels;

    // 1. Tell the backend to announce the catalog + open the media tracks.
    await SessionService.StartPublishing(
      {
        codec: VIDEO_CODEC,
        width,
        height,
        framerate,
        bitrate: VIDEO_BITRATE,
      },
      {
        codec: AUDIO_CODEC,
        samplerate: sampleRate,
        channelConfig: String(channels),
        bitrate: AUDIO_BITRATE,
      },
    );

    this.running = true;
    publishStarted(width, height);

    // 2. Configure the video encoder. "annexb" makes each keyframe carry its
    // SPS/PPS inline, so the stream is self-describing with no separate config.
    this.videoEncoder = new VideoEncoder({
      output: (chunk) => this.#onVideoChunk(chunk),
      error: (e) => this.onStatus(`video encoder error: ${e.message}`),
    });
    this.videoEncoder.configure({
      codec: VIDEO_CODEC,
      width,
      height,
      framerate,
      bitrate: VIDEO_BITRATE,
      latencyMode: "realtime",
      avc: { format: "annexb" },
    });

    // 3. Pump camera frames into the encoder.
    this.videoEl = document.createElement("video");
    this.videoEl.srcObject = new MediaStream([videoTrack]);
    this.videoEl.muted = true;
    this.videoEl.playsInline = true;
    await this.videoEl.play();

    const pump = (_now, metadata) => {
      if (!this.running || !this.videoEncoder || !this.videoEl) return;
      try {
        const seconds = metadata?.mediaTime ?? this.videoEl.currentTime;
        const frame = new VideoFrame(this.videoEl, {
          timestamp: Math.round(seconds * 1e6),
        });
        if (this.videoEncoder.encodeQueueSize < 2) {
          this.videoEncoder.encode(frame, {
            keyFrame: this.frameCount % KEYFRAME_EVERY === 0,
          });
          this.frameCount++;
        } else {
          // Encoder is backed up (CPU-bound capture): skip this frame rather
          // than queue it. Counted so the panel can surface capture pressure.
          recordEncodeDrop(this.videoEncoder.encodeQueueSize);
        }
        frame.close();
      } catch (e) {
        this.onStatus(`video frame error: ${e.message ?? e}`);
      }
      this.videoEl.requestVideoFrameCallback(pump);
    };
    this.videoEl.requestVideoFrameCallback(pump);

    // 4. Audio. Configure the Opus encoder once, then start capturing PCM.
    if (audioTrack && "AudioEncoder" in window) {
      this.audioEncoder = new AudioEncoder({
        output: (chunk) => this.#onAudioChunk(chunk),
        error: (e) => this.onStatus(`audio encoder error: ${e.message}`),
      });
      this.audioEncoder.configure({
        codec: AUDIO_CODEC,
        sampleRate,
        numberOfChannels: channels,
        bitrate: AUDIO_BITRATE,
      });
      await this.#startAudioCapture(audioTrack);
    } else {
      this.onStatus("publishing video only (audio capture unavailable)");
    }
  }

  /**
   * Hot-swaps the camera the encoder reads from, without disturbing audio or
   * restarting the encoder, so remote video never stalls on a device change.
   * @param {MediaStreamTrack} track
   */
  async switchVideoTrack(track) {
    if (!this.videoEl) return;
    this.videoEl.srcObject = new MediaStream([track]);
    try {
      await this.videoEl.play();
    } catch {
      // autoplay/play race — frames resume on the next rVFC tick
    }
  }

  /**
   * Hot-swaps the microphone feeding the (unchanged) Opus encoder. The capture
   * is re-pointed at the new track while the encoder, its config, and the
   * timestamp cursor stay put, so the published audio stays decodable.
   * @param {MediaStreamTrack} track
   */
  async switchAudioTrack(track) {
    if (!this.audioEncoder) return;
    // Worklet path (WebKit): just re-point the source node at the new track,
    // keeping the same AudioContext (and thus the same sample rate).
    if (this.audioContext && this.audioNode) {
      try {
        this.audioSource?.disconnect();
      } catch {
        // already disconnected
      }
      this.audioSource = this.audioContext.createMediaStreamSource(new MediaStream([track]));
      this.audioSource.connect(this.audioNode);
      return;
    }
    // MediaStreamTrackProcessor path (Chromium): restart the reader.
    if ("MediaStreamTrackProcessor" in window) {
      try {
        await this.audioReader?.cancel();
      } catch {
        // reader already closed
      }
      // eslint-disable-next-line no-undef
      const proc = new MediaStreamTrackProcessor({ track });
      this.audioReader = proc.readable.getReader();
      this.#pumpAudio();
    }
  }

  // #startAudioCapture begins PCM capture via whichever API the webview
  // supports: MediaStreamTrackProcessor (Chromium) or an AudioWorklet
  // (WebKit/WKWebView, which lacks the former).
  /** @param {MediaStreamTrack} audioTrack */
  async #startAudioCapture(audioTrack) {
    if ("MediaStreamTrackProcessor" in window) {
      // eslint-disable-next-line no-undef
      const proc = new MediaStreamTrackProcessor({ track: audioTrack });
      this.audioReader = proc.readable.getReader();
      this.#pumpAudio();
      this.onStatus("publishing video + audio");
    } else if ("AudioWorkletNode" in window && "AudioData" in window) {
      await this.#startAudioWorklet(audioTrack);
      this.onStatus("publishing video + audio");
    } else {
      this.audioEncoder?.close();
      this.audioEncoder = null;
      this.onStatus("publishing video only (audio capture unavailable)");
    }
  }

  // #pumpAudio drains the MediaStreamTrackProcessor reader (Chromium path).
  async #pumpAudio() {
    try {
      while (this.running && this.audioReader && this.audioEncoder) {
        const { value, done } = await this.audioReader.read();
        if (done) break;
        if (this.audioEncoder.encodeQueueSize < 10) {
          this.audioEncoder.encode(value);
        }
        value.close();
      }
    } catch (e) {
      this.onStatus(`audio pump error: ${e.message ?? e}`);
    }
  }

  /**
   * Captures mic PCM via an AudioWorklet — the WebKit-compatible path — and
   * feeds each render quantum to the Opus encoder as AudioData. The encoder
   * buffers internally into 20ms Opus frames, so forwarding raw quanta is fine.
   * @param {MediaStreamTrack} audioTrack
   */
  async #startAudioWorklet(audioTrack) {
    const sampleRate = this.sampleRate;
    const channels = this.channels;
    const ctx = new AudioContext({ sampleRate });
    this.audioContext = ctx;
    // Resume in case the context starts suspended (no direct user gesture here).
    await ctx.resume().catch(() => {});
    await ctx.audioWorklet.addModule("/pcmWorklet.js");

    const source = ctx.createMediaStreamSource(new MediaStream([audioTrack]));
    this.audioSource = source;
    const node = new AudioWorkletNode(ctx, "pcm-capture");
    this.audioNode = node;

    node.port.onmessage = (ev) => {
      if (!this.running || !this.audioEncoder) return;
      const chs = ev.data.channels;
      if (!chs || chs.length === 0) return;
      const frames = chs[0].length;
      // Pack the per-channel Float32 buffers into one planar buffer.
      const planar = new Float32Array(frames * channels);
      for (let c = 0; c < channels; c++) {
        planar.set(chs[c] ?? chs[0], c * frames);
      }
      try {
        const audioData = new AudioData({
          format: "f32-planar",
          sampleRate,
          numberOfFrames: frames,
          numberOfChannels: channels,
          timestamp: Math.round((this.audioSampleCursor / sampleRate) * 1e6),
          data: planar,
        });
        if (this.audioEncoder.encodeQueueSize < 20) {
          this.audioEncoder.encode(audioData);
        }
        audioData.close();
      } catch (e) {
        this.onStatus(`audio data error: ${e.message ?? e}`);
      }
      this.audioSampleCursor += frames;
    };

    // Drive the graph: the worklet writes no output, so connecting it to the
    // destination pulls PCM through with no audible playback (no echo).
    source.connect(node);
    node.connect(ctx.destination);
  }

  /** @param {EncodedVideoChunk} chunk */
  #onVideoChunk(chunk) {
    const b64 = chunkToBase64(chunk);
    const ts = chunk.timestamp;
    const key = chunk.type === "key";
    recordEncodedFrame(chunk.byteLength, key, this.videoEncoder?.encodeQueueSize ?? 0);
    this.videoChain = this.videoChain
      .then(() => SessionService.PublishVideoChunk(b64, ts, key))
      .catch((e) => this.onStatus(`publish video chunk: ${e.message ?? e}`));
  }

  /** @param {EncodedAudioChunk} chunk */
  #onAudioChunk(chunk) {
    const b64 = chunkToBase64(chunk);
    const ts = chunk.timestamp;
    this.audioChain = this.audioChain
      .then(() => SessionService.PublishAudioChunk(b64, ts))
      .catch((e) => this.onStatus(`publish audio chunk: ${e.message ?? e}`));
  }

  async stop() {
    this.running = false;
    publishStopped();
    try {
      await this.audioReader?.cancel();
    } catch {
      // reader already closed
    }
    try {
      this.audioSource?.disconnect();
    } catch {
      // already disconnected
    }
    try {
      this.audioNode?.disconnect();
    } catch {
      // node already disconnected
    }
    try {
      await this.audioContext?.close();
    } catch {
      // context already closed
    }
    try {
      this.videoEncoder?.close();
    } catch {
      // already closed
    }
    try {
      this.audioEncoder?.close();
    } catch {
      // already closed
    }
    if (this.videoEl) {
      this.videoEl.srcObject = null;
      this.videoEl = null;
    }
    this.videoEncoder = null;
    this.audioEncoder = null;
    this.audioReader = null;
    this.audioNode = null;
    this.audioSource = null;
    this.audioContext = null;
  }
}

/**
 * Copies a WebCodecs EncodedVideoChunk/EncodedAudioChunk into a base64 string
 * suitable for transit to Go (where it's base64-decoded back to bytes).
 * @param {EncodedVideoChunk | EncodedAudioChunk} chunk
 */
function chunkToBase64(chunk) {
  const buf = new Uint8Array(chunk.byteLength);
  chunk.copyTo(buf);
  let binary = "";
  for (let i = 0; i < buf.length; i++) {
    binary += String.fromCharCode(buf[i]);
  }
  return btoa(binary);
}
