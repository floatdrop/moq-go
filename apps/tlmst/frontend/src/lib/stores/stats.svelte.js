// Frontend-side codec counters for the debug panel. The WebCodecs encoder and
// decoders live in the browser, so their stats can't come from the Go
// SessionService.Stats() call — they're collected here instead.
//
// Producers (MediaPublisher, RemotePlayer) only bump cumulative counters and
// gauges; the debug panel diffs successive samples on its poll tick to derive
// rates (fps, bitrate). Keeping all rate math in the panel means the producers
// stay timer-free and there's a single, consistent sampling interval shared
// with the QUIC stats.

/**
 * @typedef {Object} RemoteStat
 * @property {number} framesDecoded   frames handed to the canvas
 * @property {number} framesDropped   frames dropped by decode-order gating
 * @property {number} decodeErrors    decoder error / throw count
 * @property {number} decodeQueue     current VideoDecoder.decodeQueueSize
 * @property {number} width           last decoded frame width
 * @property {number} height          last decoded frame height
 */

export const codecStats = $state({
  publish: {
    active: false,
    framesEncoded: 0,
    framesDropped: 0,
    keyframes: 0,
    bytesEncoded: 0,
    encodeQueue: 0,
    width: 0,
    height: 0,
  },
  /** @type {Record<string, RemoteStat>} */
  remotes: {},
});

/** Reset publish counters at the start of a publishing session. */
export function publishStarted(width, height) {
  Object.assign(codecStats.publish, {
    active: true,
    framesEncoded: 0,
    framesDropped: 0,
    keyframes: 0,
    bytesEncoded: 0,
    encodeQueue: 0,
    width,
    height,
  });
}

export function publishStopped() {
  codecStats.publish.active = false;
}

/** @param {number} bytes @param {boolean} keyframe @param {number} queue */
export function recordEncodedFrame(bytes, keyframe, queue) {
  const p = codecStats.publish;
  p.framesEncoded++;
  p.bytesEncoded += bytes;
  if (keyframe) p.keyframes++;
  p.encodeQueue = queue;
}

/** @param {number} queue */
export function recordEncodeDrop(queue) {
  codecStats.publish.framesDropped++;
  codecStats.publish.encodeQueue = queue;
}

/**
 * Returns the (reactive) stat entry for a remote participant, creating it on
 * first use. The caller mutates the returned object directly; it's part of the
 * $state proxy, so the panel re-renders.
 * @param {string} id
 * @returns {RemoteStat}
 */
export function ensureRemoteStat(id) {
  if (!codecStats.remotes[id]) {
    codecStats.remotes[id] = {
      framesDecoded: 0,
      framesDropped: 0,
      decodeErrors: 0,
      decodeQueue: 0,
      width: 0,
      height: 0,
    };
  }
  return codecStats.remotes[id];
}

/** @param {string} id */
export function removeRemoteStat(id) {
  delete codecStats.remotes[id];
}
