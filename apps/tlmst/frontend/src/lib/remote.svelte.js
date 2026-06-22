import { Events } from "@wailsio/runtime";
import { RemotePlayer } from "./remotePlayer.js";

// Reactive list of remote participants, consumed by the call grid.
export const remotes = $state({
  /** @type {{ id: string }[]} */
  list: [],
});

/** @type {Map<string, RemotePlayer>} */
const players = new Map();
/** @type {AudioContext | null} */
let audioCtx = null;
/** @type {(() => void)[]} */
let unsub = [];

// startRemotes wires the backend's remote-media events to per-participant
// decoders. Call it from the call screen before StartSubscribing so no early
// announcements are missed.
export function startRemotes() {
  if (unsub.length) return;
  audioCtx = "AudioContext" in window ? new AudioContext() : null;
  // May start suspended without a direct gesture; resume for playback.
  audioCtx?.resume?.().catch(() => {});
  unsub.push(Events.On("moq:participant-joined", (e) => onJoined(e.data)));
  unsub.push(Events.On("moq:participant-left", (e) => onLeft(e.data)));
  unsub.push(Events.On("moq:media-chunk", (e) => onChunk(e.data)));
}

export function stopRemotes() {
  unsub.forEach((off) => off());
  unsub = [];
  players.forEach((p) => p.close());
  players.clear();
  remotes.list = [];
  audioCtx?.close().catch(() => {});
  audioCtx = null;
}

// attachCanvas connects a participant's tile canvas to its decoder.
/** @param {string} id @param {HTMLCanvasElement} canvas */
export function attachCanvas(id, canvas) {
  players.get(id)?.setCanvas(canvas);
}

/** @param {{id: string, video?: any, audio?: any}} p */
function onJoined(p) {
  if (players.has(p.id)) return;
  players.set(p.id, new RemotePlayer(p.id, p.video ?? null, p.audio ?? null, audioCtx));
  remotes.list = [...remotes.list, { id: p.id }];
}

/** @param {{id: string}} p */
function onLeft(p) {
  players.get(p.id)?.close();
  players.delete(p.id);
  remotes.list = remotes.list.filter((r) => r.id !== p.id);
}

/** @param {{participantId: string, kind: "video"|"audio", data: string, timestampMicros: number, keyframe: boolean, groupId: number, objectId: number}} c */
function onChunk(c) {
  players
    .get(c.participantId)
    ?.pushChunk(c.kind, c.data, c.timestampMicros, c.keyframe, c.groupId, c.objectId);
}
