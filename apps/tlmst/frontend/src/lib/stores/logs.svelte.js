import { Events } from "@wailsio/runtime";

// Shared, app-wide capture of backend log records (the "moq:log" event). Lives
// at module scope so it persists across navigation and accumulates everything
// from app start — session handshake, publisher, and subscriber alike.
export const logStore = $state({
  /** @type {{time: string, level: string, message: string, attrs: Record<string, string>}[]} */
  entries: [],
});

const MAX_ENTRIES = 1000;
let started = false;

// startLogCapture subscribes to backend logs exactly once. Safe to call from
// multiple places; subsequent calls are no-ops.
export function startLogCapture() {
  if (started) return;
  started = true;
  Events.On("moq:log", (e) => {
    const next = [...logStore.entries, e.data];
    if (next.length > MAX_ENTRIES) {
      next.splice(0, next.length - MAX_ENTRIES);
    }
    logStore.entries = next;
  });
}

export function clearLogs() {
  logStore.entries = [];
}

// pushLog appends a frontend-originated entry to the same panel, so decode/
// render diagnostics sit alongside the backend logs.
/** @param {string} level @param {string} message @param {Record<string, string|number|boolean>} [attrs] */
export function pushLog(level, message, attrs = {}) {
  const stringAttrs = {};
  for (const [k, v] of Object.entries(attrs)) stringAttrs[k] = String(v);
  const next = [...logStore.entries, {
    time: new Date().toISOString(),
    level,
    message: `[web] ${message}`,
    attrs: stringAttrs,
  }];
  if (next.length > MAX_ENTRIES) next.splice(0, next.length - MAX_ENTRIES);
  logStore.entries = next;
}
