<script>
import { onMount, onDestroy } from "svelte";
import { goto } from "$app/navigation";
import { session } from "$lib/stores/session.svelte.js";
import { logStore, clearLogs } from "$lib/stores/logs.svelte.js";
import { SessionService } from "../../../bindings/github.com/floatdrop/moq-go/apps/tlmst";
import { MediaPublisher } from "$lib/mediaPublisher.js";
import { remotes, startRemotes, stopRemotes } from "$lib/remote.svelte.js";
import VideoTile from "$lib/components/VideoTile.svelte";
import RemoteTile from "$lib/components/RemoteTile.svelte";
import DebugPanel from "$lib/components/DebugPanel.svelte";
import { Button } from "$lib/components/ui/button/index.js";
import * as Select from "$lib/components/ui/select/index.js";
import * as Dialog from "$lib/components/ui/dialog/index.js";
import Video from "@lucide/svelte/icons/video";
import Mic from "@lucide/svelte/icons/mic";
import Settings from "@lucide/svelte/icons/settings";
import Activity from "@lucide/svelte/icons/activity";
import PhoneOff from "@lucide/svelte/icons/phone-off";

/** @type {MediaDeviceInfo[]} */
let videoDevices = $state([]);
/** @type {MediaDeviceInfo[]} */
let audioDevices = $state([]);
let selectedVideo = $state("");
let selectedAudio = $state("");

/** @type {MediaStream | null} */
let localStream = $state(null);

/** @type {import('$lib/mediaPublisher.js').MediaPublisher | null} */
let publisher = null;
let publishStatus = $state("");

let mediaError = $state("");
let settingsOpen = $state(false);
let statsOpen = $state(false);

/** @type {HTMLDivElement | undefined} */
let logPanel = $state();

// Auto-scroll the debug log to the newest line while the dialog is open.
$effect(() => {
  logStore.entries.length;
  if (logPanel) {
    logPanel.scrollTop = logPanel.scrollHeight;
  }
});

/** @param {string} level */
const logLevelClass = (level) => {
  switch (level) {
    case "ERROR": return "text-red-400";
    case "WARN": return "text-amber-400";
    case "INFO": return "text-emerald-400";
    default: return "text-slate-400";
  }
};

/** @param {string} time */
const fmtLogTime = (time) => {
  const d = new Date(time);
  return isNaN(d.getTime()) ? time : d.toLocaleTimeString();
};

// The call grid: the local camera tile first, then a tile per remote peer.
const tiles = $derived([
  { id: "local", remote: false, label: "You", stream: localStream },
  ...remotes.list.map((r) => ({ id: r.id, remote: true, label: r.id, stream: null })),
]);

// ----- Zoom-style gallery layout --------------------------------------------
// The classic auto-fit grid only fills the first row, leaving big gaps below
// when participants are few or the viewport is wide. Instead, given N tiles
// and the current container size, pick the (rows, cols) pair that maximises
// the size of one tile when each tile is laid out at a fixed 16:9 aspect
// ratio. This is the heuristic Zoom / Meet / Teams all use.

const TILE_ASPECT = 16 / 9;

/** @type {HTMLDivElement | undefined} */
let gridEl = $state();
let gridW = $state(0);
let gridH = $state(0);

// Observe the grid's content box so re-tiling reacts to window resizes,
// devtools toggling, the bottom bar appearing, etc.
$effect(() => {
  if (!gridEl) return;
  const ro = new ResizeObserver((entries) => {
    const r = entries[0]?.contentRect;
    if (!r) return;
    gridW = r.width;
    gridH = r.height;
  });
  ro.observe(gridEl);
  return () => ro.disconnect();
});

/**
 * Given N tiles and the container's pixel size, pick rows/cols that
 * maximise per-tile area while preserving each tile's 16:9 aspect ratio.
 * Returns { rows, cols } with rows * cols >= N.
 *
 * @param {number} n
 * @param {number} W
 * @param {number} H
 */
function pickLayout(n, W, H) {
  if (n <= 0 || W <= 0 || H <= 0) return { rows: 1, cols: 1 };

  let best = { rows: 1, cols: n, area: -1 };
  // Try every (rows, cols) with rows * cols >= n. n is the participant
  // count — single digits in practice, up to a few dozen at the extreme,
  // so the O(n) sweep is negligible.
  for (let rows = 1; rows <= n; rows++) {
    const cols = Math.ceil(n / rows);
    // Tile size constrained by both width-per-column and height-per-row,
    // keeping the 16:9 aspect ratio. The smaller dimension wins.
    const tileW = Math.min(W / cols, (H / rows) * TILE_ASPECT);
    const area = tileW * (tileW / TILE_ASPECT);
    if (area > best.area) best = { rows, cols, area };
  }
  return { rows: best.rows, cols: best.cols };
}

const layout = $derived(pickLayout(tiles.length, gridW, gridH));

// 0.75rem at the default 16px root font size. Kept as a constant rather
// than read off the DOM so the geometry calculation stays cheap and
// doesn't need a second relayout to settle. Update both this and the
// gap class on the grid element if you change one.
const GRID_GAP_PX = 12;

// Concrete pixel size for each tile. Computing this here (rather than
// relying on CSS `aspect-ratio` + `max-width/height: 100%`) avoids two
// CSS pitfalls: (1) the tile components already set `aspect-video` on
// their roots, so a second aspect-ratio constraint on the wrapper
// would compete; (2) Chromium and WebKit disagree on how
// `aspect-ratio` interacts with a flex parent when both width and
// height are constrained. Fixed pixel dimensions sidestep both.
const tileSize = $derived.by(() => {
  const { rows, cols } = layout;
  if (gridW <= 0 || gridH <= 0) return { w: 0, h: 0 };
  const cellW = (gridW - GRID_GAP_PX * (cols - 1)) / cols;
  const cellH = (gridH - GRID_GAP_PX * (rows - 1)) / rows;
  const w = Math.max(0, Math.min(cellW, cellH * TILE_ASPECT));
  return { w, h: w / TILE_ASPECT };
});


const videoLabel = $derived(
  videoDevices.find((d) => d.deviceId === selectedVideo)?.label || "Camera",
);
const audioLabel = $derived(
  audioDevices.find((d) => d.deviceId === selectedAudio)?.label || "Microphone",
);

/** @param {MediaStream | null} s */
function stopStream(s) {
  s?.getTracks().forEach((t) => t.stop());
}

async function startCamera() {
  mediaError = "";
  if (!navigator.mediaDevices?.getUserMedia) {
    mediaError = "Media devices are not available in this environment.";
    return;
  }
  try {
    // `ideal` (not `exact`) on width/height/frameRate: ask for 1080p30 but
    // let the user agent fall back if the camera can't deliver it.
    // Without these constraints browsers default to 640x480, which then
    // pins the WebCodecs encoder + the announced VideoConfig at 640x480
    // even on a 1080p-capable webcam.
    const videoConstraints = {
      width:     { ideal: 1280 },
      height:    { ideal: 720 },
      frameRate: { ideal: 30 },
      ...(selectedVideo ? { deviceId: { exact: selectedVideo } } : {}),
    };
    const next = await navigator.mediaDevices.getUserMedia({
      video: videoConstraints,
      audio: selectedAudio ? { deviceId: { exact: selectedAudio } } : true,
    });
    stopStream(localStream);
    localStream = next;

    // Device labels are only populated once permission has been granted.
    const devices = await navigator.mediaDevices.enumerateDevices();
    videoDevices = devices.filter((d) => d.kind === "videoinput");
    audioDevices = devices.filter((d) => d.kind === "audioinput");
    if (!selectedVideo) {
      selectedVideo =
        next.getVideoTracks()[0]?.getSettings().deviceId ??
        videoDevices[0]?.deviceId ??
        "";
    }
    if (!selectedAudio) {
      selectedAudio =
        next.getAudioTracks()[0]?.getSettings().deviceId ??
        audioDevices[0]?.deviceId ??
        "";
    }
  } catch (err) {
    mediaError = err instanceof Error ? err.message : String(err);
  }
}

/** @param {string} deviceId */
function selectVideo(deviceId) {
  selectedVideo = deviceId;
  switchDevice("video", deviceId);
}

/** @param {string} deviceId */
function selectAudio(deviceId) {
  selectedAudio = deviceId;
  switchDevice("audio", deviceId);
}

// Switch a single capture device in place: acquire only the changed track,
// hand it to the running publisher (so its encoders never stop), then rebuild
// the preview stream and stop only the replaced track. The other track — and
// the publisher's video pipeline — is left untouched, so remote media keeps
// flowing across the switch.
/** @param {"video" | "audio"} kind @param {string} deviceId */
async function switchDevice(kind, deviceId) {
  if (!navigator.mediaDevices?.getUserMedia) return;
  try {
    // Mirror the resolution constraints used by startCamera so a device
    // hot-swap keeps the capture at 1080p instead of silently regressing
    // to the browser's 640x480 default.
    const constraints =
      kind === "video"
        ? {
            video: {
              deviceId:  { exact: deviceId },
              width:     { ideal: 1280 },
              height:    { ideal: 720 },
              frameRate: { ideal: 30 },
            },
          }
        : { audio: { deviceId: { exact: deviceId } } };
    const tmp = await navigator.mediaDevices.getUserMedia(constraints);
    const newTrack = (kind === "video" ? tmp.getVideoTracks() : tmp.getAudioTracks())[0];
    if (!newTrack) return;

    const oldTrack = (
      kind === "video" ? localStream?.getVideoTracks() : localStream?.getAudioTracks()
    )?.[0];

    if (kind === "video") await publisher?.switchVideoTrack(newTrack);
    else await publisher?.switchAudioTrack(newTrack);

    // Rebuild the preview stream (new reference so the local tile re-binds),
    // keeping the unchanged track.
    const keep =
      kind === "video" ? (localStream?.getAudioTracks() ?? []) : (localStream?.getVideoTracks() ?? []);
    localStream = new MediaStream([newTrack, ...keep]);
    oldTrack?.stop();
  } catch (err) {
    mediaError = err instanceof Error ? err.message : String(err);
  }
}

async function leave() {
  await publisher?.stop();
  publisher = null;
  stopRemotes();
  stopStream(localStream);
  localStream = null;
  try {
    await SessionService.Leave();
  } catch {
    // surface nothing — we're tearing down regardless
  }
  session.connected = false;
  session.addr = "";
  await goto("/");
}

onMount(() => {
  // Guard the route: reaching /call without an established session sends the
  // user back to the join screen (e.g. on a hard reload).
  if (!session.connected) {
    goto("/");
    return;
  }
  // Attach remote-media listeners before discovery starts so no peer
  // announcements are missed.
  startRemotes();
  (async () => {
    await startCamera();
    if (!localStream) return;
    try {
      publisher = new MediaPublisher(localStream, (msg) => (publishStatus = msg));
      await publisher.start();
    } catch (err) {
      publishStatus = err instanceof Error ? err.message : String(err);
    }
    // Begin discovering and subscribing to other participants.
    try {
      await SessionService.StartSubscribing();
    } catch (err) {
      publishStatus = err instanceof Error ? err.message : String(err);
    }
  })();
});

onDestroy(() => {
  publisher?.stop();
  stopRemotes();
  stopStream(localStream);
});
</script>

<div class="flex h-screen flex-col pt-8">
  <!-- Participant grid: Zoom-style gallery. The cell grid is sized
       (rows × cols) for the current tile count + container size so the
       overall block always fills the viewport, and each cell holds a
       16:9 tile centred inside it. -->
  <div
    bind:this={gridEl}
    class="min-h-0 flex-1 overflow-hidden p-3"
    style:display="grid"
    style:gap="{GRID_GAP_PX}px"
    style:grid-template-rows="repeat({layout.rows}, minmax(0, 1fr))"
    style:grid-template-columns="repeat({layout.cols}, minmax(0, 1fr))"
  >
    {#each tiles as tile (tile.id)}
      <!-- Each grid cell flex-centres a fixed-pixel-size tile wrapper.
           The tile components (VideoTile / RemoteTile) inherit
           width/height: 100% through their existing `h-full w-full`
           canvas, so the wrapper's pixel size propagates all the way
           down to the rendered surface. -->
      <div class="flex min-h-0 min-w-0 items-center justify-center">
        <div style:width="{tileSize.w}px" style:height="{tileSize.h}px">
          {#if tile.remote}
            <RemoteTile id={tile.id} label={tile.label} />
          {:else}
            <VideoTile stream={tile.stream} label={tile.label} muted={true} />
          {/if}
        </div>
      </div>
    {/each}
  </div>

  {#if mediaError}
    <p class="text-destructive px-4 pb-2 text-sm">{mediaError}</p>
  {/if}
  {#if publishStatus}
    <p class="text-muted-foreground px-4 pb-2 text-sm">{publishStatus}</p>
  {/if}

  <!-- Bottom control panel -->
  <div class="bg-background flex items-center gap-2 border-t p-3">
    <div class="flex items-center gap-2">
      <Select.Root type="single" value={selectedVideo} onValueChange={selectVideo}>
        <Select.Trigger class="w-[200px]">
          <Video class="size-4" />
          <span class="truncate">{videoLabel}</span>
        </Select.Trigger>
        <Select.Content>
          {#each videoDevices as device (device.deviceId)}
            <Select.Item value={device.deviceId} label={device.label}>
              {device.label || "Camera"}
            </Select.Item>
          {/each}
        </Select.Content>
      </Select.Root>

      <Select.Root type="single" value={selectedAudio} onValueChange={selectAudio}>
        <Select.Trigger class="w-[200px]">
          <Mic class="size-4" />
          <span class="truncate">{audioLabel}</span>
        </Select.Trigger>
        <Select.Content>
          {#each audioDevices as device (device.deviceId)}
            <Select.Item value={device.deviceId} label={device.label}>
              {device.label || "Microphone"}
            </Select.Item>
          {/each}
        </Select.Content>
      </Select.Root>
    </div>

    <div class="ml-auto flex items-center gap-2">
      <Button variant="outline" size="icon" onclick={() => (statsOpen = true)} aria-label="stats">
        <Activity class="size-4" />
      </Button>

      <Button variant="outline" size="icon" onclick={() => (settingsOpen = true)} aria-label="settings">
        <Settings class="size-4" />
      </Button>

      <Button variant="destructive" onclick={leave} aria-label="leave">
        <PhoneOff class="size-4" />
        Leave
      </Button>
    </div>
  </div>
</div>

<DebugPanel bind:open={statsOpen} addr={session.addr} />

<Dialog.Root bind:open={settingsOpen}>
  <Dialog.Content class="sm:max-w-3xl">
    <Dialog.Header>
      <Dialog.Title>Settings</Dialog.Title>
      <Dialog.Description>
        Connected to <span class="font-mono">{session.addr}</span>.
      </Dialog.Description>
    </Dialog.Header>

    <div class="flex items-center justify-between">
      <h3 class="text-sm font-medium">Debug logs</h3>
      <Button variant="outline" size="sm" onclick={clearLogs}>Clear</Button>
    </div>

    <div
      bind:this={logPanel}
      aria-label="debug-log-panel"
      class="h-80 overflow-y-auto rounded-md border border-neutral-800 bg-black p-3 font-mono text-xs leading-relaxed"
    >
      {#if logStore.entries.length === 0}
        <p class="text-slate-500">No logs yet.</p>
      {:else}
        {#each logStore.entries as log, i (i)}
          <div class="flex gap-2 whitespace-pre-wrap break-all">
            <span class="shrink-0 text-slate-500">{fmtLogTime(log.time)}</span>
            <span class="shrink-0 {logLevelClass(log.level)}">{log.level.padEnd(5)}</span>
            <span class="flex-1 text-slate-200">
              {log.message}
              {#each Object.entries(log.attrs ?? {}) as [k, v]}
                <span class="text-slate-500"> {k}=</span><span class="text-slate-300">{v}</span>
              {/each}
            </span>
          </div>
        {/each}
      {/if}
    </div>
  </Dialog.Content>
</Dialog.Root>
