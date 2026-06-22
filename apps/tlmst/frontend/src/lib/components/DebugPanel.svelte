<script>
import * as Dialog from "$lib/components/ui/dialog/index.js";
import Sparkline from "$lib/components/Sparkline.svelte";
import { codecStats } from "$lib/stores/stats.svelte.js";
import { SessionService } from "../../../bindings/github.com/floatdrop/moq-go/apps/tlmst/index.js";

let { open = $bindable(false), addr = "" } = $props();

const HISTORY = 60; // ~1 minute at the 1s poll cadence

// Latest numeric readouts (gauges + derived rates), refreshed each poll.
let stat = $state({
  connected: false,
  rtt: 0,
  rttLatest: 0,
  rttMin: 0,
  lossPct: 0,
  upKbps: 0,
  downKbps: 0,
  cwndKB: 0,
  inFlightKB: 0,
  packetsSent: 0,
  packetsReceived: 0,
  packetsLost: 0,
  pubFps: 0,
  pubKbps: 0,
});

// Rolling series for the sparklines.
let history = $state({
  rtt: /** @type {number[]} */ ([]),
  loss: /** @type {number[]} */ ([]),
  up: /** @type {number[]} */ ([]),
  down: /** @type {number[]} */ ([]),
});

/** @param {number[]} arr @param {number} v */
function push(arr, v) {
  arr.push(v);
  if (arr.length > HISTORY) arr.shift();
}

// Per-remote decode fps, derived from frame counters across polls.
let remoteFps = $state(/** @type {Record<string, number>} */ ({}));

// Poll the backend QUIC stats and recompute rates while the panel is open.
$effect(() => {
  if (!open) return;

  /** @type {any} */ let prevConn = null;
  let prevPubFrames = 0;
  let prevPubBytes = 0;
  /** @type {Record<string, number>} */ let prevDecoded = {};
  let prevT = 0;

  const tick = async () => {
    let conn;
    try {
      conn = await SessionService.Stats();
    } catch {
      return;
    }
    const now = performance.now();
    const dt = prevT ? (now - prevT) / 1000 : 0;

    stat.connected = conn.connected;
    stat.rtt = conn.smoothedRttMs;
    stat.rttLatest = conn.latestRttMs;
    stat.rttMin = conn.minRttMs;
    stat.cwndKB = conn.congestionBytes / 1024;
    stat.inFlightKB = conn.bytesInFlight / 1024;
    stat.packetsSent = conn.packetsSent;
    stat.packetsReceived = conn.packetsReceived;
    stat.packetsLost = conn.packetsLost;

    if (prevConn && dt > 0) {
      const dSent = conn.packetsSent - prevConn.packetsSent;
      const dLost = conn.packetsLost - prevConn.packetsLost;
      stat.lossPct = dSent > 0 ? Math.max(0, (dLost / dSent) * 100) : 0;
      stat.upKbps = ((conn.bytesSent - prevConn.bytesSent) * 8) / 1000 / dt;
      stat.downKbps = ((conn.bytesReceived - prevConn.bytesReceived) * 8) / 1000 / dt;

      // Publish-side fps/bitrate from the WebCodecs counters.
      const p = codecStats.publish;
      stat.pubFps = Math.max(0, (p.framesEncoded - prevPubFrames) / dt);
      stat.pubKbps = Math.max(0, ((p.bytesEncoded - prevPubBytes) * 8) / 1000 / dt);

      const fps = {};
      for (const [id, r] of Object.entries(codecStats.remotes)) {
        const prev = prevDecoded[id] ?? r.framesDecoded;
        fps[id] = Math.max(0, (r.framesDecoded - prev) / dt);
      }
      remoteFps = fps;

      push(history.rtt, stat.rtt);
      push(history.loss, stat.lossPct);
      push(history.up, stat.upKbps);
      push(history.down, stat.downKbps);
    }

    prevConn = conn;
    prevPubFrames = codecStats.publish.framesEncoded;
    prevPubBytes = codecStats.publish.bytesEncoded;
    prevDecoded = Object.fromEntries(
      Object.entries(codecStats.remotes).map(([id, r]) => [id, r.framesDecoded]),
    );
    prevT = now;
  };

  tick();
  const h = setInterval(tick, 1000);
  return () => clearInterval(h);
});

/** @param {number} n @param {number} [d] */
const fmt = (n, d = 0) => (Number.isFinite(n) ? n.toFixed(d) : "—");

const remoteEntries = $derived(Object.entries(codecStats.remotes));
</script>

<Dialog.Root bind:open>
  <Dialog.Content class="sm:max-w-2xl">
    <Dialog.Header>
      <Dialog.Title>Connection stats</Dialog.Title>
      <Dialog.Description>
        QUIC transport and codec metrics for <span class="font-mono">{addr}</span>.
      </Dialog.Description>
    </Dialog.Header>

    <div class="grid max-h-[70vh] gap-4 overflow-y-auto pr-1">
      <!-- Transport plots -->
      <div class="grid grid-cols-2 gap-4">
        <div class="rounded-md border p-3">
          <div class="flex items-baseline justify-between">
            <span class="text-muted-foreground text-xs">RTT (smoothed)</span>
            <span class="font-mono text-sm">{fmt(stat.rtt, 1)} ms</span>
          </div>
          <Sparkline data={history.rtt} color="#34d399" min={0} />
          <div class="text-muted-foreground mt-1 font-mono text-[10px]">
            latest {fmt(stat.rttLatest, 1)} · min {fmt(stat.rttMin, 1)} ms
          </div>
        </div>

        <div class="rounded-md border p-3">
          <div class="flex items-baseline justify-between">
            <span class="text-muted-foreground text-xs">Packet loss</span>
            <span class="font-mono text-sm">{fmt(stat.lossPct, 2)} %</span>
          </div>
          <Sparkline data={history.loss} color="#f87171" min={0} />
          <div class="text-muted-foreground mt-1 font-mono text-[10px]">
            {stat.packetsLost} lost / {stat.packetsSent} sent
          </div>
        </div>

        <div class="rounded-md border p-3">
          <div class="flex items-baseline justify-between">
            <span class="text-muted-foreground text-xs">Upload</span>
            <span class="font-mono text-sm">{fmt(stat.upKbps)} kbps</span>
          </div>
          <Sparkline data={history.up} color="#60a5fa" min={0} />
        </div>

        <div class="rounded-md border p-3">
          <div class="flex items-baseline justify-between">
            <span class="text-muted-foreground text-xs">Download</span>
            <span class="font-mono text-sm">{fmt(stat.downKbps)} kbps</span>
          </div>
          <Sparkline data={history.down} color="#a78bfa" min={0} />
        </div>
      </div>

      <!-- Transport gauges -->
      <div class="text-muted-foreground grid grid-cols-3 gap-2 font-mono text-[11px]">
        <div>cwnd <span class="text-foreground">{fmt(stat.cwndKB, 1)} KB</span></div>
        <div>in-flight <span class="text-foreground">{fmt(stat.inFlightKB, 1)} KB</span></div>
        <div>recv <span class="text-foreground">{stat.packetsReceived}</span> pkts</div>
      </div>

      <!-- Publish -->
      <div class="rounded-md border p-3">
        <h3 class="mb-1 text-sm font-medium">Publishing</h3>
        {#if codecStats.publish.active}
          <div class="text-muted-foreground grid grid-cols-2 gap-x-4 gap-y-1 font-mono text-xs">
            <div>resolution <span class="text-foreground">{codecStats.publish.width}×{codecStats.publish.height}</span></div>
            <div>fps <span class="text-foreground">{fmt(stat.pubFps, 1)}</span></div>
            <div>bitrate <span class="text-foreground">{fmt(stat.pubKbps)} kbps</span></div>
            <div>encode queue <span class="text-foreground">{codecStats.publish.encodeQueue}</span></div>
            <div>keyframes <span class="text-foreground">{codecStats.publish.keyframes}</span></div>
            <div>frames dropped <span class="text-foreground">{codecStats.publish.framesDropped}</span></div>
          </div>
        {:else}
          <p class="text-muted-foreground text-xs">Not publishing.</p>
        {/if}
      </div>

      <!-- Remote decoders -->
      <div class="rounded-md border p-3">
        <h3 class="mb-1 text-sm font-medium">Remote decoders</h3>
        {#if remoteEntries.length === 0}
          <p class="text-muted-foreground text-xs">No remote participants.</p>
        {:else}
          <div class="grid gap-2">
            {#each remoteEntries as [id, r] (id)}
              <div class="text-muted-foreground grid grid-cols-3 gap-x-4 gap-y-1 font-mono text-xs">
                <div class="text-foreground">{id}</div>
                <div>{r.width}×{r.height}</div>
                <div>{fmt(remoteFps[id] ?? 0, 1)} fps</div>
                <div>decoded <span class="text-foreground">{r.framesDecoded}</span></div>
                <div>dropped <span class="text-foreground">{r.framesDropped}</span></div>
                <div>errors <span class="text-foreground">{r.decodeErrors}</span></div>
              </div>
            {/each}
          </div>
        {/if}
      </div>
    </div>
  </Dialog.Content>
</Dialog.Root>
