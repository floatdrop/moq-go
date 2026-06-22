<script>
  import { onMount } from "svelte";
  import { goto } from "$app/navigation";
  import { Events } from "@wailsio/runtime";
  import { SessionService } from "../../bindings/github.com/floatdrop/moq-go/apps/tlmst";
  import { session } from "$lib/stores/session.svelte.js";
  import { Button } from "$lib/components/ui/button/index.js";
  import { Input } from "$lib/components/ui/input/index.js";
  import * as Card from "$lib/components/ui/card/index.js";

  let addr = $state("localhost:4433");
  /** @type {'idle'|'joining'|'connected'|'failed'} */
  let status = $state("idle");
  let error = $state("");
  /** @type {{time: string, level: string, message: string, attrs: Record<string, string>}[]} */
  let logs = $state([]);

  /** @type {HTMLDivElement | undefined} */
  let logPanel = $state();

  // Stream backend log records into the panel. Events.On returns an
  // unsubscribe function, which we call on teardown.
  onMount(() => {
    const off = Events.On("moq:log", (ev) => {
      logs = [...logs, ev.data];
    });
    return off;
  });

  // Auto-scroll to the newest line whenever a log arrives.
  $effect(() => {
    // referencing logs.length makes this effect re-run on every append
    logs.length;
    if (logPanel) {
      logPanel.scrollTop = logPanel.scrollHeight;
    }
  });

  const join = async () => {
    if (status === "joining" || !addr) {
      return;
    }
    logs = [];
    error = "";
    status = "joining";
    try {
      await SessionService.Join(addr);
      status = "connected";
      session.connected = true;
      session.addr = addr;
      await goto("/call");
    } catch (err) {
      status = "failed";
      error = err instanceof Error ? err.message : String(err);
    }
  };

  /** @param {string} level */
  const levelClass = (level) => {
    switch (level) {
      case "ERROR":
        return "text-destructive";
      case "WARN":
        return "text-yellow-600 dark:text-yellow-400";
      case "INFO":
        return "text-foreground";
      default:
        return "text-muted-foreground";
    }
  };

  /** @param {string} time */
  const fmtTime = (time) => {
    const d = new Date(time);
    return isNaN(d.getTime()) ? time : d.toLocaleTimeString();
  };

  const statusLabel = $derived(
    {
      idle: "Not connected",
      joining: "Establishing session…",
      connected: "Session established",
      failed: "Failed to join",
    }[status],
  );
</script>

<div class="flex min-h-screen flex-col items-center justify-center gap-6 p-8">
  <h1 class="text-3xl font-bold tracking-tight">tlmst</h1>

  <Card.Root class="w-full max-w-2xl">
    <Card.Header>
      <Card.Title>Join a relay</Card.Title>
      <Card.Description>
        Establish a MoQ session and watch the handshake unfold.
      </Card.Description>
    </Card.Header>

    <Card.Content class="flex flex-col gap-4">
      <div class="flex gap-2">
        <Input
          aria-label="relay-address"
          bind:value={addr}
          placeholder="Relay address (host:port)"
          autocomplete="off"
          disabled={status === "joining"}
          onkeydown={(e) => e.key === "Enter" && join()}
        />
        <Button
          aria-label="join-btn"
          onclick={join}
          disabled={status === "joining" || !addr}
        >
          {status === "joining" ? "Joining…" : "Join"}
        </Button>
      </div>

      <div class="flex items-center gap-2 text-sm">
        <span
          class="inline-block h-2 w-2 rounded-full"
          class:bg-muted-foreground={status === "idle"}
          class:bg-yellow-500={status === "joining"}
          class:bg-green-500={status === "connected"}
          class:bg-destructive={status === "failed"}
        ></span>
        <span class="text-muted-foreground">{statusLabel}</span>
        {#if error}
          <span class="text-destructive">— {error}</span>
        {/if}
      </div>

      {#if logs.length !== 0}
        <div
          bind:this={logPanel}
          aria-label="log-panel"
          class="bg-muted/50 min-h-16 max-h-64 overflow-y-auto rounded-md border p-3 font-mono text-xs leading-relaxed"
        >
          {#each logs as log, i (i)}
            <div class="flex gap-2 whitespace-pre-wrap break-all">
              <span class="text-muted-foreground shrink-0"
                >{fmtTime(log.time)}</span
              >
              <span class="shrink-0 {levelClass(log.level)}"
                >{log.level.padEnd(5)}</span
              >
              <span class="flex-1">
                {log.message}
                {#each Object.entries(log.attrs ?? {}) as [k, v]}
                  <span><span class="text-muted-foreground">{k}=</span><span>{v}</span></span>&nbsp;
                {/each}
              </span>
            </div>
          {/each}
        </div>
      {/if}
    </Card.Content>
  </Card.Root>
</div>
