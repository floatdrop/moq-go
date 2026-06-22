<script>
import { attachCanvas } from "$lib/remote.svelte.js";

let { id, label = "" } = $props();

/** @type {HTMLCanvasElement | undefined} */
let canvas = $state();

// Hand the canvas to this participant's decoder so it can draw decoded frames.
$effect(() => {
  if (canvas) attachCanvas(id, canvas);
});
</script>

<div class="bg-muted relative aspect-video overflow-hidden rounded-lg border">
  <canvas bind:this={canvas} class="h-full w-full object-contain"></canvas>
  <span
    class="bg-background/70 text-foreground absolute bottom-2 left-2 rounded px-2 py-0.5 text-xs backdrop-blur-sm"
  >
    {label || id}
  </span>
</div>
