<script>
// A single tile in the call grid. It draws a MediaStream onto a <canvas> via
// a requestAnimationFrame loop. Remote participants will reuse this component
// once SUBSCRIBE_NAMESPACE delivers their decoded frames as MediaStreams.
let { stream = null, label = "", muted = true } = $props();

/** @type {HTMLCanvasElement | undefined} */
let canvas = $state();
/** @type {HTMLVideoElement | undefined} */
let video = $state();

// Feed the stream into the hidden <video>, which decodes it for the canvas.
$effect(() => {
  if (!video) return;
  video.srcObject = stream;
  if (stream) {
    video.play().catch(() => {});
  }
});

// Draw the decoded frames onto the canvas, matching the source resolution so
// the image stays crisp; CSS handles the on-screen sizing.
$effect(() => {
  if (!canvas || !video) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  let frame = 0;
  const draw = () => {
    if (video.videoWidth && video.videoHeight) {
      if (canvas.width !== video.videoWidth) canvas.width = video.videoWidth;
      if (canvas.height !== video.videoHeight) canvas.height = video.videoHeight;
      ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    }
    frame = requestAnimationFrame(draw);
  };
  frame = requestAnimationFrame(draw);
  return () => cancelAnimationFrame(frame);
});
</script>

<div class="bg-muted relative aspect-video overflow-hidden rounded-lg border">
  <!-- Hidden decoder source; the canvas is what the user sees. -->
  <!-- svelte-ignore a11y_media_has_caption -->
  <video bind:this={video} {muted} playsinline class="hidden"></video>

  {#if stream}
    <canvas bind:this={canvas} class="h-full w-full object-contain"></canvas>
  {:else}
    <div class="text-muted-foreground flex h-full w-full items-center justify-center text-sm">
      No video
    </div>
  {/if}

  {#if label}
    <span
      class="bg-background/70 text-foreground absolute bottom-2 left-2 rounded px-2 py-0.5 text-xs backdrop-blur-sm"
    >
      {label}
    </span>
  {/if}
</div>
