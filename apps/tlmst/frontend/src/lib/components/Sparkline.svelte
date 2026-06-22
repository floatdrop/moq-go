<script>
// A dependency-free SVG sparkline. `data` is the most-recent-last series; the
// viewBox is stretched to the container width via preserveAspectRatio="none",
// and the stroke is kept crisp with vector-effect so the line doesn't scale.
let {
  data = [],
  height = 32,
  color = "currentColor",
  /** Force the y-axis floor/ceiling; defaults to the data's own min/max. */
  min = undefined,
  max = undefined,
} = $props();

// Fixed internal coordinate space; CSS scales it to the real width.
const W = 100;

const points = $derived.by(() => {
  if (data.length === 0) return "";
  const lo = min ?? Math.min(...data);
  const hi = max ?? Math.max(...data);
  const range = hi - lo || 1;
  const stepX = data.length > 1 ? W / (data.length - 1) : 0;
  return data
    .map((v, i) => {
      const x = i * stepX;
      const y = height - ((v - lo) / range) * height;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
});
</script>

<svg
  viewBox="0 0 {W} {height}"
  preserveAspectRatio="none"
  class="block w-full"
  style:height="{height}px"
  aria-hidden="true"
>
  {#if points}
    <polyline
      {points}
      fill="none"
      stroke={color}
      stroke-width="1.5"
      stroke-linejoin="round"
      vector-effect="non-scaling-stroke"
    />
  {/if}
</svg>
