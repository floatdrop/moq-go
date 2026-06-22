<script lang="ts">
	import './layout.css';
	import { onMount } from 'svelte';
	import { startLogCapture } from '$lib/stores/logs.svelte.js';

	const { children } = $props();

	// Capture backend logs from app start so the debug panel has full history.
	onMount(() => startLogCapture());
</script>

{@render children()}
