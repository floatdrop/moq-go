// Shared session state. This is a plain reactive object that survives
// client-side navigation (the app is an SPA, so the module is not re-evaluated
// when moving between routes). The Go backend owns the real MoQ session; this
// just tracks enough for the UI to gate the /call route and show the relay.
export const session = $state({
	/** @type {boolean} whether a MoQ session is currently established */
	connected: false,
	/** @type {string} the relay address that was joined */
	addr: "",
});
