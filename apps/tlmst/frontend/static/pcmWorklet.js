// AudioWorklet processor that forwards captured microphone PCM to the main
// thread. Used as the WebKit/WKWebView fallback for audio capture, where
// MediaStreamTrackProcessor is unavailable.
//
// Each render quantum delivers up to 128 frames per channel. The input
// buffers are reused across quanta, so we copy them before transferring the
// backing ArrayBuffers to the main thread (zero-copy hand-off).
class PCMCaptureProcessor extends AudioWorkletProcessor {
  process(inputs) {
    const input = inputs[0];
    if (input && input.length > 0 && input[0].length > 0) {
      const channels = input.map((channel) => {
        const copy = new Float32Array(channel.length);
        copy.set(channel);
        return copy;
      });
      this.port.postMessage(
        { channels },
        channels.map((c) => c.buffer),
      );
    }
    // Returning true keeps the processor alive even during input gaps.
    return true;
  }
}

registerProcessor("pcm-capture", PCMCaptureProcessor);
