package loc

// Object is one LOC-packaged media chunk: the LOC Public Properties
// that travel in the MOQ Object Properties block, plus the codec
// elementary stream bytes that travel in the MOQ Object Payload.
//
// Object does not own a MOQ message — it produces the bytes a caller
// drops into a [github.com/floatdrop/moq-go/pkg/moqt/message.SubgroupObject]
// (or any other MOQ message that carries an object payload). The
// caller controls Group ID, Object ID, subgroup framing, and stream
// scheduling.
//
// LOC Private Properties (SecureObjects, §3.1.3) are not modelled
// here. When that support lands, Object grows a Private field and
// Encode/Decode learn the length-prefixed-prepended-to-payload layout
// the SecureObjects spec defines.
type Object struct {
	Properties Properties
	// Payload is the codec elementary stream — the "internal data" of
	// an EncodedAudioChunk / EncodedVideoChunk in the WebCodecs Codec
	// Registry. Nil and empty are equivalent.
	Payload []byte
}

// Encode returns the bytes ready to plug into a MOQ Object:
//
//   - props goes into the surrounding message's Properties slot
//     (e.g. [message.SubgroupObject.Properties]).
//   - payload is the MOQ Object Payload.
//
// The returned props slice is the inner KV-pair blob without an outer
// length prefix; the surrounding MOQ message frames it. The returned
// payload aliases [Object.Payload]; the caller must not mutate the
// originating slice after calling Encode if it is still being read.
func (o *Object) Encode() (props, payload []byte) {
	return o.Properties.Encode(), o.Payload
}

// Decode reconstructs an Object from the Properties bytes (the inner
// KV-pair blob, no length prefix) and the Object Payload bytes. Both
// slices may be nil.
//
// The returned Object's Payload aliases the input slice. Callers that
// need to retain Payload past the lifetime of the input buffer must
// copy.
func Decode(props, payload []byte) (Object, error) {
	p, err := ParseProperties(props)
	if err != nil {
		return Object{}, err
	}
	return Object{Properties: p, Payload: payload}, nil
}
