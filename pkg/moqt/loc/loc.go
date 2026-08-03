// Package loc implements the Low Overhead Media Container described by
// draft-ietf-moq-loc-04. LOC maps a single encoded audio or video chunk
// (the "internal data" of an EncodedAudioChunk/EncodedVideoChunk in the
// WebCodecs Codec Registry) onto a single MOQ Object: the LOC Public
// Properties live in the MOQ Object Properties block (§2.2), and the
// codec elementary stream is the MOQ Object Payload.
//
// This package owns the *data format* only — it does not open streams or
// hold a session. Callers populate a [message.SubgroupObject] with the
// bytes returned by [Object.Encode], and use [Decode] on a received
// object to recover the [Object] value.
//
// # Property identifiers
//
// draft-ietf-moq-loc-04 finalises all six LOC properties in the IANA "MOQ
// Object Properties" registry (§6.1); the constants below use those
// assigned values. Each ID's parity selects its wire encoding per the
// MoQ Transport KV-pair convention (§1.4.3): an even ID carries a single
// varint value (no length prefix), an odd ID carries a length-prefixed
// byte string.
//
//	ID    Property              Scope          Value
//	0x08  TIMESCALE             Track, Object  vi64
//	0x09  VIDEO_FRAME_MARKING   Object         length-prefixed bytes
//	0x0C  AUDIO_LEVEL           Object         vi64 (low 8 bits used)
//	0x0D  VIDEO_CONFIG          Track, Object  length-prefixed bytes
//	0x0F  AUDIO_CONFIG          Track, Object  length-prefixed bytes
//	0x10  TIMESTAMP             Object         vi64
//
// These Object-scoped IDs are all distinct from the Track Properties
// registered by MoQ Transport (see [message] — 0x02, 0x04, 0x06, 0x0B,
// 0x0E, 0x22, 0x30, 0x3C, 0x3E), so LOC's Object Properties never collide
// with a Track property of the same ID even under a shared validator.
//
// # End-to-end encryption
//
// Section 3 of the LOC draft and the SecureObjects mechanism that
// carries LOC Private Properties are out of scope for this initial
// implementation. All metadata produced here travels as MOQ Object
// Properties (visible to relays); add SecureObjects support before
// using this package in any deployment requiring end-to-end
// confidentiality of timestamps, audio levels, or frame markings.
package loc

import "github.com/floatdrop/moq-go/pkg/moqt/message"

// LOC property identifiers, assigned by draft-ietf-moq-loc-04 §6.1.
const (
	// PropTimescale is the LOC Timescale property (§2.3.1.2). Value is a
	// varint giving the number of Timestamp units per second. ID is even.
	PropTimescale message.PropertyType = 0x08

	// PropVideoFrameMarking is the LOC Video Frame Marking property
	// (§2.3.2.2). Value is a length-prefixed byte string (1-4 bytes)
	// carrying the RFC 9626 frame-marking flags and layer identifiers. ID
	// is odd, so the value is length-prefixed bytes.
	PropVideoFrameMarking message.PropertyType = 0x09

	// PropAudioLevel is the LOC Audio Level property (§2.3.3.2). Value
	// is a varint whose least-significant 8 bits encode the RFC 6464
	// audio level and voice-activity indicator. ID is even.
	PropAudioLevel message.PropertyType = 0x0C

	// PropVideoConfig is the LOC Video Config property (§2.3.2.1). Value
	// is the codec "extradata" bytes (matches WebCodecs
	// VideoDecoderConfig.description). ID is odd, so the value is
	// length-prefixed bytes.
	PropVideoConfig message.PropertyType = 0x0D

	// PropAudioConfig is the LOC Audio Config property (§2.3.3.1). Value
	// is the codec configuration bytes (matches WebCodecs
	// AudioDecoderConfig.description). ID is odd, so the value is
	// length-prefixed bytes.
	PropAudioConfig message.PropertyType = 0x0F

	// PropTimestamp is the LOC Timestamp property (§2.3.1.1). Value is a
	// varint whose unit is given by [PropTimescale]. ID is even, so the
	// value is encoded as a single varint.
	PropTimestamp message.PropertyType = 0x10
)
