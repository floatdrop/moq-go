// Package loc implements the Low Overhead Media Container described by
// draft-ietf-moq-loc-02. LOC maps a single encoded audio or video chunk
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
// # Property identifier choices
//
// LOC §6.1 finalises only TIMESTAMP (0x06) and TIMESCALE (0x08) in the
// IANA MOQ Properties registry. The remaining property IDs in §2.3 carry
// an "IANA, please assign" annotation and are tentative. This package
// uses the draft-suggested values with one deviation:
//
//   - AUDIO_LEVEL: the draft suggests ID 6, which collides with
//     TIMESTAMP. We use 0x0A as a placeholder so a single Properties
//     bag can carry both at once. Update [PropAudioLevel] once IANA
//     assigns a final value.
//
// VIDEO_FRAME_MARKING uses 0x04, which is also the Track-scoped
// PropertyMaxCacheDuration in [github.com/floatdrop/moq-go/pkg/moqt/message].
// On the wire that is fine — Track Properties and Object Properties
// live in different syntactic positions in MOQ messages — but callers
// MUST NOT pass LOC-produced Object Properties through
// [message.ObjectProperties.ValidateObjectScope]: that helper checks
// IDs against the MoQ Transport property registry and will incorrectly
// reject LOC's Object-scoped redefinitions.
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

// LOC property identifiers. See package doc for the conflict notes and
// the policy used for tentative IDs.
const (
	// PropTimestamp is the LOC Timestamp property (§2.3.1.1). Value is a
	// varint whose unit is given by [PropTimescale]; if absent, the
	// timestamp is microseconds since the Unix epoch. ID is even, so the
	// value is encoded as a single varint.
	PropTimestamp message.PropertyType = 0x06

	// PropTimescale is the LOC Timescale property (§2.3.1.2). Value is a
	// varint giving the number of Timestamp units per second. ID is even.
	PropTimescale message.PropertyType = 0x08

	// PropVideoFrameMarking is the LOC Video Frame Marking property
	// (§2.3.2.2). Value is a varint carrying RFC 9626 flags in its least
	// significant bits. ID is even.
	PropVideoFrameMarking message.PropertyType = 0x04

	// PropVideoConfig is the LOC Video Config property (§2.3.2.1). Value
	// is the codec "extradata" bytes (matches WebCodecs
	// VideoDecoderConfig.description). ID is odd, so the value is
	// length-prefixed bytes.
	PropVideoConfig message.PropertyType = 0x0D

	// PropAudioLevel is the LOC Audio Level property (§2.3.3.1). Value
	// is a varint whose least-significant 8 bits encode the RFC 6464
	// audio level and voice-activity indicator. ID is even.
	//
	// The draft suggests ID 6, which collides with PropTimestamp. We
	// use 0x0A as a placeholder pending IANA assignment.
	PropAudioLevel message.PropertyType = 0x0A
)
