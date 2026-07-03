package wire

// Scanner is a sticky-error decoding cursor over a [Reader]. Each accessor
// reads one field into the supplied pointer and records the first error it
// hits; once an error is recorded every later accessor is a no-op until Err is
// consulted. It removes the repetitive per-field error handling that otherwise
// dominates message Parse methods:
//
//	func (m *Subscribe) Parse(r *wire.Reader) error {
//		s := r.Scanner()
//		s.Varint(&m.RequestID)
//		s.TrackNamespace(&m.Namespace)
//		s.VarintBytes(&m.Name)
//		if err := s.Err(); err != nil {
//			return err
//		}
//		return m.Parameters.parse(r)
//	}
//
// A Scanner delegates to its Reader and advances the same read offset, so the
// underlying Reader stays usable directly after the Scanner (e.g. for
// Parameters.parse or a RemainingBytes tail) once Err reports no error.
//
// Scanner only wraps the in-memory [Reader]; the streaming [StreamReader] /
// [Decoder] path is unaffected.
type Scanner struct {
	r   *Reader
	err error
}

// Scanner returns a sticky-error cursor over r.
func (r *Reader) Scanner() *Scanner { return &Scanner{r: r} }

// Err returns the first error any accessor recorded, or nil.
func (s *Scanner) Err() error { return s.err }

// scan runs read unless an error is already pending, storing the value into dst
// or recording the error. It is the single implementation behind every typed
// accessor below.
func scan[T any](s *Scanner, dst *T, read func() (T, error)) {
	if s.err != nil {
		return
	}
	v, err := read()
	if err != nil {
		s.err = err
		return
	}
	*dst = v
}

// Varint reads a leading-ones varint (§1.4.1) into dst.
func (s *Scanner) Varint(dst *uint64) { scan(s, dst, s.r.Varint) }

// UInt8 reads a single byte into dst.
func (s *Scanner) UInt8(dst *uint8) { scan(s, dst, s.r.UInt8) }

// VarintBytes reads a varint-length-prefixed byte slice into dst.
func (s *Scanner) VarintBytes(dst *[]byte) { scan(s, dst, s.r.VarintBytes) }

// ReasonPhrase reads a §1.4.4 reason phrase into dst.
func (s *Scanner) ReasonPhrase(dst *string) { scan(s, dst, s.r.ReasonPhrase) }

// TrackNamespace reads a §2.4.1 track namespace into dst.
func (s *Scanner) TrackNamespace(dst *TrackNamespace) { scan(s, dst, s.r.TrackNamespace) }

// KVPairsRemaining reads delta-encoded KV pairs to end-of-buffer into dst.
func (s *Scanner) KVPairsRemaining(dst *[]KVPair) { scan(s, dst, s.r.KVPairsRemaining) }
