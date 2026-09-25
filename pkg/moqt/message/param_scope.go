package message

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
)

// ErrUnknownParameter is wrapped by the parse error for a Message Parameter
// type this version does not define. §10.2: an endpoint that receives one
// "MUST close the session with PROTOCOL_VIOLATION".
var ErrUnknownParameter = errors.New("moqt/message: unknown parameter type")

// ParamScope is the message form a parameter block arrived in, as the
// per-parameter scope rules of §10.2.1 tell them apart. A REQUEST_OK takes its
// form from the request it answers (§10.5: "PUBLISH_OK, REQUEST_UPDATE_OK,
// TRACK_STATUS_OK, ..."), and a REQUEST_UPDATE from the request it updates.
// Values are bit flags so a parameter's allowed forms are one mask.
type ParamScope uint32

const (
	ScopeSubscribe ParamScope = 1 << iota
	ScopeSubscribeOK
	ScopePublish
	ScopePublishOK
	ScopeFetch
	ScopeFetchOK
	ScopeTrackStatus
	ScopeTrackStatusOK
	ScopeSubscribeNamespace
	ScopeSubscribeNamespaceOK
	ScopeSubscribeTracks
	ScopeSubscribeTracksOK
	ScopePublishNamespace
	ScopePublishNamespaceOK
	ScopePublishStateNotify
	ScopeRequestUpdateOK
	// ScopeUpdateFromSubscriber is a REQUEST_UPDATE for a subscription —
	// established by SUBSCRIBE or PUBLISH — sent by its subscriber;
	// ScopeUpdateFromPublisher one sent by its publisher, on a PUBLISH it
	// sent. §5.1.4 allows the Range Filters only "from the subscriber".
	ScopeUpdateFromSubscriber
	ScopeUpdateFromPublisher
	ScopeUpdateFetch
	ScopeUpdateTrackStatus
	ScopeUpdateSubscribeNamespace
	ScopeUpdateSubscribeTracks
	ScopeUpdatePublishNamespace
)

const (
	scopeUpdateSubscription = ScopeUpdateFromSubscriber | ScopeUpdateFromPublisher
	scopeAnyUpdate          = scopeUpdateSubscription | ScopeUpdateFetch | ScopeUpdateTrackStatus |
		ScopeUpdateSubscribeNamespace | ScopeUpdateSubscribeTracks | ScopeUpdatePublishNamespace
	// §5.1.4: "All other filter parameters MAY appear multiple times in a
	// FETCH, SUBSCRIBE, SUBSCRIBE_TRACKS, or REQUEST_UPDATE (on a
	// subscription, from the subscriber only) message", and the Track
	// Property filter in "a SUBSCRIBE_TRACKS message or REQUEST_UPDATE for
	// it". Its opening sentence names SUBSCRIBE, FETCH and SUBSCRIBE_TRACKS
	// for all five, so all five share one scope here; the text is ambiguous,
	// and the reading that closes fewer sessions was chosen.
	scopeRangeFilter = ScopeSubscribe | ScopeFetch | ScopeSubscribeTracks |
		ScopeUpdateFromSubscriber | ScopeUpdateSubscribeTracks
)

// paramScopes is each parameter's "MAY appear in" list, from its definition.
// [Parameters.CheckScope] adds SUBSCRIBE_TRACKS to every SUBSCRIBE parameter
// (§10.20.1).
var paramScopes = map[ParamID]ParamScope{
	// §10.2.2
	ParamAuthorizationToken: ScopePublish | ScopeSubscribe | scopeAnyUpdate | ScopeSubscribeNamespace |
		ScopeSubscribeTracks | ScopePublishNamespace | ScopeTrackStatus | ScopeFetch,
	// §10.2.3, §10.2.4
	ParamSubgroupDeliveryTimeout: ScopeSubscribe | ScopePublish | scopeAnyUpdate,
	ParamObjectDeliveryTimeout:   ScopeSubscribe | ScopePublish | scopeAnyUpdate,
	// §10.2.5 (also inside FILL_PARAMETERS, a separate scope)
	ParamFillTimeout: ScopeFetch,
	// §10.2.6
	ParamRendezvousTimeout: ScopeSubscribe,
	// §10.2.7
	ParamSubscriberPriority: ScopeSubscribe | ScopePublish | ScopeFetch | scopeUpdateSubscription | ScopeUpdateFetch,
	// §10.2.8
	ParamGroupOrder: ScopeSubscribe | ScopePublish | ScopeSubscribeTracks | ScopeFetch,
	// §10.2.9
	ParamLocationFilter: ScopeFetch | ScopeSubscribe | ScopePublish | scopeUpdateSubscription |
		ScopePublishStateNotify,
	// §10.2.10–§10.2.14, §5.1.4
	ParamSubgroupFilter:       scopeRangeFilter,
	ParamObjectIDFilter:       scopeRangeFilter,
	ParamPriorityFilter:       scopeRangeFilter,
	ParamObjectPropertyFilter: scopeRangeFilter,
	ParamTrackPropertyFilter:  scopeRangeFilter,
	// §10.2.15
	ParamFillParameters: ScopeSubscribe | scopeUpdateSubscription,
	// §10.2.16
	ParamExpires: ScopeSubscribeOK | ScopePublish | ScopePublishOK | ScopeSubscribeNamespaceOK |
		ScopeSubscribeTracksOK | ScopePublishNamespaceOK | ScopeRequestUpdateOK,
	// §10.2.17
	ParamLargestObject: ScopeSubscribeOK | ScopePublish | ScopeRequestUpdateOK | ScopeTrackStatusOK |
		ScopePublishStateNotify,
	// §10.2.18
	ParamForward: ScopeSubscribe | scopeUpdateSubscription | ScopeUpdateSubscribeTracks | ScopePublish |
		ScopeSubscribeTracks | ScopePublishStateNotify,
	// §10.2.19
	ParamNewGroupRequest: ScopeSubscribe | scopeUpdateSubscription,
	// §10.2.20
	ParamTrackNamespacePrefix: ScopeUpdateSubscribeNamespace | ScopeUpdateSubscribeTracks,
	// §10.2.21
	ParamIncludeProperties: ScopeSubscribe | ScopeTrackStatus | ScopeFetch | ScopeSubscribeTracks,
}

var scopeNames = [...]string{
	"SUBSCRIBE", "SUBSCRIBE_OK", "PUBLISH", "PUBLISH_OK", "FETCH", "FETCH_OK",
	"TRACK_STATUS", "TRACK_STATUS_OK", "SUBSCRIBE_NAMESPACE", "SUBSCRIBE_NAMESPACE_OK",
	"SUBSCRIBE_TRACKS", "SUBSCRIBE_TRACKS_OK", "PUBLISH_NAMESPACE", "PUBLISH_NAMESPACE_OK",
	"PUBLISH_STATE_NOTIFY", "REQUEST_UPDATE_OK",
	"REQUEST_UPDATE from a subscriber", "REQUEST_UPDATE from a publisher", "REQUEST_UPDATE for FETCH",
	"REQUEST_UPDATE for TRACK_STATUS", "REQUEST_UPDATE for SUBSCRIBE_NAMESPACE",
	"REQUEST_UPDATE for SUBSCRIBE_TRACKS", "REQUEST_UPDATE for PUBLISH_NAMESPACE",
}

func (s ParamScope) String() string {
	var names []string
	for s != 0 {
		i := bits.TrailingZeros32(uint32(s))
		if i < len(scopeNames) {
			names = append(names, scopeNames[i])
		}
		s &^= 1 << i
	}
	if len(names) == 0 {
		return "no message"
	}
	return strings.Join(names, "|")
}

// ScopeOfRequest is the scope of a request opener's parameters (§3.3), or 0
// for a type that does not open a request.
func ScopeOfRequest(t Type) ParamScope {
	//exhaustive:ignore // every other type maps to 0
	switch t {
	case TypeSubscribe:
		return ScopeSubscribe
	case TypePublish:
		return ScopePublish
	case TypeFetch:
		return ScopeFetch
	case TypeTrackStatus:
		return ScopeTrackStatus
	case TypeSubscribeNamespace:
		return ScopeSubscribeNamespace
	case TypeSubscribeTracks:
		return ScopeSubscribeTracks
	case TypePublishNamespace:
		return ScopePublishNamespace
	default:
		return 0
	}
}

// ScopeOfResponse is the scope of the success response to a request of type
// request: SUBSCRIBE_OK, FETCH_OK, or the REQUEST_OK form §10.5 names for it.
func ScopeOfResponse(request Type) ParamScope {
	//exhaustive:ignore // every other type maps to 0
	switch request {
	case TypeSubscribe:
		return ScopeSubscribeOK
	case TypePublish:
		return ScopePublishOK
	case TypeFetch:
		return ScopeFetchOK
	case TypeTrackStatus:
		return ScopeTrackStatusOK
	case TypeSubscribeNamespace:
		return ScopeSubscribeNamespaceOK
	case TypeSubscribeTracks:
		return ScopeSubscribeTracksOK
	case TypePublishNamespace:
		return ScopePublishNamespaceOK
	case TypeRequestUpdate:
		return ScopeRequestUpdateOK
	default:
		return 0
	}
}

// ScopeOfUpdate is the scope of a REQUEST_UPDATE sent by the sender of a
// request of type request. The subscriber of a PUBLISH may send one too; its
// scope is [ScopeUpdateFromSubscriber].
func ScopeOfUpdate(request Type) ParamScope {
	//exhaustive:ignore // every other type maps to 0
	switch request {
	case TypeSubscribe:
		return ScopeUpdateFromSubscriber
	case TypePublish:
		return ScopeUpdateFromPublisher
	case TypeFetch:
		return ScopeUpdateFetch
	case TypeTrackStatus:
		return ScopeUpdateTrackStatus
	case TypeSubscribeNamespace:
		return ScopeUpdateSubscribeNamespace
	case TypeSubscribeTracks:
		return ScopeUpdateSubscribeTracks
	case TypePublishNamespace:
		return ScopeUpdatePublishNamespace
	default:
		return 0
	}
}

// ParamScopeError reports a Message Parameter in a message form its
// definition does not list (§10.2.1), or repeated where it may not be
// (§10.2). Either is a PROTOCOL_VIOLATION.
type ParamScopeError struct {
	Type      ParamID
	Scope     ParamScope
	Duplicate bool
}

func (e *ParamScopeError) Error() string {
	if e.Duplicate {
		return fmt.Sprintf("moqt/message: duplicate %s in %s (PROTOCOL_VIOLATION §10.2)", e.Type, e.Scope)
	}
	return fmt.Sprintf("moqt/message: %s not allowed in %s (PROTOCOL_VIOLATION §10.2.1)", e.Type, e.Scope)
}

// CheckScope reports the first parameter of ps that may not appear in a
// message of the given scope, or that repeats where its definition does not
// allow it (see [Parameters.firstDuplicate]). A FILL_PARAMETERS value is a
// scope of its own (§10.2.15) and is checked against its table too. Every
// error is a session-level PROTOCOL_VIOLATION.
func (ps Parameters) CheckScope(scope ParamScope) error {
	for _, p := range ps {
		allowed := paramScopes[p.Type]
		// §10.20.1: "Any Parameter that can be specified on a Subscription
		// (ie: in SUBSCRIBE) is valid in SUBSCRIBE_TRACKS, unless otherwise
		// specified." They become the subscriptions' initial parameters.
		if allowed&ScopeSubscribe != 0 {
			allowed |= ScopeSubscribeTracks
		}
		if allowed&scope == 0 {
			return &ParamScopeError{Type: p.Type, Scope: scope}
		}
	}
	if t, dup := ps.firstDuplicate(); dup {
		return &ParamScopeError{Type: t, Scope: scope, Duplicate: true}
	}
	if _, _, err := FillParametersFromParam(ps); err != nil {
		return err
	}
	return nil
}

// firstDuplicate reports the first parameter type that appears more than once
// in ps where its definition does not allow it (§10.2: "Senders MUST NOT
// repeat the same Parameter Type in a message unless the parameter definition
// explicitly allows multiple instances"). The Range Filters may repeat
// (§5.1.4), and so may AUTHORIZATION_TOKEN (§10.2.2: it "MAY be repeated
// within a message").
func (ps Parameters) firstDuplicate() (ParamID, bool) {
	for i, p := range ps {
		if IsRangeFilterParam(p.Type) || p.Type == ParamAuthorizationToken {
			continue
		}
		if slices.ContainsFunc(ps[:i], func(q Parameter) bool { return q.Type == p.Type }) {
			return p.Type, true
		}
	}
	return 0, false
}

// ParamsOf returns m's Message Parameters, and false for a message type that
// has none.
func ParamsOf(m Message) (Parameters, bool) {
	switch m := m.(type) {
	case *Subscribe:
		return m.Parameters, true
	case *SubscribeOK:
		return m.Parameters, true
	case *Publish:
		return m.Parameters, true
	case *Fetch:
		return m.Parameters, true
	case *FetchOK:
		return m.Parameters, true
	case *TrackStatus:
		return m.Parameters, true
	case *RequestOK:
		return m.Parameters, true
	case *RequestUpdate:
		return m.Parameters, true
	case *SubscribeNamespace:
		return m.Parameters, true
	case *SubscribeTracks:
		return m.Parameters, true
	case *PublishNamespace:
		return m.Parameters, true
	case *PublishStateNotify:
		return m.Parameters, true
	default:
		return nil, false
	}
}
