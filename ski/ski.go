// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"github.com/plan-systems/go-plan/plan"

)

/*****************************************************
** ski.Session
**/

// Session provides lambda-lifted crypto services from an opaque service provider.
//
// TODO: make into gRPC service
type Session interface {

    // Performs signing, encryption, and decryption.
    DoCryptOp(inArgs *CryptOpArgs) (*CryptOpOut, error)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(inReason string)
}

/*****************************************************
** ski.Provider
**/

// SessionParams is a convenience struct used for ski.Provider.StartSession()
type SessionParams struct {
	Invocation     plan.Block
    UserID         []byte
    Passhash       []byte
    
	//AccessScopes   [NumKeyDomains]AccessScopes
	BaseDir        string
}

// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type Provider interface {

	// InvocationStr returns a string that identifies this provider type
	InvocationStr() string

	// StartSession starts a new session SKI.session.  In general, you should only start one session
	StartSession(
		inPB SessionParams,
	) (Session, error)
}

/*****************************************************
** ski.InvokeProvider()
**/

/*
// ProviderRegistry maps provider names to implementations
var gProviderRegistry = map[string]Provider{}

// RegisterProvider registers the given provider so it can be invoked via ski.StartSession()
func RegisterProvider(inProvider Provider) error {
	invoke := inProvider.InvocationStr()
	if gProviderRegistry[invoke] != nil {
		return plan.Errorf(nil, plan.InvocationAlreadyExists, "the ski invocation %s already exists", invoke)
	}
	gProviderRegistry[invoke] = inProvider
	return nil
}

// StartSession returns a provider implementation given an invocation block
func StartSession(
	inPB SessionParams,
) (Session, error) {

	provider := gProviderRegistry[inPB.Invocation.Label]
	if provider == nil || provider.InvocationStr() != inPB.Invocation.Label {
		return nil, plan.Errorf(nil, plan.InvocationNotAvailable, "ski.StartSession() failed to find provider for invocation %s", inPB.Invocation.Label)
	}

	session, err := provider.StartSession(inPB)

	return session, err
}
*/

/*****************************************************
** ski.Provider AccessScopes
**/

// AccessScopes specifies a set of SKI ops allowed to occur in a SKI session.  A clients requests this set when starting a new session,
//     allowing the SKI provider to provision for the requested access (e.g. getting user permission of not already granted).
type AccessScopes []string

const (

	// ContentDecryptAccess only allows the client to decrypt data
	ContentDecryptAccess = "ContentDecryptAccess"
	
	// ContentAuthoringAccess allows the client to encrypt and sign data
	ContentAuthoringAccess = "ContentDecryptAccess"

    // AddKeysAccess allows keys to be imported and generated
    AddKeysAccess = "AddKeysAccess"
)
