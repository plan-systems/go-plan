package ctx

import (
	"context"
    "encoding/base64"
	"sync"
	"time"

	crand "crypto/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)


// ClientSession represents a client session over gRPC
type ClientSession struct {

	// SessionToken is handed back to remote pnode clients during authentication and used to retrieve a ClientSession
	//     in O(1) given a matching token string at later times.
	SessionToken string

	// PrevActivityTime says when this session was last accessed, used to know how long a session has been idle.
	PrevActivityTime int64

	// OnEndSession performs any closing/cleanup associated with the given session.
	// Note: this is called via goroutine, so concurrency considerations should be made.
	OnEndSession func(session *ClientSession, reason string)

	// Used to store an impl specific info
	Cookie interface{}
}

const (

	// ClientInactive reflects that a client session is ending because the client side has been inactive
	ClientInactive = "client inactive"

	// HostShuttingDown means that a client session is ending because the host machine/server is shutting down.
	HostShuttingDown = "host shutting down"

	// SessionTokenKey is the string name used to key the session token string (i.e. "session_token")
	SessionTokenKey = "session_token"
)

var tokenEncoder = base64.URLEncoding

func genRandomSessionToken(N int) string {

	buf := make([]byte, N)
	crand.Read(buf)

    return tokenEncoder.EncodeToString(buf)
}

// SessionGroup takes a session token (a string) and hands back a ClientSession while ensuring concurrency safety.
type SessionGroup struct {
	sync.RWMutex

	table map[string]*ClientSession
}

// NewSessionGroup sets up a new SessionGroup for use
func NewSessionGroup() SessionGroup {
	return SessionGroup{
		table: map[string]*ClientSession{},
	}
}

/*
func (group *SessionGroup) Len() int {
    return len(group.byAccessTime)
}

func (group *SessionGroup) Less(i, j int) bool {
	return group.byAccessTime[i].PrevActivityTime.UnixSecs > group.byAccessTime[j].PrevActivityTime.UnixSecs
}

func (group *SessionGroup) Swap(i, j int) {
    byRank := group.byActivityRank

    byRank[i], byRank[j] = byRank[j], byRank[i]
	byRank[i].ActivityRank = i
	byRank[j].ActivityRank = j
}

func (group *SessionGroup) Push(x interface{}) {
	session := x.(*ClientSession)
	session.ActivityRank = len(*group)
	group.byActivityRank = append(group.byActivityRank, session)
}

func (group *SessionGroup) Pop() interface{} {
    byRank := group.byActivityRank

    last := len(byRank)-1
	session := byRank[last]
    group.byActivityRank = byRank[0:last]

	return session
}
*/

// ExtractSessionToken extracts the session_token from the given context
func ExtractSessionToken(ctx context.Context) (string, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || md == nil {
		return "", ErrSessTokenMissing
	}

	strList := md[SessionTokenKey]
	if len(strList) < 1 {
		return "", ErrSessTokenMissing
	}

	return strList[0], nil
}

// TransferSessionToken extracts and transfers the session token from the given context and returns a replacement with it appended to the metadata
func TransferSessionToken(ctx context.Context, md metadata.MD) (context.Context, error) {

	if md == nil {
		return nil, ErrSessTokenMissing
	}

	strList := md[SessionTokenKey]
	if len(strList) < 1 {
		return nil, ErrSessTokenMissing
	}

	ctx2 := metadata.AppendToOutgoingContext(ctx, SessionTokenKey, strList[0])

	return ctx2, nil
}

// ApplyTokenOutgoingContext applies the given binary string to the given context, encoding it into a base64 string.
func ApplyTokenOutgoingContext(ctx context.Context, inToken []byte) context.Context {

	str := tokenEncoder.EncodeToString(inToken)
	ctx2 := metadata.AppendToOutgoingContext(ctx, SessionTokenKey, str)

	return ctx2
}

// FetchSession extracts the session token string from the context, performs a session lookup, and returns the ClientSession object.
func (group *SessionGroup) FetchSession(ctx context.Context) (*ClientSession, error) {

	token, err := ExtractSessionToken(ctx)
	if err != nil {
		return nil, err
	}

	session := group.LookupSession(token, true)
	if session == nil {
		return nil, ErrSessTokenNotValid
	}

	return session, nil
}

// LookupSession returns the ClientSession having the given session token.
func (group *SessionGroup) LookupSession(inSessionToken string, inBumpActivity bool) *ClientSession {

	if len(inSessionToken) > 0 {

		group.RLock()
		session := group.table[inSessionToken]
		group.RUnlock()

		if inBumpActivity && session != nil {
			session.PrevActivityTime = time.Now().Unix()
		}

		return session

	}

	return nil
}

// NewSession creates a new ClientSession and inserts the session token into the given context
func (group *SessionGroup) NewSession(
	ctx context.Context,
	inTokenOverride []byte,
) *ClientSession {

	session := &ClientSession{
		OnEndSession: func(*ClientSession, string) {},
	}

	if len(inTokenOverride) > 0 {
		session.SessionToken = tokenEncoder.EncodeToString(inTokenOverride)
	} else {
		session.SessionToken = genRandomSessionToken(32)
	}

	group.InsertSession(session)

	tokenPair := metadata.Pairs(SessionTokenKey, session.SessionToken)
	grpc.SetTrailer(ctx, tokenPair)

	return session
}

// InsertSession inserts the given session into this SessionGroup -- THREADSAFE
func (group *SessionGroup) InsertSession(inSession *ClientSession) {

	inSession.PrevActivityTime = time.Now().Unix()

	group.Lock()
	group.table[inSession.SessionToken] = inSession
	group.Unlock()

}

// EndSession removes the given session from this SessionGroup -- THREADSAFE
func (group *SessionGroup) EndSession(inSessionToken string, inReason string) {

	group.Lock()
	session := group.table[inSessionToken]
	if session != nil {
		delete(group.table, inSessionToken)
	}
	group.Unlock()

	if session != nil {
		go session.OnEndSession(session, inReason)
	}
}

// EndInactiveSessions removes alls sessions inactive longer than the given time index -- THREADSAFE
func (group *SessionGroup) EndInactiveSessions(inExpirationTime int64) {

	var expired []*ClientSession

	// First, make a list to see if any have even expired
	group.RLock()
	for _, session := range group.table {
		if session.PrevActivityTime > inExpirationTime {
			expired = append(expired, session)
		}
	}
	group.RUnlock()

	// Only lock the session group for mutex write access if we need to remove items.
	if len(expired) > 0 {
		group.Lock()
		for _, session := range expired {
			delete(group.table, session.SessionToken)
		}
		group.Unlock()

		for _, i := range expired {
			sess := i
			go sess.OnEndSession(sess, ClientInactive)
		}
	}
}

// EndAllSessions ends all sessions
func (group *SessionGroup) EndAllSessions(inReason string) {

	group.Lock()
	oldTable := group.table
	group.table = make(map[string]*ClientSession)
	group.Unlock()

	for _, i := range oldTable {
		sess := i
		go sess.OnEndSession(sess, inReason)
	}

}

