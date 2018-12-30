package pservice


import (
    //"fmt"
    //"log"
    //"os"
    "sync"
    "crypto/rand"

    //"container/heap"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"

    //"github.com/tidwall/redcon"

    "github.com/plan-systems/go-plan/plan"

    //"crypto/md5"
    //"hash"


    //"github.com/stretchr/testify/assert"

    //"github.com/ethereum/go-ethereum/rlp"
    //"github.com/ethereum/go-ethereum/common/hexutil"

    "google.golang.org/grpc/metadata"
    "golang.org/x/net/context"
	"google.golang.org/grpc"
)



// ClientSession represents a client session over gRPC
type ClientSession struct {         

    // SessionToken is handed back to remote pnode clients during authentication and used to retrieve a ClientSession
    //     in O(1) given a matching token string at later times.
    SessionToken            string

    // PrevActivityTime says when this session was last accessed, used to know how long a session has been idle.
    PrevActivityTime        plan.Time

    // OnEndSession performs any closing/cleanup associated with the given session.  
    // Note: this is called via goroutine, so concurrency considerations should be made.
    OnEndSession            func(session *ClientSession, reason string)

    // Used to store an impl specific info
    Cookie                 interface{}
}



const (

    // ClientInactive reflects that a client session is ending because the client side has been inactive
    ClientInactive          = "client inactive"

    // HostShuttingDown means that a client session is ending because the host machine/server is shutting down.
    HostShuttingDown        = "host shutting down"

    // SessionTokenKey is the string name used to key the session token string (i.e. "session_token")
    SessionTokenKey         = "session_token"
)





func genRandomSessionToken(N int) string {

    const vocab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

    buf := make([]byte, N)

    rand.Read( buf )
    for i := 0; i < N; i++ {
        buf[i] = vocab[buf[i] & 0x3F]
    }
    
    return string( buf )
}










// SessionGroup takes a session token (a string) and hands back a ClientSession while ensuring concurrency safety.
type SessionGroup struct {
    sync.RWMutex

    table                   map[string]*ClientSession

}


// Init resets and internally sets up this SessionGroup for use
func (group *SessionGroup) Init() {

    group.table = map[string]*ClientSession{}
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
func ExtractSessionToken(ctx context.Context) (string, *plan.Perror) {

    md, ok := metadata.FromIncomingContext(ctx)
    if ! ok || md == nil {
        return "", plan.Errorf(nil, plan.SessionTokenMissing, "no context metadata (or %v) found", SessionTokenKey)
    }

    strList := md[SessionTokenKey]; 
    if len(strList) < 1 {
        return "", plan.Errorf(nil, plan.SessionTokenMissing, "%v key not found", SessionTokenKey)
    }

    return strList[0], nil
}


// TransferSessionToken extracts and transfers the session token from the given context and returns a replacement with it appended to the metadata
func TransferSessionToken(ctx context.Context, md metadata.MD) (context.Context, *plan.Perror) {

    if md == nil {
        return nil, plan.Errorf(nil, plan.SessionTokenMissing, "no header/trailer (or %v) found", SessionTokenKey)
    }

    strList := md[SessionTokenKey]; 
    if len(strList) < 1 {
        return nil, plan.Errorf(nil, plan.SessionTokenMissing, "%v key not found", SessionTokenKey)
    }

    ctx2 := metadata.AppendToOutgoingContext(ctx, SessionTokenKey, strList[0])

    return ctx2, nil
}



// FetchSession extracts the session token string from the context, performs a session lookup, and returns the ClientSession object.
func (group *SessionGroup) FetchSession(ctx context.Context) (*ClientSession, *plan.Perror) {

    token, err := ExtractSessionToken(ctx)
    if err != nil {
        return nil, err
    }
  
    session := group.LookupSession(token, true)
    if session == nil {
        return nil, plan.Errorf(nil, plan.SessionTokenNotValid, "%s not valid", SessionTokenKey)
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
            session.PrevActivityTime = plan.Now()
        }

        return session

    }
    
    return nil
}



// NewSession creates a new ClientSession and inserts the session token into the given context
func (group *SessionGroup) NewSession(
    ctx context.Context,
) *ClientSession {

    session := &ClientSession{
        SessionToken: genRandomSessionToken(32),
        OnEndSession: func(*ClientSession, string) { },
    }

    group.InsertSession(session)

    trailer := metadata.Pairs(SessionTokenKey, session.SessionToken)
    grpc.SetTrailer(ctx, trailer)

    return session
}


func (group *SessionGroup) InsertSession(inSession *ClientSession) {

    inSession.PrevActivityTime = plan.Now()

    group.Lock()
    group.table[inSession.SessionToken] = inSession
    group.Unlock()

}

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


func (group *SessionGroup) EndInactiveSessions( inExpiration plan.Time ) {

    var expired []*ClientSession

    // First, make a list to see if any have even expired
    group.RLock()
    for _, session := range group.table {
        if session.PrevActivityTime.UnixSecs > inExpiration.UnixSecs {
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
            go i.OnEndSession(i, ClientInactive)
        }
    }
}


func (group *SessionGroup) EndAllSessions(inReason string) {

    group.Lock()
    oldTable := group.table
    group.table = make(map[string]*ClientSession)
    group.Unlock()

    for _, session := range oldTable {
        go session.OnEndSession(session, inReason)
    }

}

