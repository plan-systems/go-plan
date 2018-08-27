
package pnode


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

    "github.com/plan-tools/go-plan/plan"
    "github.com/plan-tools/go-plan/pservice"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"


    //"github.com/stretchr/testify/assert"

    //"github.com/ethereum/go-ethereum/rlp"
    //"github.com/ethereum/go-ethereum/common/hexutil"

    "google.golang.org/grpc/metadata"
    "golang.org/x/net/context"
	"google.golang.org/grpc"
    "google.golang.org/grpc/codes"
)




// ClientSession represents a client session over GRPC
type ClientSession struct {         

    // AuthToken is handed back to remote pnode clients during authentication and used to retrieve a ClientSession
    //     in O(1) given a matching token string at later times.
    AuthToken               string

    // PrevActivityTime says when this session was last accessed, used to know how long a session has been idle.
    PrevActivityTime        plan.Time

    // Client session request that initiated this session
    SessionRequest          *pservice.SessionRequest


    //SKI                     SecureKeyInterface      // SKI allows pnode to encrypt/decrypt for the given user
}



const (

    // ClientInactive reflects that a client session is ending because the client side has been inactive
    ClientInactive          = "client inactive"

    // HostShuttingDown means that a client session is ending because the host machine/server is shutting down.
    HostShuttingDown        = "host shutting down"
)





func GenRandomSessionToken(N int) string {

    const vocab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

    buf := make([]byte, N)

    rand.Read( buf )
    for i := 0; i < N; i++ {
        buf[i] = vocab[ buf[i] & 0x3F ]
    }
    
    return string( buf )
}





// EndSession performs any closing/cleanup associated with the given session.  inWasInactive is true if this
//    func is being invoked because the session has been inactive.  Otherwise, the session is ending
//    for another reason (e.g. local shutdown).  
// Note: this is called via goroutine, so concurrency considerations should be made.
func (session *ClientSession) EndSession( inReason string ) {

}


// SessionGroup takes a session token (a string) and hands back a ClientSession while ensuring concurrency safety.
type SessionGroup struct {
    sync.RWMutex

    table                   map[string]*ClientSession

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






func (group *SessionGroup) FetchSession(ctx context.Context) (*ClientSession, error) {

    md, ok := metadata.FromIncomingContext( ctx )
    if ! ok {
        return nil, grpc.Errorf( codes.NotFound, "metadata not found" )
    }

    token := md["session_token"][0]
    if len( token ) == 0 {
        return nil, grpc.Errorf( codes.NotFound, "session_token not defined" )
    }

    session := group.LookupSession(token, true)
    if session == nil {
        return nil, grpc.Errorf( codes.NotFound, "invalid session token" )
    }



    return session, nil
}


func (group *SessionGroup) LookupSession(inAuthToken string, inBumpActivity bool) *ClientSession {
   
    if len(inAuthToken) > 0 {

        group.RLock()
        session := group.table[inAuthToken]
        group.RUnlock()

        if inBumpActivity && session != nil {
            session.PrevActivityTime = plan.Now()
        }

        return session

    } else {
        return nil
    }
}



func (group *SessionGroup) InsertSession(inSession *ClientSession) {

    inSession.PrevActivityTime = plan.Now()

    group.Lock()
    group.table[inSession.AuthToken] = inSession
    group.Unlock()

}

func (group *SessionGroup) EndSession(inAuthToken string, inReason string) {

    group.Lock()
    session := group.table[inAuthToken]
    if session != nil {
        delete(group.table, inAuthToken)
    }
    group.Unlock()

    if session != nil {
        go session.EndSession(inReason)
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
            delete(group.table, session.AuthToken)
        }
        group.Unlock()

        for _, i := range expired {
            go i.EndSession(ClientInactive)
        }
    }
}


func (group *SessionGroup) EndAllSessions(inReason string) {

    group.Lock()
    oldTable := group.table
    group.table = make(map[string]*ClientSession)
    group.Unlock()

    for _, session := range oldTable {
        go session.EndSession(inReason)
    }

}

