package datastore

import (

    //"fmt"
    "os"
    "path"
    "encoding/base64"
    "io/ioutil"
    //"strings"
    "sync"
    "encoding/json"
    //"time"
    //"hash"
    //"crypto/rand"



    log "github.com/sirupsen/logrus"

    ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"

    "github.com/ipfs/go-ds-badger"
    "github.com/ipfs/go-ds-leveldb"
    boltds "github.com/ipfs/go-ds-bolt"
    //"github.com/dgraph-io/badger"

    //"github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"


  
)


// StorageConfig contains core info about a db/store
type StorageConfig struct {
    HomePath                string                  `json:"home_path"`
    ImplName                string                  `json:"impl_name"`
}


// Store wraps a PLAN community UUID and a datastore
type Store struct {
    CommunityID                 plan.CommunityID
    AgentDesc                   string    
    Config                      *StorageConfig
    Epoch                       pdi.StorageEpoch

    AbsPath                     string
    keyEncoding                 base64.Encoding

    commitScrap                 []byte

    //db                          *badger.DB
    ds                          ds.Datastore

    QueryInbox                  chan *QueryJob
    CommitInbox                 chan *CommitJob

    // close when shutting down (polite shutdown)
    shuttingDown                chan *sync.WaitGroup

    TxnDecoder                   pdi.TxnDecoder

    log                         *log.Logger

    DefaultFileMode             os.FileMode

}


// DefaultImplName should be used when a datastore impl is not specified
const DefaultImplName = "badger"

// NewStore makes a new Datastore
func NewStore(
    inConfig *StorageConfig,
    inBasePath string,
) *Store {

    St := &Store{
        Config: inConfig,
        DefaultFileMode: plan.DefaultFileMode,
        QueryInbox: make(chan *QueryJob),
        CommitInbox: make(chan *CommitJob, 16),
        shuttingDown: make(chan *sync.WaitGroup),
        log: log.StandardLogger(),
    }

    St.TxnDecoder = NewTxnDecoder()

    if path.IsAbs(St.Config.HomePath) {
        St.AbsPath = St.Config.HomePath
    } else {
        St.AbsPath = path.Clean(path.Join(inBasePath, St.Config.HomePath))
    }

    return St
}

// Startup should be called once Datastore is preprared and ready to invoke the underlying implementation.
func (St *Store) Startup(inFirstTime bool) error {

    logE := log.WithFields(log.Fields{ 
        "impl": St.Config.ImplName,
        "path": St.AbsPath,
    })
    logE.Info( "Startup()" )

    // Load community info
    {
        pathname := path.Join(St.AbsPath, pdi.GenesisEpochFilename)
        buf, err := ioutil.ReadFile(pathname)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "missing %s", pathname)
        }

        err = json.Unmarshal(buf, &St.Epoch)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "error unmarshalling %s", pathname)
        }
    }

    var err error

    switch St.Config.ImplName {
        case "badger":
            badgerDS, dsErr := badger.NewDatastore(St.AbsPath, nil)
            if dsErr != nil {
                err = plan.Error(dsErr, plan.FailedToLoadDatabase, "badger.NewDatastore() failed")
            } else {
                St.ds = badgerDS
            }

        case "leveldb":
            levelDS, dsErr := leveldb.NewDatastore(St.AbsPath, nil)
            if dsErr != nil {
                err = plan.Error(dsErr, plan.FailedToLoadDatabase, "leveldb.NewDatastore() failed")
            } else {
                St.ds = levelDS
            }

        case "bolt":
            boldDS, dsErr := boltds.NewBoltDatastore(St.AbsPath, "ds", false)
            if dsErr != nil {
                err = plan.Error(dsErr, plan.FailedToLoadDatabase, "boltds.NewBoltDatastore() failed")
            } else {
                St.ds = boldDS
            }

        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", St.Config.ImplName)
    }

    if err != nil {
        return err
    }

    St.startProcessingJobs()

    return err
}


// Shutdown closes this datastore, if open, blocking until completion.
func (St *Store) Shutdown(inGroup *sync.WaitGroup) {

    if St.ds != nil {

        // Signal a shutdown
        var waiter sync.WaitGroup
        waiter.Add(2)
        St.shuttingDown <- &waiter
        St.shuttingDown <- &waiter
        waiter.Wait()

        St.ds.Close()
    }

    inGroup.Done()
}



// QueryJob represents a pending Query() call to a StorageProvider
type QueryJob struct {
    TxnQuery  *pdi.TxnQuery
    Outlet     pdi.StorageProvider_QueryServer
    OnComplete chan error
}

// CommitJob represents a pending CommitTxn() call to a StorageProvider
type CommitJob struct {
    ReadiedTxn *pdi.ReadiedTxn
    Outlet      pdi.StorageProvider_CommitTxnServer
    OnComplete  chan error
}

// UTIDKeyForTxn creates a ds.Key based on the transaction's timestamp and hash.UTIDKeyForTxn
// See comments for ConvertToUTID
func UTIDKeyForTxn(txnInfo *pdi.TxnInfo) ds.Key {
    key := pdi.FormUTID("/", txnInfo.TimeSealed, txnInfo.TxnHashname)
    return ds.RawKey(key)
}

// AccountKeyForPubKey makes a datastore Key for a given PubKey
func AccountKeyForPubKey(pubKey []byte) ds.Key {

    const (
        maxPubKeyLen = 24       // Arbitrary. As a ref point, BTC and ETH public address size is 20 bytes
        encodedKeyLen = (maxPubKeyLen * 8 + 5) / 6
        prefixLen = 3
    )

	overhang := len(pubKey) - maxPubKeyLen
    if overhang > 0 {
        pubKey = pubKey[overhang:]
    }

    var out [encodedKeyLen+prefixLen]byte
    out[0] = '/'
    out[1] = '@'
    out[2] = '/'
	pdi.Base64.Encode(out[prefixLen:], pubKey)

    finalLen := prefixLen + pdi.Base64.EncodedLen(len(pubKey))
    return ds.RawKey(string(out[:finalLen]))
}


func (St *Store) startProcessingJobs() {

    // CommitInbox processor
    go func() {

        var doneSignal *sync.WaitGroup

        // TODO: add throttling?

        // Keep looping until the commit inbox is clear
        for doneSignal == nil {

            select {
                case commitJob := <- St.CommitInbox:

                    // Unlike QueryJobs, we only process once commit job at a time
                    St.doCommitJob(commitJob)

                case doneSignal = <-St.shuttingDown:
            }
        }

        doneSignal.Done()
    }()

    // QueryInbox processor
    go func() {

        var doneSignal *sync.WaitGroup

        for doneSignal == nil {

            select {
                case queryJob := <- St.QueryInbox:
                    
                    // Each QueryJob runs in a goroutine
                    // TODO: limit number of in-flight query jobs?
                    go St.doQueryJob(queryJob)

                case doneSignal = <-St.shuttingDown:
            }
        }

        doneSignal.Done()
    }()

}


func (St *Store) updateAccount(
    dsAccess DsAccess,
    inAcctAddr []byte,
    inOp func(acct *pdi.StorageAccount) error,
) (ds.Key, error) {

    dsKey := AccountKeyForPubKey(inAcctAddr)
    var (
        creatingNew bool
        acct pdi.StorageAccount
        err error
    )

    // Load the acct from the db
    {
        var val []byte
        val, err = dsAccess.Get(dsKey)
        if err == ds.ErrNotFound {
            err = nil
            creatingNew = true
        } else if err == nil {
            err = acct.Unmarshal(val)
        }

        if err != nil {
            err = plan.Errorf(err, plan.StorageNotReady, "failed to get/unmarshal account %v", dsKey)
        }
    }

    if err == nil {
        err = inOp(&acct)
    }

    // Write the new acct out
    if err == nil {
        var scrap [200]byte
        acctSz, err := acct.MarshalTo(scrap[:])
        if err != nil {
            err = plan.Errorf(err, plan.StorageNotReady, "failed to marshal account %v", dsKey)
        }

        if err == nil {
            if creatingNew {
                St.log.Infof("Creating account %v to receive deposit", dsKey)
            }
            err = dsAccess.Put(dsKey, scrap[:acctSz]) 
            if err != nil {
                err = plan.Errorf(err, plan.StorageNotReady, "failed to update acct %v", dsKey)
            }
        }
    }

    // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
    if err != nil {
        St.log.WithField( 
            "dsKey", dsKey,
        ).Warning(err)
    }

    return dsKey, err
}

// DepositTransfers deposits the given amount to 
func (St *Store) DepositTransfers(inTransfers []*pdi.Transfer) error {

    if len(inTransfers) == 0 {
        return nil
    }

    txn := St.NewTxnHelper()

    for txn.NextAttempt() {
        var err error

        for _, xfer := range inTransfers {
            _, err = St.updateAccount(
                txn.DsAccess,
                xfer.To,
                func (ioAcct *pdi.StorageAccount) error {
                    return ioAcct.Deposit(xfer) 
                },
            )

            // Bail if any deposit returns an err
            if err != nil {
                break
            }
        }

        txn.Finish(err)
    }

    return txn.FatalErr()
}

const txnKeyPrefix = "/"
const txnKeyPrefixLen = 1

func (St *Store) doCommitJob(commitJob *CommitJob) {


    var (
        txnInfo pdi.TxnInfo
        dsKey ds.Key
    )

    _, err := St.TxnDecoder.DecodeRawTxn(
        commitJob.ReadiedTxn.RawTxn,
        &txnInfo,
    )

    var txnMetaInfo pdi.TxnMetaInfo

    if err == nil {
        
        txnMetaInfo.TxnStatus = pdi.TxnStatus_COMMITTING
        commitJob.Outlet.Send(&txnMetaInfo)

        txn := St.NewTxnHelper()

        for txn.NextAttempt() {

            // Debit the senders account (from Fuel and any transfers ordered)
            {
                fuelForTxn := St.Epoch.FuelPerTxn + int64(St.Epoch.FuelPerKb) * int64( len(commitJob.ReadiedTxn.RawTxn) >> 10 )

                dsKey, err = St.updateAccount(
                    txn.DsAccess,
                    txnInfo.From,
                    func (ioAcct *pdi.StorageAccount) error {

                        // Debit Fuel needed for txn
                        if ioAcct.FuelBalance < fuelForTxn {
                            return plan.Errorf(nil, plan.InsufficientFuel, "insufficient Fuel for txn cost of %v", fuelForTxn)
                        }

                        ioAcct.FuelBalance -= fuelForTxn

                        // Debit explicit transfers
                        for _, xfer := range txnInfo.Transfers {
                            err = ioAcct.Withdraw(xfer)
                            if err != nil {
                                return err
                            }
                        }

                        return nil
                    },
                )
            }

            // Deposit txn credits to recipients
            if err == nil {
                for _, xfer := range txnInfo.Transfers {
                    _, err = St.updateAccount(
                        txn.DsAccess,
                        xfer.To,
                        func (ioAcct *pdi.StorageAccount) error {
                            return ioAcct.Deposit(xfer)
                        },
                    )
                }
            }

            // Write the raw txn
            if err == nil {
                key := UTIDKeyForTxn(&txnInfo)
                err := txn.DsAccess.Put(key, commitJob.ReadiedTxn.RawTxn)
                if err != nil {
                    err = plan.Error(err, plan.StorageNotReady, "failed to write raw txn data to db")
                }
            }

            txn.Finish(err)
        }

        err = txn.FatalErr()
    }


    if err == nil {

        // Since this is a centralized db, every txn committed is finalized
        txnMetaInfo.TxnStatus = pdi.TxnStatus_FINALIZED
        commitJob.Outlet.Send(&txnMetaInfo)

    } else {

        perr, _ := err.(*plan.Err)
        if perr == nil {
            perr = plan.Error(err, plan.FailedToCommitTxn, "txn commit failed")
        }

        St.log.WithFields(log.Fields{
            "dsKey": dsKey,
        }).Warn(err)

        txnMetaInfo.TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
        txnMetaInfo.Alert =  &plan.Status{
            Code: perr.Code,
            Msg: perr.Msg,
        }
        commitJob.Outlet.Send(&txnMetaInfo)

        St.log.WithError(err).Warn("CommitJob error")
    }

    // This releases the GRPC handler
    commitJob.OnComplete <- err
}


func (St *Store) doQueryJob(job *QueryJob) {

    // Note how if len(job.TxnQuery.TxnHashname) == 0, then .Prefix becomes only the encoded timestamp 
    qryParams := dsq.Query{
        Prefix: "/0",
        SeekPrefix: pdi.FormUTID("/", job.TxnQuery.TimestampMin, nil),
        KeysOnly: true,
    }

    qry, err := St.ds.Query(qryParams)
    if err != nil {
        err = plan.Error(err, plan.TxnQueryFailed, "db.Query() failed")
    }

    stopKey := pdi.FormUTID("/", job.TxnQuery.TimestampMax, nil)

    const batchMax = 20
    var (
        txnUTID     [batchMax]string
    )

    for err == nil && qry != nil {
    
        batchCount := int32(0)
        totalCount := int32(0)
        approxSz := 0

        for result := range qry.Next() {

            err = result.Error
            if err != nil {
                err = plan.Error(err, plan.TxnQueryFailed, "dsq.Query.Next() error")
            }

            if result.Key > stopKey || err != nil {
                qry.Close()
                qry = nil
                break
            }

            approxSz += 100
            txnUTID[batchCount] = result.Key[2:]

            batchCount++
            totalCount++
            if totalCount == job.TxnQuery.MaxTxns {
                qry.Close()
                qry = nil
                break
            } else if batchCount == batchMax {
                break
            }
        }

        if err == nil && batchCount > 0 {
            job.Outlet.Send(&pdi.TxnBatch{
                UTIDs: txnUTID[:batchCount],
            })
        }

        // IF we didn't get to the batchMax, then we know we're donezo
        if batchCount < batchMax {
            break
        }
    }

    if err != nil {
        St.log.WithError(err).Warn("QueryJob error")   
    }

    // This releases the GRPC handler
    job.OnComplete <- err

}
