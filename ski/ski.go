package ski


import "github.com/plan-tools/go-plan/plan"



// CryptArgs maps parameter keys to values
type CryptArgs map[string][]byte


// ArgKey* are strings used as keys for CryptArgs when calling SKI.DispatchOp()
const (
    ArgKeyVersion           = "version"

    ArgKeySenderPubKey      = "sender"
    ArgKeyRecipientPubKey   = "recip"
    ArgKeySignerPubKey      = "signer"
    ArgKeyMulticodecName    = "mc"
    ArgKeyMsg               = "msg" 
    ArgKeySig               = "sig"
    ArgKeySymmetricKeyID    = "sym_kid"
)


var (

    // CryptSKIVersion should be used for ArgKeyVersion
    CryptSKIVersion = []byte("0.1")
)


// CryptOp specifies which cryptographic operation to perform and implies parameters the CryptArgs to expect.
type CryptOp string 
const (

    // OpEncryptSymmetric encrypts ArgKeyMsg using the key referenced by ArgKeySymmetricKeyID (symmetric crypto)
    OpEncryptSymmetric      = "encrypt_sym"

    // OpDecryptSymmetric decrypts ArgKeyMsg using the key referenced by ArgKeySymmetricKeyID (symmetric crypto)
    OpDecryptSymmetric      = "decrypt_sym"



    // OpEncryptForID encrypts ArgKeyMsg for ArgKeyRecipientPubKey, using the private key paired with ArgKeySenderPubKey (asymmetric crypto)
    OpEncryptForID          = "encrypt_for_id"

    // OpDecryptFromID decrypts ArgKeyMsg from ArgKeySenderPubKey, using the private key paired with ArgKeyRecipientPubKey (asymmetric crypto)
    OpDecryptFromID         = "decrypt_from_id"
    


    // OpSignMsg creates a signature buffer of ArgKeyMsg using the private key paired with ArgKeySignerPubKey
    OpSignMsg               = "sign_msg"

    // OpVerifySig verifies that ArgKeySig is in fact the signature of ArgKeyMsg signed using the private key paired with ArgKeySignerPubKey
    OpVerifySig             = "verify_sig"
)




// SKI (Secure Key Interface) provides asynchronous crypto services for an opaque key holder.  
type SKI interface {
    
    // DispatchOp is dispatches the given op and args for completetion via inOnCompletion()
    DispatchOp( inOp CryptOp, inArgs CryptArgs, inOnCompletion plan.DataHandler)

}





