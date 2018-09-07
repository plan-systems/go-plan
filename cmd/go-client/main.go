package main

/**********************************************************************************************************************

The purpose of this exe is to be a test/demo stand-in for the full PLAN Unity-based client.

Here's roughly what the order of events:
    1)  If not already running, the client starts a pgateway daemon
    2)  A gRPC session is started with pgateway. 
    3)  One of 2 authentication methods 
        - the client prompts the user to authenticate w/ a password that the gateway uses to unlock the personal keyring
        - the client uses their crypto device and unlocks their keyring that way
    4)  With the personal keyring unlocked, the gateway can now decrypt and encrypt community entries
    5)  pgateway starts a new session with a community pnode and authenticates by decrypting a challenge issued by pnode,
        encrypting using the client's public key
    6)  pgateway challenges the pnode back by using a key for specific pnode id.
    7)  With the pnode now verified, pgateway sends the pnode the community keyring
    8a) With the community keyring in hand, pnode can now process entries from the designated storage provider(s) in the background
        - During this time, pnode "syncs" up (relatively fast since the data is already locally within the storage layer)
    8)  The client issues channel queries and awaits decrypted entries
    9)  Pgateway passes on the clients requests to pnode
    9)  pnode sends responses (encrypted entries) to pgateway, 
    10) pgateway decrypts entries as they arrive and forwards them them to the client (decrypted)
    11)  When the client authors a new entry, it's sent to pgateway where it is encrypted and forwarded onto pnode for processing.  
    
    
*/



func main() {


}
