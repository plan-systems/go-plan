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
    
    
The client has 3 main op modes:

A) admin mode -- assuming the user is an admin, new members can be created, etc
B) channel talk/interact mode 
    1) fetches the community registry (for step 3)
    2) opens the specified channel ID as a talk channel (hash of some input str)
    3) displays all entries in the cannel (mapping each author member ID to the member's alias using registry from step 1)
    4) newly typed entries are sent to the channel, etc
C) diagnostic (simulated traffic) mode -- sends and checks dense traffic for testing, etc.


Maybe pnode and pgateway are fused and it's storage layer this is remote (or is implmemented locally but uses RPC call to, say, matrix)


    1)  If not already running, the client starts pnode
    2)  The client starts a gRPC session is started with pnode
    3)  One of 2 authentication methods 
        - the client prompts the user to authenticate w/ a password that pnode uses to unlock the personal keyring
        - the client uses their crypto device and unlocks their keyring that way
    4)  With the personal keyring unlocked, the gateway can now decrypt and encrypt community entries and personal entries
    5a) With the community keyring in hand, pnode can now process entries from the designated storage provider(s) in the background
        - During this time, pnode "syncs" up (relatively fast since the data is already locally within the storage layer)
    6)  The client issues channel queries and awaits decrypted entries
    7)  pnode handle queries, decrypting entries on the fly and sending them to the client
    8)  When the client authors a new entry, it's sent to pnode where it is encrypted and forwarded onto pnode for processing.  


0) Unity client starts
    a) launches local pgateway
    b) which community IDs to unlock (and pw for each)
    c) pgateway.StartSession() w/ pws encrypted by pgateway's private key (a KeyList with an SKI invocation that makes is a key loaders)
1) pgateway instantiates new SKI provider based on 0.(c)
    a) provider loads keyrings from disk, dencrypting each w/ the given pws
    b) pgateway now has full user keyrings
2) gateway logs into pnode(s), passing it itx txn signing key 
3) newly authored entries are authored in the client and sent to pgateway
4) new entries are encrypted and signed with the gateway's keys
5) new entries are packaged into a plan.Block and committed to the StorageProvider. 
*/



func main() {


}
