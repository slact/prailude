Prailude, a full RaiBlocks node and wallet
------------------------

## Overview

Prailude is an independent implementation of the Nano node that will be 100% compatible with the Nano network. It will have complete feature parity with the official node, as well as extra features and optimizations, and could serve as a testbed for Nano development in the future.

This is a feverish work in progress. Stay tuned.

Contributions of encouragement welcome. [xrb_1f3ta53d59z8x9ixooz1ypzcparkboq935a67war46qcqinre9wb3sujnecj](xrb:xrb_1f3ta53d59z8x9ixooz1ypzcparkboq935a67war46qcqinre9wb3sujnecj)

## Progress

There's still a lot of work to do, but much has also been completed. **BONUS** labels denote features not found in the core node.

* Node
  * Data storage - **DONE**  
    I've chosen to use Sqlite3 as the data store. The db logic is entirely encapsulated and can be swapped for something more performant later if necessary.
  * Crypto - **DONE**  
    All crypto primitives are implemented. The most efficient implementation of ed25519 -- `Ed25519-donna` is used.
    * **BONUS** Batch signature verification - **DONE**  
      Ed25519 has the peculiar property that it is more efficient to verify several signatures at once than one at a time. In my benchmarks, this offers a 40-70% decrease in CPU cycles used for sig checking. Considering sig checking is the most CPU-hungry task save for generating PoW, this is pretty good.
  * Protocol
    * Parsing - **95%**  
      Everything except generating `bulk_pull` and `frontier_req` responses is finished. Parsing of all incoming network data is complete.
  * Network
    * Peer Discovery - **DONE**  
      Keepalives are sent and processed as needed
    * Bootstrapping
      * Frontier fetch - **DONE**  
        Frontiers are fetched from several peers in parallel, with detailed progress reports. Frontiers that are too small are detected in the first second of data transfer and aborted to move on to better peers.
      * Account pulls - **DONE**  
        Frontiers get loaded from the faster peers in parallel, and peers failing to respond with useful data are de-prioritized.
      * Fork resultion - **TODO**  
        Forks encountered during bootstrapping need to be resolved before proceeding. There are several heuristics available to do this, as well as using `confirm_req`s to resolve specific blocks when the heuristics fail. This is one of the most complex parts of the Nano protocol.
      * Block verification - **60%**  
        Once loaded, blocks must be verified and stored in the database. Any issues here must be resolved  before considering the node as synced.
      * Process pending-blocks backlog - **TODO**  
        While we were bootstrapping, a whole bunch of blocks make their wait to us on the network.
    * Block handling - **TODO**  
      Incoming blocks must be verified for proof-of-work, associated with the owner's account, signature-checked, and verified that its data (like balance) is consistent. Afterwards, votes for the block must be tallied until quorum is reached. All newly seen (and valid) blocks must be rebroadcast to at least some peers.
    * Vote handling -  **30%**  
      Votes must be validated ( **DONE** ), associated with pending blocks, and tallied. Votes on forking blocks must be respected.
    * Serving as a representative - **TODO**  
      As a voting representative, the node must vote on incoming blocks, handle fork elections and respect their results.
    * Publishing blocks - **50%**  
      In response to user input, the node must publish transactions onto the network.
    * **BONUS** Node reputation tracking - **30%**  
      remember which nodes were helpful during bootstrapping, which ones were out of sync, too slow, or outright malicious. Which ones publish invalid blocks or votes. Treat them accordingly.
  * Wallet
    * Seeding - **TODO**  
      Accounts must be generated from a seed, which can be backed up and restored. The backup should be compatible with the core node's format.
    * Balance tracking - **40%**  
      Balances must be kept up-to-date and xrb/raw units must be respected.
    * **BONUS** Account groups  
      several accounts can be grouped together to form a single usable entity tracking its balance and using the sub-accounts as needed for transactions
  * Interface
    * RPC - **TODO**  
      Feature-complete RPC interface with parity to core node, and then some.
    * Websocket - **TODO**  
      Enables realtime tracking of data.
    * GUI Wallet  
      Write an Electron app (sorry, i know it's a bloat, but I don't have time to learn Qt) or use Canoe.
  * **BONUS** Named Accounts  
    Unique, decentralized, distributed account naming system, built by thoroughly reusing the Nano protocol with a slightly different DAG validation scheme. I've already worked out most of the details, and this is readily possible. In order for this system to go live, however, most representatives would need to run a supporting rep node.
  
