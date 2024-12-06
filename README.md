
# Brook-2PL
This repository showcases the development of brook-2pl, a deadlock-free two-phase locking protocol designed for high contention workloads. It (1) introduces SLW-Graph for enabling deadlock-free transaction execution, and (2) proposes partial transaction chopping to facilitate early lock release.


## Structure

* [RetryFreeDB](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB) implements a transaction system featuring Brook-2PL, Wound-Wait, and Bamboo as two-phase locking protocols.
  * The [repo](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/repo) directory contains the implementation of the transaction system.
  * The [slwGraph](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/slwGraph) directory includes an application designed to verify the correctness of the SLW-Graph representation of transactions.
  * The [benchmark](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/benchmark) directory contains a benchmarking application to evaluate the performance of the retry-free database system.

For a list of supported operations by RetryFreeDB, please refer to the [valid RPCs](https://github.com/gsoosk/Brook-2PL/blob/main/src/main/proto/RetryFreeDB.proto).
