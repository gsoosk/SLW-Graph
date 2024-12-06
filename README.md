
# Brook-2PL
This repository showcases the development of brook-2pl, a deadlock-free two-phase locking protocol designed for high contention workloads. It (1) introduces SLW-Graph for enabling deadlock-free transaction execution, and (2) proposes partial transaction chopping to facilitate early lock release.


## Structure

* [RetryFreeDB](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB) implements a transaction system featuring Brook-2PL, Wound-Wait, and Bamboo as two-phase locking protocols.
  * The [repo](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/repo) directory contains the implementation of the transaction system.
  * The [slwGraph](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/slwGraph) directory includes an application designed to verify the correctness of the SLW-Graph representation of transactions.
  * The [benchmark](https://github.com/gsoosk/Brook-2PL/tree/main/src/main/java/edgelab/retryFreeDB/benchmark) directory contains a benchmarking application to evaluate the performance of the retry-free database system.


For a list of supported operations by RetryFreeDB, please refer to the [valid RPCs](https://github.com/gsoosk/Brook-2PL/blob/main/src/main/proto/RetryFreeDB.proto).



## How to run? 

### Ansible
We provide two ansible playbooks to install requirements and build/deploy applications on servers specified in inventory.
For more information, please refer to their [README](./ansible/README.md).


### Data Initialization
To initialize the database, use [this ansible](https://github.com/gsoosk/Brook-2PL/blob/main/ansible/postgres.yml) script by changing the data to either `tpcc` or `store`. 
### Build
In order to run the following programs, their code should be built by maven: 
```
mvn clean install
```
### RetryFreeDB Server

Run the servers
```
java -jar ./target/Server.jar <server_port> <server_address> <postgres_port> <db_tables_seperated_by_comma>
```
### Performance
This program benchmarks a cluster by putting a desired load on the system based on the provided configuration. 

```
java -jar ./target/Client.jar --help                                                                                                       

usage: producer-performance [-h] --address ADDRESS --port PORT [--result-file RESULT-FILE] [--metric-file METRIC-FILE] [--partition-id PARTITION-ID]          
                            [--batch-size BATCH-SIZE] [--payload-delimiter PAYLOAD-DELIMITER] --throughput THROUGHPUT [--interval INTERVAL] [--timeout TIMEOUT]
                            [--dynamic-batch-size DYNAMICBATCHSIZE] [--dynamic-batch-time DYNAMICBATCHTIME] [--dynamic-interval DYNAMICINTERVAL]
                            [--dynamic-interval-time DYNAMICINTERVALTIME] [--exponential-load EXPONENTIALLOAD] [--dynamic-memory MEMORYTRIGGER]
                            [--dynamic-memory-time MEMORYTRIGGERTIME] --hot-players HOTPLAYERS --hot-listings HOTLISTING --hot-selection-prob HOTSELECTION
                            [--max-threads MAXTHREADS] [--max-retry MAXRETRY] [--max-items-threads MAXITEMSTHREADS] --read-item-number READITEMNUMBER
                            [--operation-delay OPERATIONDELAY] [--2pl-mode 2PLMODE] [--benchmark-mode BENCHMARK_MODE] [--num-of-warehouses WAREHOUSECOUNT]
                            [--hot-warehouses HOTWAREHOUSE] (--num-records NUM-RECORDS | --benchmark-time BENCHMARK-TIME)

This tool is used to verify the producer performance.

named arguments:
  -h, --help             show this help message and exit
  --address ADDRESS      leader's address
  --port PORT            leader's port
  --result-file RESULT-FILE
                         a csv file containing the total result of benchmark
  --metric-file METRIC-FILE
                         a csv file containing the timeline result of benchmark
  --partition-id PARTITION-ID
                         Id of the partition that you want to put load on (default: )
  --batch-size BATCH-SIZE
                         batch size in bytes. This producer batches records in this size and send them to kv store
  --payload-delimiter PAYLOAD-DELIMITER
                         provides delimiter to be used when --payload-file is provided. Defaults to  new  line. Note that this parameter will be ignored if --payload-file is
                         not provided. (default: \n)
  --throughput THROUGHPUT
                         throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.
  --interval INTERVAL    interval between each packet.  Set this -1 to send packets blocking
  --timeout TIMEOUT      timeout of each batch request. It is two times of interval by default
  --dynamic-batch-size DYNAMICBATCHSIZE
                         dynamic batch size until a specific time
  --dynamic-batch-time DYNAMICBATCHTIME
                         deadline for a dynamic batch size
  --dynamic-interval DYNAMICINTERVAL
                         dynamic interval until a specific time
  --dynamic-interval-time DYNAMICINTERVALTIME
                         deadline for a dynamic interval
  --exponential-load EXPONENTIALLOAD
                         requests follow an exponential random distribution with lambda=1000/interval
  --dynamic-memory MEMORYTRIGGER
                         trigger the memory after trigger time to this amount
  --dynamic-memory-time MEMORYTRIGGERTIME
                         time of memory trigger
  --hot-players HOTPLAYERS
                         path to hot players
  --hot-listings HOTLISTING
                         path to hot listings
  --hot-selection-prob HOTSELECTION
                         chance of a hot record being selected
  --max-threads MAXTHREADS
                         number of maximum threads for sending the load
  --max-retry MAXRETRY   Maximum number of times a request can be retried (default: -1)
  --max-items-threads MAXITEMSTHREADS
                         number of maximum threads for reading item
  --read-item-number READITEMNUMBER
                         number of items to read at the same time
  --operation-delay OPERATIONDELAY
                         the amount of time each operation will be delayed, mimicking the thinking time.
  --2pl-mode 2PLMODE     2pl algorithm used in server. ww: Wound-Wait, bamboo: Bamboo, slw: SLW-Graph (default: slw)
  --benchmark-mode BENCHMARK_MODE
                         type of the benchmark used. could be either "store" or "tpcc". (default: store)
  --num-of-warehouses WAREHOUSECOUNT
                         number of warehouses in tpcc benchmark. (default: 1)
  --hot-warehouses HOTWAREHOUSE
                         number of hot warehouse records in tpcc benchmark. (default: 0)

  either --num-records or --benchmark-time must be specified but not both.

  --num-records NUM-RECORDS
                         number of messages to produce
  --benchmark-time BENCHMARK-TIME
                         benchmark time in seconds
                                                                                                       
```
### Automated Benchmark
We provide some docker-compose and shell script scripts run `server` and `benchmark` programs automatically. For these scripts please refer to [here](https://github.com/gsoosk/Brook-2PL/tree/main/ansible/roles/build/files)


