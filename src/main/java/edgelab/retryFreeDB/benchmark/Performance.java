package edgelab.retryFreeDB.benchmark;



import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.Client;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.logging.log4j.core.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.append;


@Slf4j
public class Performance {

    private class ServerRequest {
        enum Type {
            BUY,
            SELL
        }
        @Getter
        private long transactionId;
        @Getter
        private Type type;
        @Getter
        private int retried;
        @Getter
        private Map<String, String> values;
        @Getter
        private Long start;
        public ServerRequest(Type type, long batchId, Map<String, String> values, Long start) {
            this.type = type;
            this.values = values;
            this.transactionId = batchId;
            this.start = start;
            this.retried = 0;
        }


        public void retry() {
            this.retried++;
        }

    }
    private static final Integer INFINITY = -1;

    private RetryFreeDBServerGrpc.RetryFreeDBServerBlockingStub datastore;
    private RetryFreeDBServerGrpc.RetryFreeDBServerStub asyncDatastore;
    private ManagedChannel channel;
    private void connectToDataStore(String address, int port) throws MalformedURLException, RemoteException {
        this.channel = ManagedChannelBuilder.forAddress(address, port).maxInboundMessageSize(Integer.MAX_VALUE).usePlaintext().build();
        ManagedChannel asyncChannel = ManagedChannelBuilder.forAddress(address, port).maxInboundMessageSize(Integer.MAX_VALUE).usePlaintext().build();
        this.datastore = RetryFreeDBServerGrpc.newBlockingStub(channel);
        this.asyncDatastore = RetryFreeDBServerGrpc.newStub(asyncChannel);

        while (true) {
            try {
                if (this.channel.getState(true) == ConnectivityState.READY &&
                        asyncChannel.getState(true) == ConnectivityState.READY) {
                    log.info("Connected to grpc server");
                    break;
                }
            } catch (Exception e) {
                System.out.println("Remote connection failed, trying again in 5 seconds");
                log.error("Remote connection failed", e);
                // wait for 5 seconds before trying to re-establish connection
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
    }

    private Namespace res;

    public static void main(String[] args) throws Exception{
        Performance perf = new Performance();

        perf.parseArguments(args);
        perf.start();


    }

    private void parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = argParser();
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                parser.handleError(e);
            }
           throw e;
        }
    }


    private String address;
    private int port;

//    Multi-threading
    private final int MAX_THREADS = 8;
    private final int MAX_QUEUE_SIZE = 16;
    private ThreadPoolExecutor executor;
    private Client client;
    private long txId = 0;

    void start() throws Exception {
        /* parse args */
        address = res.getString("address");
        port = res.getInt("port");

        Integer batchSize = res.getInt("batchSize");
        Double interval = res.getDouble("interval");

        List<Integer> dynamicBatchSize = res.getList("dynamicBatchSize");
        List<Integer> dynamicBatchTimes = res.getList("dynamicBatchTime");
        int currentBatchIndex = 0;
        boolean dynamicBatching = false;
        if (dynamicBatchSize != null && dynamicBatchTimes != null ) {
            if (dynamicBatchSize.size() != dynamicBatchTimes.size())
                throw new Exception("number of dynamic batch times and batch sizes should be equal");
            batchSize = dynamicBatchSize.get(currentBatchIndex);
            dynamicBatching = true;
        }
        List<Double> dynamicInterval = res.getList("dynamicInterval");
        List<Integer> dynamicIntervalTime = res.getList("dynamicIntervalTime");
        int currentIntervalIndex = 0;
        boolean dynamicIntervaling = false;
        if (dynamicInterval != null && dynamicIntervalTime != null ) {
            if (dynamicInterval.size() != dynamicIntervalTime.size())
                throw new Exception("number of dynamic interval times and intervals should be equal");
            interval = dynamicInterval.get(currentIntervalIndex);
            dynamicIntervaling = true;
        }

        Integer recordSize = res.getInt("recordSize") == null ? 255 : res.getInt("recordSize") ;
        long warmup = batchSize != null ? (batchSize / recordSize) * 4L : 100;
        long numRecords = res.getLong("numRecords") == null ? Integer.MAX_VALUE - warmup - 2 : res.getLong("numRecords");
        numRecords += warmup;
        int throughput = res.getInt("throughput");
        String payloadFilePath = res.getString("payloadFile");
        String resultFilePath = res.getString("resultFile") == null ? "result.csv" : res.getString("resultFile") ;
        String metricsFilePath = res.getString("metricsFile") == null ? "metrics.csv" : res.getString("metricsFile") ;
        String partitionId = res.getString("partitionId");
        // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
        String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");
        Long benchmarkTime = res.getLong("benchmarkTime");
//        Integer timeout = res.getInt("timeout") != null ? res.getInt("timeout") : interval.intValue() * 2;
        Integer maxRetry = res.getInt("maxRetry");

        Boolean exponentialLoad = res.getBoolean("exponentialLoad") == null ? false : res.getBoolean("exponentialLoad");

        List<Long> dynamicMemory = res.getList("dynamicMemory");
        List<Integer> dynamicMemoryTime = res.getList("dynamicMemoryTime");

        //         METRICS
        DefaultExports.initialize(); // export jvm
        HTTPServer metricServer = new HTTPServer.Builder()
                .withPort(9002)
                .build();

//        connectToDataStore(address, port);

        Stats stats = new Stats(numRecords, 1000, resultFilePath, metricsFilePath, recordSize, 0, 0.0, 0, 20000);
        long startMs = System.currentTimeMillis();

        ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);
//        if (memoryTrigger) {
//            datastore.changeMemory(Size.newBuilder().setValue(dynamicMemory.get(currentMemoryIndex)).build());
//            stats.changeMemory(dynamicMemory.get(currentMemoryIndex));
//        }

        log.info("Running benchmark for partition: " + partitionId);

        Thread.sleep(3000);


        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);
        client = new Client(address, port);

        long sendingStart = System.currentTimeMillis();

//            TODO: Refactor this function
        long startToWaitTime = System.currentTimeMillis();
        for (long i = 0; i < numRecords; i++) {

            long sendStartMs = System.currentTimeMillis();
            stats.report(sendStartMs);
            ServerRequest request = getNextRequest();

            executeRequestFromThreadPool(request, stats);
//            sendRequestAsync(stats, recordSize, request, c, warmup, maxRetry, timeout, partitionId);


            if (benchmarkTime != null) {
                long timeElapsed = (sendStartMs - sendingStart) / 1000;
                if (timeElapsed >= benchmarkTime)
                    break;
            }



            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
        // wait for retries to be done
//        Thread.sleep(timeout * 3L);


        // TODO: closing open things?
        /* print final results */
        stats.printTotal();
        metricServer.close();

    }

    private ServerRequest getNextRequest() {
//        TODO: Change to correct tx selection
        long sendStartMs = System.currentTimeMillis();
        Map<String, String> tx = new HashMap<>();
        Random rand = new Random();
        tx.put("PId", Integer.toString(rand.nextInt(1, 500000)));
        tx.put("LId", Integer.toString(rand.nextInt(1, 100000)));
        return new ServerRequest(ServerRequest.Type.BUY, txId++, tx , sendStartMs);
    }

    private void executeRequestFromThreadPool(ServerRequest request, Stats stats) {

        stats.nextAdded(1);
        executor.submit(() -> {
            if (request.getType() == ServerRequest.Type.BUY) {
                client.buyListingSLW(request.getValues().get("PId"), request.getValues().get("LId"));
            }
            else if (request.getType() == ServerRequest.Type.SELL) {
                client.addListingSLW(request.getValues().get("PId"), request.getValues().get("IId"), 1);
            }
            stats.nextCompletion(request.start, 1);
            return null;
        });

        while (executor.getQueue().size() >= MAX_QUEUE_SIZE) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    private void sendRequestAsync(Stats stats, serverRequest serverRequest, int maxRetry, long timeout) throws  InterruptedException {
//
//        StreamObserver<Result> observer = new StreamObserver<>() {
//            @Override
//            public void onNext(Result result) {
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                 serverRequest.retry();
//                if (serverRequest.getRetried() < maxRetry) {
//                    try {
//                        log.info("Retrying batch request {}, number of retried: {}", serverRequest.getBatchId(), serverRequest.getRetried());
//                        stats.addRetry(serverRequest.batchId);
//                        sendRequestAsync(stats, recordSize, serverRequest, c, warmup, maxRetry, timeout, partitionId);
//                    } catch (ExecutionException e) {
//                        throw new RuntimeException(e);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                else {
//                    log.info("batch request {} reached max retry", serverRequest.getBatchId());
//                }
//            }
//
//            @Override
//            public void onCompleted() {
//                log.info("Batch request {} finished", serverRequest.getBatchId());
//                stats.completeRetry(serverRequest.getBatchId());
////            log.info("Completed batch request {}", batchRequest.getBatchId());
//                long end = System.currentTimeMillis();
////                for (int j = 0; j < batchRequest.getStarts().size() ; j++) {
////                    if (c + j > warmup)
////                        stats.nextCompletion(batchRequest.getStarts().get(j), end, recordSize + 5);
////                }
//                stats.nextBatchCompletion(serverRequest.getValues().size(), serverRequest.getStarts().get(0), end, (recordSize + 5) * serverRequest.getValues().size());
//                log.info("Batch request {} recorded", serverRequest.getBatchId());
//            }
//        };
//
//
//        log.info("Sending Request {} with size {}", serverRequest.getBatchId(), (recordSize + 5) * serverRequest.getValues().size() / (1000 * 1000));
//        stats.nextAdded((recordSize + 5) * serverRequest.getValues().size());
//        asyncDatastore.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).batch(Values.newBuilder()
//                .setId(serverRequest.batchId)
//                .setPartitionId(partitionId)
//                .putAllValues(serverRequest.getValues())
//                .build()
//        , observer);
//
//        log.info("batch request {} submitted", serverRequest.getBatchId());
//    }

    static byte[] generateRandomPayload(Integer recordSize, List<byte[]> payloadByteList, byte[] payload,
                                        Random random) {
        if (!payloadByteList.isEmpty()) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        } else if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else {
            throw new IllegalArgumentException("no payload File Path or record Size provided");
        }
        return payload;
    }


    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            log.info("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            log.info("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        return payloadByteList;
    }

    /** Get the command-line argument parser. */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

//        MutuallyExclusiveGroup payloadOptions = parser
//                .addMutuallyExclusiveGroup()
//                .required(true)
//                .description("either --record-size or --payload-file must be specified but not both.");

        MutuallyExclusiveGroup numberOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --num-records or --benchmark-time must be specified but not both.");

        parser.addArgument("--address")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ADDRESS")
                .dest("address")
                .help("leader's address");

        parser.addArgument("--port")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("PORT")
                .dest("port")
                .help("leader's port");

        numberOptions.addArgument("--num-records")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        numberOptions.addArgument("--benchmark-time")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("BENCHMARK-TIME")
                .dest("benchmarkTime")
                .help("benchmark time in seconds");

//        payloadOptions.addArgument("--record-size")
//                .action(store())
//                .required(false)
//                .type(Integer.class)
//                .metavar("RECORD-SIZE")
//                .dest("recordSize")
//                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

//        payloadOptions.addArgument("--payload-file")
//                .action(store())
//                .required(false)
//                .type(String.class)
//                .metavar("PAYLOAD-FILE")
//                .dest("payloadFile")
//                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
//                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
//                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--result-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("RESULT-FILE")
                .dest("resultFile")
                .help("a csv file containing the total result of benchmark");

        parser.addArgument("--metric-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("METRIC-FILE")
                .dest("metricsFile")
                .help("a csv file containing the timeline result of benchmark");

        parser.addArgument("--partition-id")
                .action(store())
                .required(false)
                .setDefault("")
                .type(String.class)
                .metavar("PARTITION-ID")
                .dest("partitionId")
                .help("Id of the partition that you want to put load on");

        parser.addArgument("--batch-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("BATCH-SIZE")
                .dest("batchSize")
                .help("batch size in bytes. This producer batches records in this size and send them to kv store");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--interval")
                .action(store())
                .required(false)
                .type(Double.class)
                .dest("interval")
                .metavar("INTERVAL")
                .help("interval between each packet.  Set this -1 to send packets blocking");

        parser.addArgument("--timeout")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("timeout")
                .metavar("TIMEOUT")
                .help("timeout of each batch request. It is two times of interval by default");

        parser.addArgument("--max-retry")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .dest("maxRetry")
                .metavar("MAXRETRY")
                .help("Maximum number of times a request can be retried");

        parser.addArgument("--dynamic-batch-size")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchSize")
                .metavar("DYNAMICBATCHSIZE")
                .help("dynamic batch size until a specific time");

        parser.addArgument("--dynamic-batch-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchTime")
                .metavar("DYNAMICBATCHTIME")
                .help("deadline for a dynamic batch size");


        parser.addArgument("--dynamic-interval")
                .action(append())
                .required(false)
                .type(Double.class)
                .dest("dynamicInterval")
                .metavar("DYNAMICINTERVAL")
                .help("dynamic interval until a specific time");

        parser.addArgument("--dynamic-interval-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicIntervalTime")
                .metavar("DYNAMICINTERVALTIME")
                .help("deadline for a dynamic interval");

        parser.addArgument("--exponential-load")
                .action(store())
                .required(false)
                .type(Boolean.class)
                .dest("exponentialLoad")
                .metavar("EXPONENTIALLOAD")
                .help("requests follow an exponential random distribution with lambda=1000/interval");

        parser.addArgument("--dynamic-memory")
                .action(append())
                .required(false)
                .type(Long.class)
                .dest("dynamicMemory")
                .metavar("MEMORYTRIGGER")
                .help("trigger the memory after trigger time to this amount");

        parser.addArgument("--dynamic-memory-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicMemoryTime")
                .metavar("MEMORYTRIGGERTIME")
                .help("time of memory trigger");


        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        private long numRecords;
        private int recordSize;
        private int batchSize;
        private String resultFilePath;
        private double interval;
        private int timeout;
        private Map<Long, Integer> retries;
        private int previousWindowRequestRetried;
        private int previousWindowRetries;
        private int completedRetries;
        private int previousWindowCompletedRetries;

        private String metricsFilePath;
        private int started;
        private int startedBytes;
        private int startedWindowBytes;

        // Metrics
        private static final Summary finishedRequestsBytes = Summary.build()
                .name("paxos_requests_finished_bytes")
                .help("size of requests that finished")
                .register();
        private static final Summary addedRequestsBytes = Summary.build()
                .name("paxos_requests_sent_bytes")
                .help("size of requests sent to the leader")
                .register();
        private static final Gauge batchSizeGauge = Gauge.build()
                .name("paxos_batch_size")
                .help("Batch size")
                .register();
        private static final Gauge intervalGauge = Gauge.build()
                .name("paxos_interval_total")
                .help("Interval between requests")
                .register();
        private static final Gauge timeoutGauge = Gauge.build()
                .name("paxos_timeout_total")
                .help("timeout of a request")
                .register();
        private static final Summary finishedRequestsLatency = Summary.build()
                .name("paxos_requests_finished_latency")
                .help("Latency of requests responded")
                .quantile(0.5, 0.001)    // 0.5 quantile (median) with 0.01 allowed error
                .quantile(0.95, 0.005)  // 0.95 quantile with 0.005 allowed error
                .register();
        private static final Counter requestRetried = Counter.build()
                .name("paxos_requests_retired_total")
                .help("Number of requests that retried")
                .register();
        private static final Counter allRetries = Counter.build()
                .name("paxos_request_retries_total")
                .help("Number of request retries")
                .register();
        private static final Counter retriesCompleted = Counter.build()
                .name("paxos_request_retry_completed_total")
                .help("Number of retries that has been compelted")
                .register();

        private static final Gauge memoryGauge = Gauge.build()
                .name("memory_usage")
                .help("Memory usage of database")
                .register();

        public Stats(long numRecords, int reportingInterval, String resultFilePath, String metricsFilePath, int recordSize, int batchSize, Double interval, int timeout, long memory) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.started = 0;
            this.startedBytes = 0;
            this.startedWindowBytes = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.previousWindowRequestRetried = 0;
            this.previousWindowRetries = 0;
            this.completedRetries = 0;
            this.previousWindowCompletedRetries = 0;
            this.reportingInterval = reportingInterval;
            this.resultFilePath = resultFilePath;
            this.metricsFilePath = metricsFilePath;
            this.numRecords = numRecords;
            this.recordSize = recordSize;
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
            this.interval = interval;
            intervalGauge.set(interval);
            this.timeout = timeout;
            timeoutGauge.set(timeout);
            memoryGauge.set(memory);
            this.retries = new HashMap<>();
            createResultCSVFiles(resultFilePath, metricsFilePath);
        }

        private void createResultCSVFiles(String resultFilePath, String metricFilePath) {
            if (!Files.exists(Paths.get(resultFilePath))){
                String CSVHeader = "num of records, record size, interval, timeout, batch size, throughput, goodput, average latency, max latency, 50th latency, 95th latency, requests retried, retries, completed retries\n";
                try {
                    BufferedWriter out = new BufferedWriter(
                            new FileWriter(resultFilePath, true));

                    // Writing on output stream
                    out.write(CSVHeader);
                    // Closing the connection
                    out.close();
                }
                catch (IOException ex) {
                    log.warn("Invalid path");
                }
            }

            String CSVHeader = "num of records, record size, interval, timeout, batch size, throughput, goodput, average latency, max latency, requests retried, retries, completed retries\n";
            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(metricFilePath, false));

                // Writing on output stream
                out.write(CSVHeader);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                log.warn("Invalid path");
            }
        }

        public synchronized void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            finishedRequestsBytes.observe(bytes);
            finishedRequestsLatency.observe((double) latency/1000);
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            report(time);
        }

        private void report(long time) {
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public void nextCompletion(long start, int bytes) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            record(iteration, latency, bytes, now);
            this.iteration++;
        }

        public void nextBatchCompletion(long quantity, long start, long end, int bytes) {
            this.count += quantity - 1;
            this.windowCount += quantity - 1;
            nextCompletion(start, end, bytes);
        }

        public synchronized void nextAdded(int bytes) {
            long now = System.currentTimeMillis();
            this.started++;
            addedRequestsBytes.observe(bytes);
            this.startedBytes += bytes;
            this.startedWindowBytes += bytes;

            report(now);
        }

        public void nextCompletion(long start, long end, int bytes) {
            int latency = (int) (end - start);
            record(iteration, latency, bytes, end);
            this.iteration++;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedWindowBytes / (double) elapsed / (1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.1f ms avg latency, %.1f ms max latency.%n%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);



            String resultCSV = String.format("%d,%d,%.1f,%d,%d,%.3f,%.3f,%.2f,%.2f,%d,%d,%d\n",
                    count,
                    recordSize,
                    interval,
                    timeout,
                    batchSize,
                    throughputMbPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) maxLatency,
                    retries.size() - previousWindowRetries,
                    retries.values().stream().mapToInt(Integer::intValue).sum() - previousWindowRequestRetried,
                    completedRetries - previousWindowCompletedRetries);

            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(metricsFilePath, true));

                // Writing on output stream
                out.write(resultCSV);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                log.warn("Invalid path");
            }
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.startedWindowBytes = 0;
            previousWindowRetries = retries.size();
            previousWindowRequestRetried = retries.values().stream().mapToInt(Integer::intValue).sum();
            previousWindowCompletedRetries = completedRetries;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedBytes / (double) elapsed / (1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n --  requests retried: %d, retries: %d\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3],
                    retries.size(),
                    retries.values().stream().mapToInt(Integer::intValue).sum());

            String resultCSV = String.format("%d,%d,%f,%d,%d,%.3f,%.3f,%.2f,%.2f,%d,%d,%d,%d,%d\n",
                    count,
                    recordSize,
                    interval,
                    timeout,
                    batchSize,
                    throughputMbPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    retries.size(),
                    retries.values().stream().mapToInt(Integer::intValue).sum(),
                    completedRetries);
            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(resultFilePath, true));

                // Writing on output stream
                out.write(resultCSV);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                log.warn("Invalid path");
            }
            log.info(resultCSV);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        public void addRetry(long c) {
            allRetries.inc();
            if (retries.containsKey(c))
                retries.put(c, retries.get(c) + 1);
            else {
                retries.put(c, 1);
                requestRetried.inc();
            }
        }

        public Integer getNumberOfRetries(long c) {
            return retries.getOrDefault(c, 0);
        }

        public void updateBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
        }


        public void updateInterval(Double interval) {
            this.interval = interval;
            intervalGauge.set(interval);
        }

        public void completeRetry(long batchId) {
            if (retries.containsKey(batchId)) {
                retries.remove(batchId);
                retriesCompleted.inc();
                completedRetries++;
            }
        }

        public boolean isThereAnyRetry() {
            return !retries.isEmpty();
        }

        public void changeMemory(long memoryTrigger) {
            memoryGauge.set(memoryTrigger);
        }
    }



}