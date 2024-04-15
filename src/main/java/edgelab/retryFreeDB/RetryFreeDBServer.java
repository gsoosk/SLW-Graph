package edgelab.retryFreeDB;

import edgelab.proto.Data;
import edgelab.proto.Empty;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import edgelab.retryFreeDB.repo.storage.Postgres;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RetryFreeDBServer {
    private final Server server;
    private final int port;
    public RetryFreeDBServer(int port, ServerBuilder<?> serverBuilder,String postgresAddress, String postgresPort, String[] tables) throws SQLException {
        this.server = serverBuilder
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .addService(new RetryFreeDBService(postgresAddress, postgresPort, tables))
                .build();
        this.port = port;
    }

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        int port = Integer.parseInt(args[0]);
        String postgresAddress = args[1];
        String postgresPort = args[2];
        String[] tables = args[3].split(",");
//      SERVER
        RetryFreeDBServer server = new RetryFreeDBServer(port, ServerBuilder.forPort(port), postgresAddress, postgresPort, tables);
        server.start();
        server.blockUntilShutdown();
    }

    public void start() throws IOException {
        server.start();
        log.info("Server started, listening on " + port);
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    @Slf4j
    public static class RetryFreeDBService extends RetryFreeDBServerGrpc.RetryFreeDBServerImplBase {
        private final Postgres repo;
        ConcurrentHashMap<String, Connection> transactions;
        ConcurrentHashMap<String, AtomicInteger> lastIdUsed = new ConcurrentHashMap<>();
        private final AtomicLong lastTransactionId;
        public RetryFreeDBService(String postgresAddress, String postgresPort, String[] tables) throws SQLException {
            repo = new Postgres(postgresAddress, postgresPort);
            transactions = new ConcurrentHashMap<>();
            lastTransactionId =  new AtomicLong(0);

            for (String table :
                    tables) {
                lastIdUsed.put(table, new AtomicInteger(repo.lastId(table)));
            }
            log.info("Table metadata initialization finished");

        }

        @Override
        public void beginTransaction(Empty request, StreamObserver<Result> responseObserver) {
            try  {
                Connection conn = repo.connect();
                conn.setAutoCommit(false);
                long transactionId = lastTransactionId.incrementAndGet();
                transactions.put(Long.toString(transactionId), conn);
                responseObserver.onNext(Result.newBuilder()
                                        .setStatus(true)
                                        .setMessage(Long.toString(transactionId)).build());
                responseObserver.onCompleted();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't connect/commit,  {}", ex.getMessage());
                responseObserver.onNext(Result.newBuilder()
                        .setStatus(false)
                        .setMessage("b error: couldn't connect/commit,  {}").build());
                responseObserver.onError(ex);
            }
        }

        private DBData deserilizeDataToDBData(Data request) {
            DBData d = DBTransaction.deserializeData(request);
            if (d instanceof DBInsertData && ((DBInsertData) d).getRecordId().isEmpty()) {
                lastIdUsed.get(d.getTable()).incrementAndGet();
                ((DBInsertData) d).setRecordId(lastIdUsed.get(d.getTable()).toString());
            }
            return d;
        }

        @Override
        public void lock(Data request, StreamObserver<Result> responseObserver) {
            if (checkTransactionExists(request.getTransactionId(), responseObserver)) return;

            Connection conn = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.lock(request.getTransactionId(), conn, d);
                    if (d instanceof DBInsertData)
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build());
                    else
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                    responseObserver.onCompleted();
                } catch (SQLException e) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not lock").build());
                    responseObserver.onError(new Exception("Could not lock"));
                }
            }

        }


        @Override
        public void lockAndUpdate(Data request, StreamObserver<Result> responseObserver) {
            if (checkTransactionExists(request.getTransactionId(), responseObserver)) return;

            Connection conn = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.lock(request.getTransactionId(), conn, d);
                } catch (SQLException e) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not lock").build());
                    responseObserver.onError(new Exception("Could not lock"));
                    return;
                }
                updateDBDataOnRepo(responseObserver, d, conn);
            }
            else {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build());
                responseObserver.onError(new Exception("Could not deserialize data"));
            }
        }


        @Override
        public void update(Data request, StreamObserver<Result> responseObserver) {
            if (checkTransactionExists(request.getTransactionId(), responseObserver)) return;

            Connection conn = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                updateDBDataOnRepo(responseObserver, d, conn);
            }
            else {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build());
                responseObserver.onError(new Exception("Could not deserialize data"));
            }
        }

        private void updateDBDataOnRepo(StreamObserver<Result> responseObserver, DBData d, Connection conn) {
            try {
                String result = "done";
                if (d instanceof DBDeleteData)
                    repo.remove(conn, (DBDeleteData) d);
                else if (d instanceof DBWriteData)
                    repo.update(conn, (DBWriteData) d);
                else if(d instanceof DBInsertData)
                    repo.insert(conn, (DBInsertData) d);
                else
                    result = repo.get(conn, d);
                responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(result).build());
                responseObserver.onCompleted();
            }
            catch (SQLException ex) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not perform update: " + ex.getMessage()).build());
                responseObserver.onError(new Exception("Could not perform update: " + ex.getMessage()));
            }
        }

        private boolean checkTransactionExists(String request, StreamObserver<Result> responseObserver) {
            if (!transactions.containsKey(request)) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("no transaction with this id").build());
                responseObserver.onCompleted();
                return true;
            }
            return false;
        }


        @Override
        public void commitTransaction(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            if (checkTransactionExists(transactionId.getId(), responseObserver)) return;

            Connection conn = transactions.get(transactionId.getId());
            try {
                repo.release(conn);
                transactions.remove(transactionId.getId());
                log.warn("{}, Transaciton commited", transactionId.getId());
                responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("released").build());
                responseObserver.onCompleted();
            } catch (SQLException e) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not release the locks").build());
                responseObserver.onError(e);
            }
        }



//
//        @Override
//        public void get(Key request, StreamObserver<Result> responseObserver) {
////            try {
////                String value = repo.find(request.getKey());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(value).build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void delete(Key request, StreamObserver<Result> responseObserver) {
////            try {
////                repo.delete(request.getKey());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("deleted").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void clear(StreamObserver<Result> responseObserver) {
////            try {
////                repo.clear();
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("cleared").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException | IOException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void batch(Values request, StreamObserver<Result> responseObserver) {
//////            if (repo.isChangingMemory())
//////                responseObserver.onError(new RocksDBException("Changing memory"));
////            try {
//////                repo.saveBatchWithGRPCInterrupt(request.getValuesMap());
////                repo.saveBatch(request.getValuesMap());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("saved the batch").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//////            catch (InterruptedException e) {
//////                responseObserver.onError(e);
//////            }
//
//        }
//
//
//        @Override
//        public void changeMemory(Size size, StreamObserver<Result> responseObserver) {
////            try {
////                repo.changeMemorySize(size.getValue());
////                log.info("Memory changed to  {}", size.getValue());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("Changed the memory size to " + size.getValue()).build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException | IOException e) {
////                responseObserver.onError(e);
////            }
//        }


    }
}
