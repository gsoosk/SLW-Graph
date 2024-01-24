package edgelab.retryFreeDB;

import edgelab.proto.Data;
import edgelab.proto.Empty;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import edgelab.retryFreeDB.repo.storage.Postgres;
import edgelab.retryFreeDB.repo.storage.Storage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.filefilter.FalseFileFilter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RetryFreeDBServer {
    private final Server server;
    private final int port;
    public RetryFreeDBServer(int port, ServerBuilder<?> serverBuilder, String postgresPort) {
        this.server = serverBuilder
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .addService(new RetryFreeDBService(postgresPort))
                .build();
        this.port = port;
    }


    @Slf4j
    public static class RetryFreeDBService extends RetryFreeDBServerGrpc.RetryFreeDBServerImplBase {
        private final Postgres repo;
        Map<String, Connection> transactions;
        private long lastTransactionId;
        public RetryFreeDBService(String postgresPort) {
            repo = new Postgres(postgresPort);
            transactions = new HashMap<>();
            lastTransactionId = 0;
        }

        @Override
        public void beginTransaction(Empty request, StreamObserver<Result> responseObserver) {
            try (Connection conn = repo.connect()) {
                transactions.put(Long.toString(lastTransactionId), conn);
                responseObserver.onNext(Result.newBuilder()
                                        .setStatus(true)
                                        .setMessage(Long.toString(lastTransactionId)).build());
                responseObserver.onCompleted();
                lastTransactionId++;
            }
            catch (SQLException ex) {
                log.info("db error: couldn't connect/commit,  {}", ex.getMessage());
                responseObserver.onError(ex);
            }
        }

        @Override
        public void update(Data request, StreamObserver<Result> responseObserver) {
            Connection conn = transactions.get(request.getTransactionId());
            DBData d = DBTransaction.deserializeData(request);
            if (d != null) {
                try {
                    repo.lock(conn, d);
                } catch (SQLException e) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not lock").build());
                    responseObserver.onError(new Exception("Could not lock"));
                    return;
                }
                try {
                    String result = "done";
                    if (d instanceof DBDeleteData)
                        repo.remove(conn, (DBDeleteData) d);
                    else if (d instanceof DBWriteData)
                        repo.update(conn, (DBWriteData) d);
                    else
                        result = repo.get(conn, d);
                    responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(result).build());
                    responseObserver.onCompleted();
                }
                catch (SQLException ex) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not perform update").build());
                    responseObserver.onError(new Exception("Could not perform update"));
                }
            }
            else {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build());
                responseObserver.onError(new Exception("Could not deserialize data"));
            }
        }


        @Override
        public void commitTransaction(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            if (!transactions.containsKey(transactionId.getId())) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("no transaction with this id").build());
                responseObserver.onCompleted();
                return;
            }

            Connection conn = transactions.get(transactionId.getId());
            try {
                repo.release(conn);
                transactions.remove(transactionId.getId());
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
