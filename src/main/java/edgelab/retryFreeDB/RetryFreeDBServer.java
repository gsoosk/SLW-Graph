package edgelab.retryFreeDB;

import edgelab.proto.Data;
import edgelab.proto.Transaction;
import edgelab.proto.Result;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.repo.storage.Storage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RetryFreeDBServer {
    private final Server server;
    private final int port;
    public RetryFreeDBServer(int port, ServerBuilder<?> serverBuilder) {
        this.server = serverBuilder
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .addService(new RetryFreeDBService())
                .build();
        this.port = port;

    }


    @Slf4j
    public static class RetryFreeDBService extends RetryFreeDBServerGrpc.RetryFreeDBServerImplBase {
        Map<Key, Value>
        public RetryFreeDBService() {

        }


        @Override
        public void update(Transaction request, StreamObserver<Result> observer) {
            List<Data> readWriteList = request.getReadWriteList();

            // Lock

            // Perform

            // Unlock


//            try {
//                repo.save(request.getKey(), request.getValue());
//                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("saved").build());
//                responseObserver.onCompleted();
//            } catch (RocksDBException e) {
//                responseObserver.onError(e);
//            }
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
