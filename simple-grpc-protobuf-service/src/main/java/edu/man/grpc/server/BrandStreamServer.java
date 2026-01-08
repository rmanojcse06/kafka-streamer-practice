package edu.man.grpc.server;

import edu.man.proto.grpc.generated.BrandStreamRequest;
import edu.man.proto.grpc.generated.BrandStreamResponse;
import edu.man.proto.grpc.generated.BrandStreamServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class BrandStreamServer {
    public static void main(String[] args) {
        log.info("Starting BrandServer to test gRPC Streaming application on port 15051");
        try {
            Server gRPCServer = ServerBuilder.forPort(15051).addService(new BrandStreamImpl()).build();
            gRPCServer.start();
            gRPCServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            log.info("Brandserver stopped due to unhandled exception {}",e.getMessage());
            throw new RuntimeException(e);
        }
    }


    static class BrandStreamImpl extends BrandStreamServiceGrpc.BrandStreamServiceImplBase {
        @Override
        public StreamObserver<BrandStreamRequest> processBrands(StreamObserver<BrandStreamResponse> responseObserver) {
            return new StreamObserver<BrandStreamRequest>() {
                @Override
                public void onNext(BrandStreamRequest request) {
                    log.info("Received from client: {}", request.getName());
                    BrandStreamResponse response = BrandStreamResponse.newBuilder()
                            .setMessage("Processed Brand: " + request.getName().toUpperCase())
                            .setCurrentTime(System.currentTimeMillis())
                            .build();
                    responseObserver.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Stream error occurred in server: ", t);
                }

                @Override
                public void onCompleted() {
                    log.info("Client stream completed successfully.");
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
