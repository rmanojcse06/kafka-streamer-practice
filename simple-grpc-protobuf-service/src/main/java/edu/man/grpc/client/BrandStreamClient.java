package edu.man.grpc.client;

import edu.man.proto.grpc.generated.BrandStreamRequest;
import edu.man.proto.grpc.generated.BrandStreamResponse;
import edu.man.proto.grpc.generated.BrandStreamServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BrandStreamClient {
    public static void main(String[] args) {
        log.info("Starting BrandStreamClient and requesting data from channel {}", 15051);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 15051)
                .usePlaintext()
                .build();
        BrandStreamServiceGrpc.BrandStreamServiceStub asyncStub = BrandStreamServiceGrpc.newStub(channel);
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<BrandStreamRequest> requestObserver = asyncStub.processBrands(new StreamObserver<BrandStreamResponse>() {
            @Override
            public void onNext(BrandStreamResponse response) {
                log.info("Server Response: {} at {}", response.getMessage(), response.getCurrentTime());
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error from server", t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("Server finished responding.");
                latch.countDown();
            }
        });

        // Sending a stream of data
        try {
            boolean startLooping = true;
            List<String> brandPool = List.of("Nike", "Adidas", "Puma", "Reebok", "Under Armour", "Asics", "New Balance");
            Random random = new Random();
            while(startLooping) {
                String randomBrand = brandPool.get(random.nextInt(brandPool.size()));
                log.info("Streaming brand to server: {}", randomBrand);
                requestObserver.onNext(BrandStreamRequest.newBuilder().setName(randomBrand).build());
                Thread.sleep(500); // Simulate delay
            }
        } catch (Exception e) {
            requestObserver.onError(e);
        }

        requestObserver.onCompleted();

        try {
            latch.await(5, TimeUnit.SECONDS);
        }catch (InterruptedException ie){
            ie.printStackTrace();
        }
        channel.shutdown();
    }
}
