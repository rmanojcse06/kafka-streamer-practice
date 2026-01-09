package edu.man.udp.client;

import edu.man.proto.grpc.generated.BrandStreamRequest;
import edu.man.proto.grpc.generated.BrandStreamResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.Random;

@Slf4j
public class BrandStreamUdpClient {
    public static int BUFFER_SIZE = 1024;
    public int port;
    public BrandStreamUdpClient(int port){
        this.port = port;
    }
    public void streamBrands() {
        boolean startLooping = true;
        List<String> brandPool = List.of("Nike", "Adidas", "Puma", "Reebok", "Under Armour", "Asics", "New Balance");
        Random random = new Random();
        try (DatagramSocket socket = new DatagramSocket()) {
            InetAddress address = InetAddress.getByName("localhost");
            log.info("Starting UDP Stream to {}:{}", address.getHostAddress(), this.port);
            int i = 0;
            while (startLooping) {
                ++i;
                String randomBrand = brandPool.get(random.nextInt(brandPool.size()));
                log.info("Streaming brand to server: {}", randomBrand);
                Thread.sleep(500); // Simulate delay

                // 1. Build the Protobuf Request
                BrandStreamRequest request = BrandStreamRequest.newBuilder()
                        .setName(randomBrand)
                        .build();

                // 2. Serialize to bytes and wrap in a DatagramPacket
                byte[] buffer = request.toByteArray();
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, this.port);

                log.info("Streaming packet #{} | Brand: {}", i + 1, randomBrand);
                socket.send(packet);

                // 3. Non-blocking/Timeout-based receive for acknowledgement
                byte[] responseBuffer = new byte[BUFFER_SIZE];
                DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length);
                socket.setSoTimeout(500); // Wait 500ms for an ACK

                try {
                    socket.receive(responsePacket);
                    // Decode only the received bytes
                    byte[] receivedData = new byte[responsePacket.getLength()];
                    System.arraycopy(responsePacket.getData(), 0, receivedData, 0, responsePacket.getLength());

                    BrandStreamResponse response = BrandStreamResponse.parseFrom(receivedData);
                    log.info("  Server ACK: {}", response.getMessage());
                } catch (java.net.SocketTimeoutException e) {
                    log.warn("  Packet #{} ACK timeout (UDP loss simulated)", i + 1);
                }

                // Interval between stream events
                Thread.sleep(500); // Simulate delay
            }
        } catch (IOException | InterruptedException e) {
            log.error("UDP Client error", e);
        }
        log.info("BrandStreamUdpClient have been stopped streaming brands");
    }


    public static void main(String[] args) throws InterruptedException {
        // Brief delay to ensure server is bound to port
        Thread.sleep(1000);
        // Start Client streaming logic
        new BrandStreamUdpClient(15053).streamBrands();
    }
}

