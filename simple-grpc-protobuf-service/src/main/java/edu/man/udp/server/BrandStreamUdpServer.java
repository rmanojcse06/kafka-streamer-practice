package edu.man.udp.server;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.man.grpc.server.BrandStreamGrpcServer;
import edu.man.proto.grpc.generated.BrandStreamRequest;
import edu.man.proto.grpc.generated.BrandStreamResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

@Slf4j
public class BrandStreamUdpServer extends Thread{

    private int port;
    private static volatile boolean canStartServer = false;
    public static final int BUFFER_SIZE = 1024;
    public BrandStreamUdpServer(int port){
        super("brand-stream-udp-server-"+port);
        this.port = port;
        this.canStartServer=false;
    }
    public void run(){
        if (this.canStartServer){
            try (DatagramSocket socket = new DatagramSocket(this.port)) {
                log.info("Brand Stream UDP server started by {} is listening in {}",getName(),this.port);
                byte[] buffer = new byte[BUFFER_SIZE];
                while (this.canStartServer) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    // receive() blocks until a packet arrives
                    socket.receive(packet);

                    // 1. Extract and deserialize the Request from the UDP packet bytes
                    // It is important to only use the actual length received (packet.getLength())
                    byte[] data = new byte[packet.getLength()];
                    System.arraycopy(packet.getData(), 0, data, 0, packet.getLength());

                    BrandStreamRequest request = BrandStreamRequest.parseFrom(data);
                    log.info("UDP Stream received: {}", request.getName());

                    // 2. Prepare a Response
                    BrandStreamResponse response = BrandStreamResponse.newBuilder()
                            .setMessage("Stream ACK for: " + request.getName())
                            .setCurrentTime(System.currentTimeMillis())
                            .build();

                    // 3. Serialize and send response back to the client's return address/port
                    byte[] responseBytes = response.toByteArray();
                    DatagramPacket responsePacket = new DatagramPacket(
                            responseBytes,
                            responseBytes.length,
                            packet.getAddress(),
                            packet.getPort()
                    );
                    socket.send(responsePacket);

                }

                if(!this.canStartServer){
                    log.info("Stopping {} UDP server thread",this.getName());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void startServer(){
        this.canStartServer = true;
        this.start();
    }

    public void stopServer(){
        this.canStartServer = false;
        try {
            Thread.sleep(5);
            this.interrupt();
        } catch (Throwable e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        log.info("Starting BrandServer to test UDP Streaming application on port 15053");
        try {
            BrandStreamUdpServer server = new BrandStreamUdpServer(15053);
            server.startServer();
        } catch (Exception e) {
            log.info("Brandserver stopped due to unhandled exception {}",e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
