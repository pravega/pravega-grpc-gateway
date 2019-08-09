package io.pravega.example.pravega_gateway;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code PravegaGateway} server.
 */
public class PravegaGateway {
    private static final Logger logger = Logger.getLogger(PravegaGateway.class.getName());

    private Server server;

    private void start() throws IOException {
        int port = Parameters.getListenPort();
        server = NettyServerBuilder.forPort(port)
                .permitKeepAliveTime(1, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .addService(new PravegaServerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        logger.info("Pravega controller is " + Parameters.getControllerURI());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                PravegaGateway.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final PravegaGateway server = new PravegaGateway();
        server.start();
        server.blockUntilShutdown();
    }

}
