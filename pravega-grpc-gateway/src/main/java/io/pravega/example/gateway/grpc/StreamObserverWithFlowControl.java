package io.pravega.example.gateway.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * A wrapper class that enables flow control for sending responses to clients.
 * This avoids excessive buffering on the server, leading to OOM.
 */
public class StreamObserverWithFlowControl<T> implements StreamObserver<T> {
    final private ServerCallStreamObserver<T> responseObserver;

    public StreamObserverWithFlowControl(StreamObserver<T> responseObserver) {
        this.responseObserver = (ServerCallStreamObserver<T>)  responseObserver;
    }

    @Override
    public void onNext(T value) {
        // TODO: Use onReadyHandler to avoid polling
        try {
            while (!this.responseObserver.isReady()) {
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
        }
        responseObserver.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
        responseObserver.onError(t);
    }

    @Override
    public void onCompleted() {
        responseObserver.onCompleted();
    }
}
