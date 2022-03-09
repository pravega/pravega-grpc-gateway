/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
