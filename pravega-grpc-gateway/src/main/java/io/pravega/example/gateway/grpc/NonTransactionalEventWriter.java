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

import io.pravega.client.stream.EventStreamWriter;

public class NonTransactionalEventWriter<T> extends AbstractEventWriter<T>  {
    private final EventStreamWriter<T> pravegaWriter;

    public NonTransactionalEventWriter(EventStreamWriter<T> pravegaWriter) {
        this.pravegaWriter = pravegaWriter;
    }

    @Override
    void writeEvent(String routingKey, T event) {
        pravegaWriter.writeEvent(routingKey, event);
    }

    @Override
    void close() {
        pravegaWriter.close();
    }
}
