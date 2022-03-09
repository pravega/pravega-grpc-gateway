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

import io.pravega.client.stream.TxnFailedException;

abstract class AbstractEventWriter<T> {
    void open() {
    }

    abstract void writeEvent(String routingKey, T event) throws TxnFailedException;

    void commit() throws TxnFailedException {
    }

    void abort() {
    }

    void close() throws TxnFailedException {
    }
}
