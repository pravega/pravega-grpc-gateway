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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class CachedFetchEventReader implements Closeable {
    private final ReaderGroupManager readerGroupManager;
    private final String readerGroup;
    private final EventStreamClientFactory clientFactory;
    private final EventStreamReader<ByteBuffer> reader;

    public CachedFetchEventReader(ReaderGroupManager readerGroupManager, String readerGroup, EventStreamClientFactory clientFactory, EventStreamReader<ByteBuffer> reader) {
        this.readerGroupManager = readerGroupManager;
        this.readerGroup = readerGroup;
        this.clientFactory = clientFactory;
        this.reader = reader;
    }

    public EventStreamReader<ByteBuffer> getReader() {
        return reader;
    }

    @Override
    public void close() {
        reader.close();
        clientFactory.close();
        readerGroupManager.deleteReaderGroup(readerGroup);
        readerGroupManager.close();
    }
}
