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
