package io.pravega.example.pravega_gateway;

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
