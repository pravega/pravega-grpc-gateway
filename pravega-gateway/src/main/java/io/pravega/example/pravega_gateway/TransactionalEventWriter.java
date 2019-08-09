package io.pravega.example.pravega_gateway;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;

import java.util.logging.Logger;

public class TransactionalEventWriter<T> extends AbstractEventWriter<T>  {
    private static final Logger logger = Logger.getLogger(TransactionalEventWriter.class.getName());

    private final EventStreamWriter<T> pravegaWriter;

    /**
     * The currently running transaction to which we write
     */
    private Transaction<T> currentTxn = null;

    public TransactionalEventWriter(EventStreamWriter<T> pravegaWriter) {
        this.pravegaWriter = pravegaWriter;
    }

    @Override
    void writeEvent(String routingKey, T event) throws TxnFailedException {
        if (currentTxn == null) {
            currentTxn = pravegaWriter.beginTxn();
            logger.info("writeEvent: began transaction " + currentTxn.getTxnId());
        }
        currentTxn.writeEvent(routingKey, event);
    }

    @Override
    void commit() throws TxnFailedException {
        if (currentTxn != null) {
            logger.info("commit: committing transaction " + currentTxn.getTxnId());
            currentTxn.commit();
            currentTxn = null;
        }
    }

    @Override
    void abort() {
        if (currentTxn != null) {
            logger.info("abort: aborting transaction " + currentTxn.getTxnId());
            currentTxn.abort();
            currentTxn = null;
        }
    }

    @Override
    void close() throws TxnFailedException {
        abort();
        pravegaWriter.close();
    }
}
