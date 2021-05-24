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

import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;

import java.util.logging.Logger;

public class TransactionalEventWriter<T> extends AbstractEventWriter<T>  {
    private static final Logger logger = Logger.getLogger(TransactionalEventWriter.class.getName());

    private final TransactionalEventStreamWriter<T> pravegaWriter;

    /**
     * The currently running transaction to which we write
     */
    private Transaction<T> currentTxn = null;

    public TransactionalEventWriter(TransactionalEventStreamWriter<T> pravegaWriter) {
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
    void close() {
        abort();
        pravegaWriter.close();
    }
}
