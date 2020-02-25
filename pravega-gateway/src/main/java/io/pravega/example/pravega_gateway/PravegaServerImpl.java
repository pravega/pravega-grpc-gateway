package io.pravega.example.pravega_gateway;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

class PravegaServerImpl extends PravegaGatewayGrpc.PravegaGatewayImplBase {
    private static final Logger logger = Logger.getLogger(PravegaGateway.class.getName());
    static final long DEFAULT_TIMEOUT_MS = 1000;

    private final ClientConfig clientConfig;

    PravegaServerImpl() {
        clientConfig = ClientConfig.builder().controllerURI(Parameters.getControllerURI()).build();
    }

    @Override
    public void createScope(CreateScopeRequest req, StreamObserver<CreateScopeResponse> responseObserver) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            boolean created = streamManager.createScope(req.getScope());
            responseObserver.onNext(CreateScopeResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createStream(CreateStreamRequest req, StreamObserver<CreateStreamResponse> responseObserver) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(toPravegaScalingPolicy(req.getScalingPolicy()))
                    .retentionPolicy(toPravegaRetentionPolicy(req.getRetentionPolicy()))
                    .build();
            boolean created = streamManager.createStream(req.getScope(), req.getStream(), streamConfig);
            responseObserver.onNext(CreateStreamResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void updateStream(UpdateStreamRequest req, StreamObserver<UpdateStreamResponse> responseObserver) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(toPravegaScalingPolicy(req.getScalingPolicy()))
                    .retentionPolicy(toPravegaRetentionPolicy(req.getRetentionPolicy()))
                    .build();
            boolean updated = streamManager.updateStream(req.getScope(), req.getStream(), streamConfig);
            responseObserver.onNext(UpdateStreamResponse.newBuilder().setUpdated(updated).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void readEvents(ReadEventsRequest req, StreamObserver<ReadEventsResponse> responseObserver) {
        final String scope = req.getScope();
        final String streamName = req.getStream();
        final io.pravega.client.stream.StreamCut fromStreamCut =  toPravegaStreamCut(req.getFromStreamCut());
        final io.pravega.client.stream.StreamCut toStreamCut =  toPravegaStreamCut(req.getToStreamCut());
        final boolean haveEndStreamCut = (toStreamCut != io.pravega.client.stream.StreamCut.UNBOUNDED);
        final long timeoutMs = req.getTimeoutMs() == 0 ? DEFAULT_TIMEOUT_MS : req.getTimeoutMs();

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final Stream stream = Stream.of(scope, streamName);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(stream, fromStreamCut, toStreamCut)
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            try {
                final String readerId = UUID.randomUUID().toString();
                try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                     EventStreamReader<ByteBuffer> reader = clientFactory.createReader(
                             readerId,
                             readerGroup,
                             new ByteBufferSerializer(),
                             ReaderConfig.builder().build())) {
                    final StreamCutBuilder streamCutBuilder = new StreamCutBuilder(stream, fromStreamCut);
                    for (; ; ) {
                        try {
                            EventRead<ByteBuffer> event = reader.readNextEvent(timeoutMs);
                            if (event.isCheckpoint()) {
                                final ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                        .setCheckpointName(event.getCheckpointName())
                                        .build();
                                logger.fine("readEvents: response=" + response.toString());
                                responseObserver.onNext(response);
                            } else if (event.getEvent() != null) {
                                final io.pravega.client.stream.Position position = event.getPosition();
                                final io.pravega.client.stream.EventPointer eventPointer = event.getEventPointer();
                                streamCutBuilder.addEvent(position);
                                final io.pravega.client.stream.StreamCut streamCut = streamCutBuilder.getStreamCut();
                                final ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                        .setEvent(ByteString.copyFrom(event.getEvent()))
                                        .setPosition(Position.newBuilder()
                                                .setBytes(ByteString.copyFrom(position.toBytes()))
                                                .setDescription(position.toString())
                                                .build())
                                        .setEventPointer(EventPointer.newBuilder()
                                                .setBytes(ByteString.copyFrom(eventPointer.toBytes()))
                                                .setDescription(eventPointer.toString()))
                                        .setStreamCut(StreamCut.newBuilder()
                                                .setText(streamCut.asText())
                                                .setDescription(streamCut.toString()))
                                        .build();
                                logger.fine("readEvents: response=" + response.toString());
                                responseObserver.onNext(response);
                            } else {
                                if (haveEndStreamCut) {
                                    // If this is a bounded stream with an end stream cut, then we
                                    // have reached the end stream cut.
                                    logger.info("readEvents: no more events, completing RPC");
                                    break;
                                } else {
                                    // If this is an unbounded stream, all events have been read and a
                                    // timeout has occurred.
                                }
                            }

                            if (Context.current().isCancelled()) {
                                logger.warning("context cancelled");
                                responseObserver.onError(Status.CANCELLED.asRuntimeException());
                                return;
                            }
                        } catch (ReinitializationRequiredException e) {
                            // There are certain circumstances where the reader needs to be reinitialized
                            logger.warning(e.toString());
                            responseObserver.onError(e);
                            return;
                        }
                    }
                }
            } finally {
                readerGroupManager.deleteReaderGroup(readerGroup);
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<WriteEventsRequest> writeEvents(StreamObserver<WriteEventsResponse> responseObserver) {
        return new StreamObserver<WriteEventsRequest>() {
            EventStreamClientFactory clientFactory;
            AbstractEventWriter<ByteBuffer> writer;
            String scope;
            String streamName;
            Boolean useTransaction;
            final String writerId = UUID.randomUUID().toString();

            @Override
            public void onNext(WriteEventsRequest req) {
                logger.fine("writeEvents: req=" + req.toString());
                if (writer == null) {
                    scope = req.getScope();
                    streamName = req.getStream();
                    useTransaction = req.getUseTransaction();
                    clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                    if (useTransaction) {
                        TransactionalEventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createTransactionalEventWriter(
                                writerId,
                                streamName,
                                new ByteBufferSerializer(),
                                EventWriterConfig.builder().build());
                        writer = new TransactionalEventWriter<>(pravegaWriter);
                    } else {
                        EventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createEventWriter(
                                streamName,
                                new ByteBufferSerializer(),
                                EventWriterConfig.builder().build());
                        writer = new NonTransactionalEventWriter<>(pravegaWriter);
                    }
                    writer.open();
                } else {
                    // Stream-wide parameters in 2nd and subsequent requests can be the same
                    // as the first request or can be the GRPC default (empty string and 0).
                    if (!(req.getScope().isEmpty() || req.getScope().equals(scope))) {
                        responseObserver.onError(
                                Status.INVALID_ARGUMENT
                                .withDescription(String.format(
                                    "Scope must be the same for all events; received=%s, expected=%s",
                                       req.getScope(), scope))
                                .asRuntimeException());
                        return;
                    }
                    if (!(req.getStream().isEmpty() || req.getStream().equals(streamName))) {
                        responseObserver.onError(
                                Status.INVALID_ARGUMENT
                                        .withDescription(String.format(
                                                "Stream must be the same for all events; received=%s, expected=%s",
                                                req.getStream(), streamName))
                                .asRuntimeException());
                        return;
                    }
                    if (!(req.getUseTransaction() == false || req.getUseTransaction() == useTransaction)) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription(String.format(
                                        "UseTransaction must be the same for all events; received=%d, expected=%d",
                                        req.getUseTransaction(), useTransaction))
                                .asRuntimeException());
                        return;
                    }
                }
                try {
                    writer.writeEvent(req.getRoutingKey(), req.getEvent().asReadOnlyByteBuffer());
                    if (req.getCommit()) {
                        writer.commit();
                    }
                } catch (TxnFailedException e) {
                    responseObserver.onError(Status.ABORTED.asRuntimeException());
                    return;
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.WARNING, "Encountered error in writeEvents", t);
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (TxnFailedException e) {
                        // Ignore error
                    }
                    writer = null;
                }
                if (clientFactory != null) {
                    clientFactory.close();
                    clientFactory = null;
                }
            }

            @Override
            public void onCompleted() {
                if (writer != null) {
                    try {
                        writer.commit();
                        writer.close();
                    } catch (TxnFailedException e) {
                        responseObserver.onError(Status.ABORTED.asRuntimeException());
                        return;
                    }
                    writer = null;
                }
                if (clientFactory != null) {
                    clientFactory.close();
                    clientFactory = null;
                }
                WriteEventsResponse response = WriteEventsResponse.newBuilder()
                        .build();
                logger.fine("writeEvents: response=" + response.toString());
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void getStreamInfo(GetStreamInfoRequest req, StreamObserver<GetStreamInfoResponse> responseObserver) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            final StreamInfo streamInfo = streamManager.getStreamInfo(req.getScope(), req.getStream());
            final GetStreamInfoResponse response = GetStreamInfoResponse.newBuilder()
                    .setHeadStreamCut(toGrpcStreamCut(streamInfo.getHeadStreamCut()))
                    .setTailStreamCut(toGrpcStreamCut(streamInfo.getTailStreamCut()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void batchReadEvents(BatchReadEventsRequest req, StreamObserver<BatchReadEventsResponse> responseObserver) {
        final String scope = req.getScope();
        final String streamName = req.getStream();
        final io.pravega.client.stream.StreamCut fromStreamCut =  toPravegaStreamCut(req.getFromStreamCut());
        final io.pravega.client.stream.StreamCut toStreamCut =  toPravegaStreamCut(req.getToStreamCut());

        try (BatchClientFactory batchClient = BatchClientFactory.withScope(scope, clientConfig)) {
            batchClient.getSegments(Stream.of(scope, streamName), fromStreamCut, toStreamCut).getIterator().forEachRemaining(
                    segmentRange -> {
                        SegmentIterator<ByteBuffer> iterator = batchClient.readSegment(segmentRange, new ByteBufferSerializer());
                        while (iterator.hasNext()) {
                            long offset = iterator.getOffset();
                            ByteBuffer event = iterator.next();
                            BatchReadEventsResponse response = BatchReadEventsResponse.newBuilder()
                                    .setEvent(ByteString.copyFrom(event))
                                    .setSegmentId(segmentRange.getSegmentId())
                                    .setOffset(offset).build();
                            responseObserver.onNext(response);
                        }
                    }
            );
        }

        responseObserver.onCompleted();
    }

    private io.pravega.client.stream.ScalingPolicy toPravegaScalingPolicy(ScalingPolicy grpcScalingPolicy) {
        // TODO: Support other scaling policies.
        final int minNumSegments = Integer.max(1, grpcScalingPolicy.getMinNumSegments());
        return io.pravega.client.stream.ScalingPolicy.fixed(minNumSegments);
    }

    private io.pravega.client.stream.RetentionPolicy toPravegaRetentionPolicy(RetentionPolicy grpcRetentionPolicy) {
        if (grpcRetentionPolicy.getRetentionParam() == 0) {
            return null;
        }
        io.pravega.client.stream.RetentionPolicy.RetentionType retentionType = io.pravega.client.stream.RetentionPolicy.RetentionType.SIZE;
        if (grpcRetentionPolicy.getRetentionType() == RetentionPolicy.RetentionPolicyType.TIME) {
            retentionType = io.pravega.client.stream.RetentionPolicy.RetentionType.TIME;
        }
        return io.pravega.client.stream.RetentionPolicy.builder()
                .retentionType(retentionType)
                .retentionParam(grpcRetentionPolicy.getRetentionParam())
                .build();
    }

    private io.pravega.client.stream.StreamCut toPravegaStreamCut(StreamCut grpcStreamCut) {
        if (grpcStreamCut.getText().isEmpty()) {
            return io.pravega.client.stream.StreamCut.UNBOUNDED;
        } else {
            return io.pravega.client.stream.StreamCut.from(grpcStreamCut.getText());
        }
    }

    private StreamCut toGrpcStreamCut(io.pravega.client.stream.StreamCut streamCut) {
        StreamCut.Builder grpcStreamCutBuilder = StreamCut.newBuilder()
                .setText(streamCut.asText());
        for (Map.Entry<Segment, Long> entry : streamCut.asImpl().getPositions().entrySet()) {
            grpcStreamCutBuilder.putCut(entry.getKey().getSegmentId(), entry.getValue());
        }
        return grpcStreamCutBuilder.build();
    }

}
