#!/usr/bin/env python

import argparse
import datetime
import grpc
import itertools
import logging
import time
import uuid

import pravega.grpc_gateway as pravega


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--gateway', default='localhost:54672')
    parser.add_argument('--scope', default='examples')
    parser.add_argument('--num_events', default=2, type=int)
    parser.add_argument('--no_create_scope', dest='create_scope', action='store_false')
    args = parser.parse_args()
    logging.info('args=%s' % str(args))

    with grpc.insecure_channel(args.gateway, options=[('grpc.max_receive_message_length', 9*1024*1024)]) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        if args.create_scope:
            logging.info('-------- Create scope --------')
            request = pravega.pb.CreateScopeRequest(scope=args.scope)
            logging.info('CreateScope request=%s' % request)
            response = pravega_client.CreateScope(request)
            logging.info('CreateScope response=%s' % response)

        logging.info('-------- Create stream --------')
        stream = 'test-%s' % str(uuid.uuid4())
        request = pravega.pb.CreateStreamRequest(
            scope=args.scope,
            stream=stream,
            scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=1),
        )
        logging.info('CreateStream request=%s' % request)
        response = pravega_client.CreateStream(request)
        logging.info('CreateStream response=%s' % response)

        logging.info('-------- List streams --------')
        list_streams_request = pravega.pb.ListStreamsRequest(
            scope=args.scope,
        )
        logging.info('ListStreams request=%s', list_streams_request)
        list_streams_response = list(pravega_client.ListStreams(list_streams_request))
        logging.info('ListStreams response=%s', list_streams_response)
        logging.info('len(list_streams_response)=%d', len(list_streams_response))
        assert len(list_streams_response) > 0

        logging.info('-------- Update stream --------')
        request = pravega.pb.UpdateStreamRequest(
            scope=args.scope,
            stream=stream,
            scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=1),
        )
        logging.info('UpdateStream request=%s' % request)
        response = pravega_client.UpdateStream(request)
        logging.info('UpdateStream response=%s' % response)

        logging.info('-------- Write events --------')
        events_to_write = [pravega.pb.WriteEventsRequest(
                scope=args.scope,
                stream=stream,
                event=('%d,%s' % (i, datetime.datetime.now())).encode(encoding='UTF-8'),
                routing_key='0',
            ) for i in range(args.num_events)]
        logging.info("events_to_write=%s", events_to_write)
        write_response = pravega_client.WriteEvents(iter(events_to_write))
        logging.info("WriteEvents response=" + str(write_response))

        logging.info('-------- Get stream info --------')
        stream_info = pravega_client.GetStreamInfo(pravega.pb.GetStreamInfoRequest(scope=args.scope, stream=stream))
        logging.info('GetStreamInfo response=%s' % stream_info)

        logging.info('-------- Read events without stream cuts --------')
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=args.scope,
            stream=stream,
        )
        logging.info('ReadEvents request=%s', read_events_request)
        read_events_response = list(
            itertools.islice(
                pravega_client.ReadEvents(read_events_request),
                args.num_events))
        logging.info('ReadEvents response=%s', read_events_response)
        logging.info('len(read_events_response)=%d', len(read_events_response))
        assert len(read_events_response) == args.num_events

        logging.info('-------- Read events with stream cuts --------')
        read_events_request = pravega.pb.ReadEventsRequest(
            scope=args.scope,
            stream=stream,
            from_stream_cut=stream_info.head_stream_cut,
            to_stream_cut=stream_info.tail_stream_cut,
        )
        logging.info('ReadEvents request=%s', read_events_request)
        read_events_response = list(
            pravega_client.ReadEvents(read_events_request))
        logging.info('ReadEvents response=%s', read_events_response)
        logging.info('len(read_events_response)=%d', len(read_events_response))
        assert len(read_events_response) == args.num_events

        logging.info('-------- Fetch single event from event pointer --------')
        t0 = time.time()
        for read_event in read_events_response:
            event_pointer = read_event.event_pointer
            fetch_event_request = pravega.pb.FetchEventRequest(
                scope=args.scope,
                stream=stream,
                event_pointer=event_pointer,
            )
            logging.info('FetchEvent request=%s', fetch_event_request)
            fetch_event_response = pravega_client.FetchEvent(fetch_event_request)
            logging.info('FetchEvent response=%s', fetch_event_response)
        fetch_event_sec = time.time() - t0
        fetch_event_sec_per_call = fetch_event_sec / len(read_events_response)
        logging.info('fetch_event_sec_per_call=%f' % fetch_event_sec_per_call)

        logging.info('-------- Batch read events --------')
        batch_read_events_request = pravega.pb.BatchReadEventsRequest(
            scope=args.scope,
            stream=stream,
            from_stream_cut=stream_info.head_stream_cut,
            to_stream_cut=stream_info.tail_stream_cut,
        )
        logging.info('BatchReadEvents request=%s', read_events_request)
        batch_read_events_response = list(pravega_client.BatchReadEvents(batch_read_events_request))
        logging.info('BatchReadEvents response=%s' % batch_read_events_response)
        logging.info('len(batch_read_events_response)=%d', len(batch_read_events_response))
        assert len(batch_read_events_response) == args.num_events

        logging.info('-------- Truncate stream --------')
        request = pravega.pb.TruncateStreamRequest(
            scope=args.scope,
            stream=stream,
            stream_cut=stream_info.tail_stream_cut,
        )
        logging.info('TruncateStream request=%s' % request)
        response = pravega_client.TruncateStream(request)
        logging.info('TruncateStream response=%s' % response)

        logging.info('-------- Delete stream --------')
        request = pravega.pb.DeleteStreamRequest(
            scope=args.scope,
            stream=stream,
        )
        logging.info('DeleteStream request=%s' % request)
        response = pravega_client.DeleteStream(request)
        logging.info('DeleteStream response=%s' % response)

        logging.info('-------- Done --------')


if __name__ == '__main__':
    main()
