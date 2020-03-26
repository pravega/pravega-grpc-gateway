#!/usr/bin/env python3

import argparse
import logging
import struct
import grpc
import pravega.grpc_gateway as pravega


def ignore_non_events(read_events):
    for read_event in read_events:
        if len(read_event.event) > 0:
            yield read_event


def main():
    parser = argparse.ArgumentParser(
        description=
            'Copy events from a Pravega Stream through the Pravega GRPC Gateway to a file. '
            'The output file is in the same format as a Pravega segment as stored in a file on tier 2.'
    )
    parser.add_argument('--gateway', default='localhost:54672')
    parser.add_argument('--log_level', type=int, default=logging.INFO, help='10=DEBUG,20=INFO')
    parser.add_argument('--max_events', type=int, default=0)
    parser.add_argument('--print_size', type=int, default=100, help='Print this many bytes in each event')
    parser.add_argument('--scope', default='examples')
    parser.add_argument('--stream', required=True)
    parser.add_argument('file', type=str)
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    logging.info('args=%s' % str(args))

    with grpc.insecure_channel(args.gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        stream_info = pravega_client.GetStreamInfo(
            pravega.pb.GetStreamInfoRequest(scope=args.scope, stream=args.stream))
        logging.info('stream_info=%s' % str(stream_info))
        stream_size_MB = (stream_info.tail_stream_cut.cut[0] - stream_info.head_stream_cut.cut[0]) * 1e-6
        logging.info('stream_size_MB=%f' % stream_size_MB)

        from_stream_cut = stream_info.head_stream_cut
        to_stream_cut = stream_info.tail_stream_cut

        read_events_request = pravega.pb.ReadEventsRequest(
            scope=args.scope,
            stream=args.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )

        with open(args.file, 'wb') as f:
            total_records = 0
            total_bytes = 0
            read_events = ignore_non_events(pravega_client.ReadEvents(read_events_request))
            for read_event in read_events:
                total_records += 1
                num_bytes = len(read_event.event)
                total_bytes += num_bytes
                logging.debug("num_bytes=%d, event_pointer=%s" % (num_bytes, str(read_event.event_pointer.description)))
                logging.debug("event=%s..." % str(read_event.event)[0:args.print_size])
                header = struct.pack('>Q', num_bytes)   # 64-bit unsigned integer
                f.write(header)
                f.write(read_event.event)
                if 0 < args.max_events <= total_records:
                    break

    logging.info('Processed %0.6f MB in %d records.' % (total_bytes / 1e6, total_records))


if __name__ == "__main__":
    main()
