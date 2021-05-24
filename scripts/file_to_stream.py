#!/usr/bin/env python3
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import logging
import struct
import grpc
import pravega.grpc_gateway as pravega


def main():
    parser = argparse.ArgumentParser(
        description=
            'Copy events directly from a Pravega segment as stored in a file on a tier 2 '
            'to a Pravega stream through the Pravega GRPC Gateway. '
            'These files are named like "0.#epoch.0$offset.0".'
    )
    parser.add_argument('--delete_stream', dest='delete_stream', action='store_true')
    parser.add_argument('--gateway', default='localhost:54672')
    parser.add_argument('--log_level', type=int, default=logging.INFO, help='10=DEBUG,20=INFO')
    parser.add_argument('--max_events', type=int, default=0)
    parser.add_argument('--no_create_scope', dest='create_scope', action='store_false')
    parser.add_argument('--no_create_stream', dest='create_stream', action='store_false')
    parser.add_argument('--print_size', type=int, default=100, help='Print this many bytes in each event')
    parser.add_argument('--routing_key', default='')
    parser.add_argument('--scope', default='examples')
    parser.add_argument('--stream', required=True)
    parser.add_argument('file', type=str)
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    logging.info('args=%s' % str(args))

    with grpc.insecure_channel(args.gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        if args.create_scope:
            request = pravega.pb.CreateScopeRequest(scope=args.scope)
            logging.info('CreateScope request=%s' % request)
            response = pravega_client.CreateScope(request)
            logging.info('CreateScope response=%s' % response)

        if args.delete_stream:
            request = pravega.pb.DeleteStreamRequest(
                scope=args.scope,
                stream=args.stream,
            )
            logging.info('DeleteStream request=%s' % request)
            response = pravega_client.DeleteStream(request)
            logging.info('DeleteStream response=%s' % response)

        if args.create_stream:
            request = pravega.pb.CreateStreamRequest(
                scope=args.scope,
                stream=args.stream,
                scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=1),
            )
            logging.info('CreateStream request=%s' % request)
            response = pravega_client.CreateStream(request)
            logging.info('CreateStream response=%s' % response)

        with open(args.file, 'rb') as f:
            total_records = 0
            total_bytes = 0

            def events_to_write_generator():
                nonlocal total_records
                nonlocal total_bytes
                while True:
                    try:
                        header = f.read(8)
                        if not header:
                            break
                        num_bytes = struct.unpack('>Q', header)[0]  # 64-bit unsigned integer
                        event = f.read(num_bytes)
                        total_records += 1
                        total_bytes += num_bytes
                        logging.debug("num_bytes=%d, event=%s..." % (num_bytes, str(event)[0:args.print_size]))
                        event_to_write = pravega.pb.WriteEventsRequest(
                            scope=args.scope,
                            stream=args.stream,
                            event=event,
                            routing_key=args.routing_key,
                        )
                        yield event_to_write
                        if 0 < args.max_events <= total_records:
                            break
                    except EOFError:
                        break

            write_response = pravega_client.WriteEvents(events_to_write_generator())
            logging.info('WriteEvents response=%s' % write_response)

    logging.info('Processed %0.6f MB in %d records.' % (total_bytes / 1e6, total_records))


if __name__ == "__main__":
    main()
