#!/usr/bin/env python

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

import logging
import datetime
import time
import grpc
import random
import argparse

import pravega.grpc_gateway as pravega


def events_to_write_generator(args):
    while True:
        event_to_write = pravega.pb.WriteEventsRequest(
            scope=args.scope,
            stream=args.stream,
            use_transaction=args.use_transaction,
            event=str(datetime.datetime.now()).encode(encoding='UTF-8'),
            routing_key=str(random.randint(0, 10)),
        )
        logging.info("event_to_write=%s", event_to_write)
        yield event_to_write
        event_to_write = pravega.pb.WriteEventsRequest(
            commit=args.use_transaction,
            event=str(datetime.datetime.now()).encode(encoding='UTF-8'),
            routing_key=str(random.randint(0, 10)),
        )
        logging.info("event_to_write=%s", event_to_write)
        yield event_to_write
        time.sleep(1)


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--gateway', default='localhost:54672')
    parser.add_argument('--min_num_segments', default=3)
    parser.add_argument('--no_create_scope', dest='create_scope', action='store_false')
    parser.add_argument('--scope', default='examples')
    parser.add_argument('--stream', default='stream2')
    parser.add_argument('--use_transaction', action='store_true')
    args = parser.parse_args()
    logging.info('args=%s' % str(args))

    with grpc.insecure_channel(args.gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        if args.create_scope:
            request = pravega.pb.CreateScopeRequest(scope=args.scope)
            logging.info('CreateScope request=%s' % request)
            response = pravega_client.CreateScope(request)
            logging.info('CreateScope response=%s' % response)

        request = pravega.pb.CreateStreamRequest(
            scope=args.scope,
            stream=args.stream,
            scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=args.min_num_segments),
            retention_policy=pravega.pb.RetentionPolicy(retention_type='TIME', retention_param=2*24*60*60*1000),
        )
        logging.info('CreateStream request=%s' % request)
        response = pravega_client.CreateStream(request)
        logging.info('CreateStream response=%s' % response)

        # response = pravega_client.UpdateStream(pravega.pb.UpdateStreamRequest(
        #     scope=scope,
        #     stream=stream,
        #     scaling_policy=pravega.pb.ScalingPolicy(min_num_segments=3),
        # ))
        # logging.info('UpdateStream response=%s' % response)

        write_response = pravega_client.WriteEvents(events_to_write_generator(args))
        logging.info("write_response=" + str(write_response))


if __name__ == '__main__':
    main()
