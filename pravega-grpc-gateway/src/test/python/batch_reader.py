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
import grpc
import base64
import gzip
import argparse

import pravega.grpc_gateway as pravega


def decode_stream_cut_text(text):
    """Based on StreamCutImpl.java"""
    plaintext = gzip.decompress(base64.b64decode(text)).decode('utf-8')
    split = plaintext.split(':', 5)
    stream = split[1]
    segment_numbers = [int(s) for s in split[2].split(',')]
    epochs = [int(s) for s in split[3].split(',')]
    offsets = [int(s) for s in split[4].split(',')]
    zipped = list(zip(zip(segment_numbers, epochs), offsets))
    positions = dict(zipped)
    return {
        'plaintext': plaintext,
        'stream': stream,
        'positions': positions,   # map from (segment_number, epoch) to offset
    }


def encode_stream_cut_text(stream_cut):
    """Based on StreamCutImpl.java"""
    zipped = list(stream_cut['positions'].items())
    logging.info(zipped)
    segment_numbers = [str(z[0][0]) for z in zipped]
    epochs = [str(z[0][1]) for z in zipped]
    offsets = [str(z[1]) for z in zipped]
    split = [
        str(0),
        stream_cut['stream'],
        ','.join(segment_numbers),
        ','.join(epochs),
        ','.join(offsets),
    ]
    plaintext = ':'.join(split)
    text = base64.b64encode(gzip.compress(plaintext.encode('utf-8')))
    return text


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--from_head_streamcut', action='store_true')
    parser.add_argument('--gateway', default='localhost:54672')
    parser.add_argument('--scope', default='examples')
    parser.add_argument('--stream', default='stream2')
    parser.add_argument('--to_tail_streamcut', action='store_true')
    args = parser.parse_args()
    logging.info('args=%s' % str(args))

    with grpc.insecure_channel(args.gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        stream_info = pravega_client.GetStreamInfo(pravega.pb.GetStreamInfoRequest(scope=args.scope, stream=args.stream))
        logging.info('GetStreamInfo response=%s' % stream_info)

        if args.from_head_streamcut:
            decoded = decode_stream_cut_text(stream_info.tail_stream_cut.text)
            logging.info(decoded)
            encoded = encode_stream_cut_text(decoded)
            logging.info(encoded)
            from_stream_cut_decoded = decode_stream_cut_text(stream_info.head_stream_cut.text)
            logging.info(from_stream_cut_decoded)
            # from_stream_cut_decoded['positions'][(0,0)] += 1
            from_stream_cut_encoded = encode_stream_cut_text(from_stream_cut_decoded)
            logging.info(from_stream_cut_encoded)
            from_stream_cut = pravega.pb.StreamCut(text=from_stream_cut_encoded)
        else:
            from_stream_cut = None

        if args.to_tail_streamcut:
            to_stream_cut = stream_info.tail_stream_cut
        else:
            to_stream_cut = None

        read_events_request = pravega.pb.BatchReadEventsRequest(
            scope=args.scope,
            stream=args.stream,
            from_stream_cut=from_stream_cut,
            to_stream_cut=to_stream_cut,
        )
        logging.info('read_events_request=%s', read_events_request)
        for r in pravega_client.BatchReadEvents(read_events_request):
            logging.info('BatchReadEvents: response=%s' % str(r))


if __name__ == '__main__':
    main()
