#!/usr/bin/env python

import logging
import grpc
import pravega
import base64
import gzip


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
        # 'text': text,
        # 'split': split,
        'plaintext': plaintext,
        'stream': stream,
        # 'segment_numbers': segment_numbers,
        # 'epochs': epochs,
        # 'offsets': offsets,
        # 'zipped': zipped,
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


def run():
    with grpc.insecure_channel('localhost:54672') as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)

        scope = 'examples'
        stream = 'stream2'

        response = pravega_client.GetStreamInfo(pravega.pb.GetStreamInfoRequest(scope=scope, stream=stream))
        logging.info('GetStreamInfo response=%s' % response)
        stream_info = response

        decoded = decode_stream_cut_text(stream_info.tail_stream_cut.text)
        logging.info(decoded)
        encoded = encode_stream_cut_text(decoded)
        logging.info(encoded)

        from_stream_cut_decoded = decode_stream_cut_text(stream_info.head_stream_cut.text)
        logging.info(from_stream_cut_decoded)
        # from_stream_cut_decoded['positions'][(0,0)] += 1
        from_stream_cut = encode_stream_cut_text(from_stream_cut_decoded)
        logging.info(from_stream_cut)

        read_events_request = pravega.pb.BatchReadEventsRequest(
            scope=scope,
            stream=stream,
            from_stream_cut=pravega.pb.StreamCut(text=from_stream_cut),
            to_stream_cut=stream_info.tail_stream_cut,
        )
        logging.info('read_events_request=%s', read_events_request)
        for r in pravega_client.BatchReadEvents(read_events_request):
            logging.info('ReadEvents: response=%s' % str(r))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
