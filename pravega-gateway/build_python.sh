#!/usr/bin/env bash
set -e

export PATH=$PWD/env/bin:$PATH

python -m grpc_tools.protoc \
-Isrc/main/proto/ \
--python_out=src/main/python \
--grpc_python_out=src/main/python \
src/main/proto/pravega/pravega.proto
