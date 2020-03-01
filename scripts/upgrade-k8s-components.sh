#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..

helm upgrade \
    pravega-grpc-gateway \
    ${ROOT_DIR}/charts/pravega-grpc-gateway
