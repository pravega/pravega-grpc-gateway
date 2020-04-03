#! /bin/bash
set -x

NAMESPACE=${NAMESPACE:-examples}

helm delete pravega-grpc-gateway \
--namespace ${NAMESPACE}
