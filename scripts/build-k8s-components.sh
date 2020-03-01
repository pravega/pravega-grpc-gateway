#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..

docker build -f ${ROOT_DIR}/pravega-grpc-gateway/Dockerfile ${ROOT_DIR} --tag ${DOCKER_REPOSITORY}/pravega-grpc-gateway:${IMAGE_TAG}

docker push ${DOCKER_REPOSITORY}/pravega-grpc-gateway:${IMAGE_TAG}
