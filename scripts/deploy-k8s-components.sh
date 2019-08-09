#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..
NAMESPACE=examples

helm upgrade --install \
pravega-gateway \
--namespace ${NAMESPACE} \
${ROOT_DIR}/charts/pravega-gateway \
--set image.repository=${DOCKER_REPOSITORY}/pravega-gateway \
--set image.tag=${IMAGE_TAG}
