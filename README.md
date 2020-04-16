# Pravega GRPC Gateway

This is a sample GRPC server that provides a gateway to Pravega.
It provides **limited** Pravega functionality to any environment that support GRPC, including Python.

Using a GRPC gateway is better than a REST gateway for the following reasons:

- GRPC streaming is used for reading and writing events. This allows the Pravega connection to remain open for the life
  of the streaming request. Within a streaming request, any number of read or write operations can be performed.
  In the case of writing, these can also be wrapped in transactions. 
  Events can be marked to commit the current transaction and open a new one.
  
- GRPC uses Protobuf for efficient serialization.
  REST/JSON requires base64 encoding of binary data which is less efficient.
  
- GRPC stubs (client code) can be easily created for nearly any language and environment.

# Example Usage

## Example Usage with Python

Initialize:
```python
import grpc
import pravega.grpc_gateway as pravega
gateway = 'pravega-grpc-gateway.example.com:80'
pravega_channel = grpc.insecure_channel(gateway, options=[
        ('grpc.max_receive_message_length', 9*1024*1024),
    ])
pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)
```

Read events from a stream:
```python
read_events_request = pravega.pb.ReadEventsRequest(
    scope='examples',
    stream='my-stream',
    from_stream_cut=None,
    to_stream_cut=None,
)
for event in pravega_client.ReadEvents(read_events_request):
    print(event.event.args.decode('UTF-8')
```

Write events to a stream:
```python
events_to_write = (pravega.pb.WriteEventsRequest(
        scope='examples',
        stream='my-stream',
        event=('%d,%s' % (i, datetime.datetime.now())).encode('UTF-8'),
        routing_key='0',
    ) for i in range(10))
pravega_client.WriteEvents(events_to_write)
```

See [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py) for more examples.

See [pravega.proto](pravega/grpc_gateway/pravega.proto) for all available RPCs.

# Run Gateway Locally

```
export PRAVEGA_CONTROLLER=tcp://localhost:9090
../gradlew run
```

# Run Gateway in Docker

```
export DOCKER_REPOSITORY=claudiofahey
export IMAGE_TAG=0.7.0
export PRAVEGA_CONTROLLER=tcp://${HOST_IP}:9090
scripts/build-k8s-components.sh && \
docker run -d \
  --restart always \
  -e PRAVEGA_CONTROLLER \
  -p 54672:80 \
  --name pravega-grpc-gateway \
  ${DOCKER_REPOSITORY}/pravega-grpc-gateway:${IMAGE_TAG}
```

# Run Gateway in Dell EMC Streaming Data Platform (SDP)

1. Edit the file charts/pravega-grpc-gateway/values.yaml as needed.
   You will need to set `pravega.controller` and `service.annotations.external-dns.alpha.kubernetes.io/hostname`
   to match your environment.

2. Build and then deploy using Helm.

```
export DOCKER_REPOSITORY=claudiofahey
export IMAGE_TAG=0.7.0
export NAMESPACE=examples
scripts/build-k8s-components.sh
scripts/deploy-k8s-components.sh
```

# Using the Pravega GRPC Gateway from Python

```
git clone https://github.com/pravega/pravega-grpc-gateway /tmp/pravega-grpc-gateway && \
cd /tmp/pravega-grpc-gateway && \
pip install grpcio pravega-grpc-gateway/src/main/python
```

See [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py).

# Create Python Environment using Conda

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html).

2. Create Conda environment.
    ```
    cd pravega-grpc-gateway
    ./create_conda_env.sh
    ```

# Run Test and Sample Applications

1. Create Python environment as shown above.

2. Run the following commands:
    ```
    cd pravega-grpc-gateway
    conda activate ./env
    pip install -e src/main/python
    src/test/python/integration_test1.py
    ```

# Rebuild Python GRPC Stub for Pravega Gateway

This section is only needed if you make changes to the pravega.proto file.

This will build the Python files necessary to allow a Python application to call this gateway.

1. Create Python environment as shown above.

2. Run Protobuf compiler.
    ```
    ./build_python.sh
    ```
