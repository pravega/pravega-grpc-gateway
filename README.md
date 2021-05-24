# Pravega gRPC Gateway

## Introduction

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

## Running Pravega gRPC Gateway

### Running Locally

1. Make sure to have `JDK-8` & `JRE-8` in your system.

2. Save Pravega Controller Running address in environment. Change `localhost` and `9090` according to your setup.

    ``` bash
    export PRAVEGA_CONTROLLER=tcp://localhost:9090
    ```

3. Run `gradlew` file.

    ```bash
    ./gradlew run
    ```

4. Pravega gRPC Gateway would be running at Port `54672`.

### Running in Docker

1. Save Pravega Controller Running address in environment. Change `localhost` and `9090` according to your setup.

    ``` bash
    export PRAVEGA_CONTROLLER=tcp://localhost:9090
    ```

2. Run the following commands to have Pravega gRPC Gateway Up & Running at Port `54672`.

    ``` bash
    export DOCKER_REPOSITORY=claudiofahey
    export IMAGE_TAG=0.7.0
    docker run -d \
    --restart always \
    -e PRAVEGA_CONTROLLER \
    -p 54672:80 \
    --name pravega-grpc-gateway \
    ${DOCKER_REPOSITORY}/pravega-grpc-gateway:${IMAGE_TAG}
    ```

### Running in Dell EMC Streaming Data Platform (SDP)

1. Edit the file `charts/pravega-grpc-gateway/values.yaml` as needed. You will need to set `pravega.controller` and `service.annotations.external-dns.alpha.kubernetes.io/hostname` to match your environment.

2. Build and then deploy using Helm.

    ```bash
    export DOCKER_REPOSITORY=claudiofahey
    export IMAGE_TAG=0.7.0
    export NAMESPACE=examples
    scripts/build-k8s-components.sh
    scripts/deploy-k8s-components.sh
    ```

## Accessing Pravega gRPC Gateway using Python 3

### Using Virtual Enivornment

1. Create & Enabling Virtual Enivornment.

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Run to following command to install Pravega gRPC Client in your environment.

    ``` bash
    git clone https://github.com/pravega/pravega-grpc-gateway /tmp/pravega-grpc-gateway && \
    cd /tmp/pravega-grpc-gateway && \
    pip install grpcio pravega-grpc-gateway/src/main/python
    ```

3. Run [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py) for confirming the Setup & Connection. Do Change Pravega gRPC Gateway IP & Port in [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py), according to your setup.

### Using Conda Environment

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html).

2. Create Conda environment.

    ```bash
    git clone https://github.com/pravega/pravega-grpc-gateway 
    cd pravega-grpc-gateway/pravega-grpc-gateway/
    ./create_conda_env.sh
    ```

3. Activate Conda Environemnt.

    ```bash
    conda activate ./env
    ```

4. Install Pravega gRPC Gateway Client.

    ```bash
    pip install -e src/main/python
    ```

5. Run [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py) for confirming the Setup & Connection. Do Change Pravega gRPC Gateway IP & Port in [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py), according to your setup.

## Code-Snippets

### Initalizing

Replace `pravega-grpc-gateway.example.com:80` with your Host IP & Port of Pravega gRPC Gateway.

```python
import grpc
import pravega.grpc_gateway as pravega
gateway = 'pravega-grpc-gateway.example.com:80'
pravega_channel = grpc.insecure_channel(gateway, options=[
        ('grpc.max_receive_message_length', 9*1024*1024),
    ])
pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)
```

### Write Events to a Stream

```python
events_to_write = (pravega.pb.WriteEventsRequest(
        scope='examples',
        stream='my-stream',
        event=('%d,%s' % (i, datetime.datetime.now())).encode('UTF-8'),
        routing_key='0',
    ) for i in range(10))
pravega_client.WriteEvents(events_to_write)
```

### Read Events From a Stream

```python
read_events_request = pravega.pb.ReadEventsRequest(
    scope='examples',
    stream='my-stream',
    from_stream_cut=None,
    to_stream_cut=None,
)
for event in pravega_client.ReadEvents(read_events_request):
    print(event.event.args.decode('UTF-8'))
```

### More Examples

1. [event_generator.py](pravega-grpc-gateway/src/test/python/event_generator.py) contains example for writing into stream.

2. [event_reader.py](pravega-grpc-gateway/src/test/python/event_reader.py) contains example for reading from stream event by event.

3. [batch_reader.py](pravega-grpc-gateway/src/test/python/batch_reader.py) contains example for reading stream into batches.

4. [integration_test1.py](pravega-grpc-gateway/src/test/python/integration_test1.py) contains bunch of implementation (eg. Truncating & Deleting Stream).

5. See [pravega.proto](pravega-grpc-gateway/src/main/proto/pravega/grpc_gateway/pravega.proto) for all available RPCs.

## Developer Corner

### Rebuilding Python gRPC Stub for Pravega gRPC Gateway

This section is only needed if you make changes to the pravega.proto file.

This will build the Python files necessary to allow a Python application to call this gateway.

1. Create Python environment as shown above.

2. Run Protobuf compiler.

    ```bash
    ./build_python.sh
    ```
