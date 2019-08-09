# Pravega Gateway

This is a GRPC server that provides a gateway to a Pravega server.
It provides **limited** Pravega functionality to any environment that support GRPC, including Python.

Using a GRPC gateway is better than a REST gateway for the following reasons:

- GRPC streaming is used for reading and writing events. This allows the Pravega connection to remain open for the life
  of the streaming request. Within a streaming request, any number of read or write operations can be performed.
  In the case of writing, these can also be wrapped in transactions. 
  Events can be marked to commit the current transaction and open a new one.
  
- GRPC uses Protobuf for efficient serialization.
  REST/JSON requires base64 encoding of binary data which is less efficient.
  
- GRPC stubs (client code) can be easily created for nearly any language and environment.

# Run Gateway Locally

```
export PRAVEGA_CONTROLLER=tcp://localhost:9090
../gradlew run
```

# Run Gateway in Kubernetes

See [Deploying to Nautilus](../README.md#deploying-to-nautilus). 

# Rebuild Python GRPC Stub for Pravega Gateway

This section is only needed if you make changes to the pravega.proto file.

This will build the Python files necessary to allow a Python application to call this gateway.

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html).

2. Create Conda environment.
    ```
    ./create_conda_env.sh
    ```

3. Run Protobuf compiler.
    ```
    ./build_python.sh
    ```

# Run Test and Sample Applications

```
conda activate ./env
pip install -e src/main/python
src/test/python/event_generator.py
```
