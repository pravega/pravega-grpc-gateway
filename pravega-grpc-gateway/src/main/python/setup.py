import setuptools

setuptools.setup(
    name="pravega-grpc-gateway-client",
    version="0.0.4",
    author="Claudio Fahey",
    author_email="claudio.fahey@dell.com",
    description="Pravega GRPC Gateway Client",
    url="https://github.com/pravega/pravega-grpc-gateway",
    packages=setuptools.find_namespace_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
