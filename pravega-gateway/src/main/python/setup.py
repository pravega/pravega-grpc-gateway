import setuptools

setuptools.setup(
    name="pravega",
    version="0.0.1",
    author="Claudio Fahey",
    author_email="claudio.fahey@dell.com",
    description="Pravega GRPC Gateway Client",
    url="https://github.com/pravega/pravega",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
