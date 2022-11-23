from setuptools import setup

setup(
    name="isolate-ipython",
    version="0.0.1",
    packages=["isolate_ipython"],
    install_requires=["isolate[grpc]", "dill==0.3.5.1"],
)
