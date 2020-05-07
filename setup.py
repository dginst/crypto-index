from setuptools import setup
import cryptoindex

setup(
    name=cryptoindex.name,
    version=cryptoindex.__version__,
    packages=['cryptoindex'],
    test_suite="cryptoindex.tests"
)
