from setuptools import setup, find_packages

setup(
    name='pelican',
    version='0.0.1',
    install_requires=[
        "fastavro",
        "flask",
        "flask-restful",
        "inflection",
        "psycopg2-binary",
        "pyspark",
        "requests"
    ],
    packages=find_packages(),
)
