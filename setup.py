#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name = "snakepit",
    version = "0.1",
    packages = find_packages(),

    author = "Tommi Virtanen",
    author_email = "tv@eagain.net",
    description = "HiveDB partitioned database schema Python client",
    long_description = """

Implement best current practices for horizontal database partitioning
(aka sharding), using Python and SQLAlchemy.

Because hives are for ants and bees.

""".strip(),
    license = "MIT",
    keywords = "database hivedb",
    url = "http://eagain.net/software/snakepit/",

    entry_points = {
        'console_scripts': [
            'snakepit-create-hive = snakepit.cli:create_hive',
            'snakepit-create-dimension = snakepit.cli:create_dimension',
            'snakepit-create-node = snakepit.cli:create_node',
            ],
        },

    )

