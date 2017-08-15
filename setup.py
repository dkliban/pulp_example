#!/usr/bin/env python3

from setuptools import setup

requirements = [
    'pulpcore-plugin',
]

setup(
    name='pulp-example',
    version='0.0.1a1.dev1',
    description='Example plugin for the Pulp Project',
    author='Pulp Project Developers',
    author_email='pulp-dev@redhat.com',
    url='http://www.pulpproject.org/',
    install_requires=requirements,
    include_package_data=True,
    packages=['pulp_example'],
    entry_points={
        'pulpcore.plugin': [
            'pulp_example = pulp_example:default_app_config',
        ]
    }
)
