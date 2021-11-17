# Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import os

from setuptools import find_packages, setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


console_scripts = [
    "sqswatcher = sqswatcher.sqswatcher:main",
    "nodewatcher = nodewatcher.nodewatcher:main",
    "jobwatcher = jobwatcher.jobwatcher:main",
    "slurm_resume = slurm_plugin.resume:main",
    "slurm_suspend = slurm_plugin.suspend:main",
    "clustermgtd = slurm_plugin.clustermgtd:main",
    "computemgtd = slurm_plugin.computemgtd:main",
]
version = "2.11.4"
requires = ["boto3>=1.7.55", "retrying>=1.3.3", "paramiko>=2.4.2", "requests>=2.24.0"]

setup(
    name="aws-parallelcluster-node",
    version=version,
    author="Amazon Web Services",
    description="aws-parallelcluster-node provides the scripts for an AWS ParallelCluster node.",
    url="https://github.com/aws/aws-parallelcluster-node",
    license="Apache License 2.0",
    packages=find_packages("src", exclude=["tests"]),
    package_dir={"": "src"},
    python_requires=">=3.5",
    install_requires=requires,
    entry_points=dict(console_scripts=console_scripts),
    zip_safe=False,
    package_data={"slurm_plugin": ["logging/*.conf"]},
    long_description=(
        "aws-parallelcluster-node is the python package installed on the Amazon EC2 instances launched "
        "as part of AWS ParallelCluster."
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: Apache Software License",
    ],
)
