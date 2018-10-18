# Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import os, sys
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


console_scripts = ['sqswatcher = sqswatcher.sqswatcher:main',
                   'nodewatcher = nodewatcher.nodewatcher:main',
                   'jobwatcher = jobwatcher.jobwatcher:main']
version = "1.6.0"
requires = ['boto3>=1.7.55', 'python-dateutil>=2.6.1']

if sys.version_info[:2] == (2, 6):
    # For python2.6 we have to require argparse since it
    # was not in stdlib until 2.7.
    requires.append('argparse>=1.4')
    requires.append('paramiko==2.3.3')
    requires.append('pycparser==2.18')
else:
    requires.append('paramiko>=2.4.2')

setup(
    name = "cfncluster-node",
    version = version,
    author = "Dougal Ballantyne",
    author_email = "dougalb@amazon.com",
    description = ("cfncluster-node provides the scripts for a cfncluster node."),
    url = ("https://github.com/awslabs/cfncluster"),
    license = "Apache License 2.0",
    packages = find_packages(),
    install_requires = requires,
    entry_points=dict(console_scripts=console_scripts),
    include_package_data = True,
    zip_safe = False,
    package_data = {
        '' : ['examples/config'],
    },
    long_description = ("cfncluster-node is the python package installed on the Amazon EC2 instances launched as part of CfnCluster."),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: Apache Software License",
    ],
)
