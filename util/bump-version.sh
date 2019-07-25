#!/bin/bash

set -ex

if [ -z "$1" ]; then
    echo "New version not specified. Usage: bump-version.sh NEW_VERSION"
    exit 1
fi

NEW_VERSION=$1
CURRENT_VERSION=$(sed -ne "s/^version = \"\(.*\)\"/\1/p" setup.py)

sed -i "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/g" setup.py
