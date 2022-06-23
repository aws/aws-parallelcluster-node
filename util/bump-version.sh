#!/bin/bash

set -ex

# On Mac OS, the default implementation of sed is BSD sed, but this script requires GNU sed.
if [ "$(uname)" == "Darwin" ]; then
  command -v gsed >/dev/null 2>&1 || { echo >&2 "[ERROR] Mac OS detected: please install GNU sed with 'brew install gnu-sed'"; exit 1; }
  PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
fi

if [ -z "$1" ]; then
    echo "New version not specified. Usage: bump-version.sh NEW_VERSION"
    exit 1
fi

NEW_VERSION=$1
CURRENT_VERSION=$(sed -ne "s/^version = \"\(.*\)\"/\1/p" setup.py)

sed -i "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/g" setup.py
