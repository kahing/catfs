#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <src> <cache>"
    exit 1
fi

SRC="$1"
CACHE="$2"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BIN="$DIR/$0"

if [ $# == 2 ]; then
    pushd "$CACHE" > /dev/null
    find . -type f -print0 | xargs -0 -n 1 -P 100 "$BIN" "$SRC" "$CACHE"
    popd > /dev/null
else
    FILE="$3"
    pushd "$SRC" > /dev/null
    strtosign=$(getfattr -e hex --match=s3.etag -d "$FILE" 2>/dev/null | grep =; \
                /usr/bin/stat -t --printf "%Y\n%s\n" "$FILE")
    sum=$(echo "$strtosign" | sha512sum | cut -f1 '-d ')
    setfattr -n user.catfs.src_chksum -v 0x$sum "$CACHE/$FILE"
    echo "$FILE $strtosign $sum"
    popd > /dev/null
fi
