#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="goofys-bench"}
: ${FAST:="false"}
: ${SSHFS_OPTS:="-o StrictHostKeyChecking=no -o Cipher=arcfour ${SSHFS_SERVER}:/tmp"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir -p target/src
mkdir -p target/cache
mkdir -p target/mnt

CATFS="catfs target/src target/cache target/mnt"
LOCAL="cat"
SSHFS="sshfs -f ${SSHFS_OPTS} target/mnt"

function catsshfs {
    # sometimes we wouldn't umount sshfs cleanly after the previous run
    fusermount -u target/src >& /dev/null || true
    sshfs ${SSHFS_OPTS} target/src
    sleep 1
    catfs target/src target/cache target/mnt
    fusermount -u target/src >& /dev/null || true
}

export -f catsshfs

CATSSHFS="catsshfs"

for fs in cat catfs sshfs catsshfs; do
    case $fs in
        cat)
            FS=$LOCAL
            export FAST=false
            ;;
        catfs)
            FS=$CATFS
            export FAST=false
            ;;
        sshfs)
            FS=$SSHFS
            export FAST=true
            ;;
        catsshfs)
            FS=$CATSSHFS
            export FAST=true
            ;;
    esac

    rm target/bench.$fs 2>/dev/null || true
    if [ "$t" = "" ]; then
        for tt in create create_parallel io cleanup ls; do
            $dir/bench.sh "$FS" target/mnt $tt |& tee -a target/bench.$fs
        done
    else
        $dir/bench.sh "$FS" target/mnt $t |& tee target/bench.$fs
    fi

done

rmdir target/src
rmdir target/cache
rmdir target/mnt

$dir/bench_format.py <(paste target/bench.catfs target/bench.cat) > target/bench.data
FAST=true $dir/bench_format.py <(paste target/bench.catsshfs target/bench.sshfs) > target/bench.catfs_vs_sshfs.data

gnuplot -c $dir/bench_graph.gnuplot target/bench.data target/bench.png catfs local
gnuplot -c $dir/bench_graph.gnuplot target/bench.catfs_vs_sshfs.data target/bench.catfs_vs_sshfs.png \
        "catfs over sshfs" "sshfs"

convert -rotate 90 target/bench.png target/bench.png
convert -rotate 90 target/bench.catfs_vs_sshfs.png target/bench.catfs_vs_sshfs.png
