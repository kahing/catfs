#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

dir=$(dirname $0)

mkdir bench/src
mkdir bench/cache
mkdir bench/mnt

CATFS="catfs bench/src bench/cache bench/mnt"
LOCAL="cat"

$dir/bench.sh $LOCAL bench/mnt $t |& tee $dir/bench.local
$dir/bench.sh "$CATFS" bench/mnt $t |& tee $dir/bench.catfs

rmdir bench/src
rmdir bench/cache
rmdir bench/mnt

$dir/bench_format.py <(paste $dir/bench.catfs $dir/bench.local) > $dir/bench.data

gnuplot $dir/bench_graph.gnuplot && convert -rotate 90 $dir/bench.png $dir/bench.png
