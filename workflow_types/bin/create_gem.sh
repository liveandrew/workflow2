#!/usr/bin/env bash

set -e

DIR="/tmp/workflow2_types_gem/"

rm -rf $DIR

mkdir -p $DIR/lib
cp gen-rb/* $DIR/lib
cp workflow2_types.gemspec $DIR

cd $DIR

gem build workflow2_types.gemspec

echo "Gem built in $DIR"
