#!/bin/bash
set -e

./setup.sh
# TODO: better method of avoiding linting / testing in build, install, log, ...
mkdir -p .build_ws/src
cp -rpa * .build_ws/src/
cd .build_ws
chown postgres:postgres . -R
su postgres
./build.sh
./test.sh
