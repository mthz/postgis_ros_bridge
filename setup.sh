#!/bin/bash
set -e

export DEBIAN_FRONTEND=noninteractive

apt-get update

apt-get install -y curl

curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg-testing main 15" | tee /etc/apt/sources.list.d/pgdg-testing.list

apt-get update \
   && apt-get -y install --no-install-recommends \
   postgresql-14 postgresql-14-postgis-3 \
   python3-pip \
   git-lfs \
   libpq-dev \
   && apt-get autoremove -y

# pip install sqlalchemy shapely psycopg2-binary pyproj scipy pytest-postgresql

rosdep update
rosdep install --from-paths . --ignore-src -y
