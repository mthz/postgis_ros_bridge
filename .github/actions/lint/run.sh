#!/bin/bash
set -e

./setup.sh
source /opt/ros/${ROS_DISTRO}/setup.bash
ament_${LINTER} ./
