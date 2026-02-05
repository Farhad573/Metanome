#!/usr/bin/env bash

echo "This script does not support Microsoft Windows please use the run.bat script.\n"

trap 'kill %1' SIGINT
# new port to avoid conflict with old version running on same system, to make comparison possible.
java -Xmx2g -jar webapp-runner.jar backend --port 5172  &
java -Xmx2g -jar webapp-runner.jar frontend
