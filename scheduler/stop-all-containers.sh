#!/usr/bin/env bash

# Usage: stop all containers beginning with prefix.
# Example: stop-all-containers.sh scrape-scheduled

# defaults

# FIXME: use container labels in addition to prefixes

DOCKER_PREFIX=scrape-scheduled

if [ $# -ne 0 ]
then
    DOCKER_PREFIX=$1
fi

RUNNING_CONTAINERS=$(docker ps -q --filter="name=${DOCKER_PREFIX}" | xargs)

if [[ $RUNNING_CONTAINERS ]]; then 
	echo "STOPPING CONTAINERS:"
	docker rm -f ${RUNNING_CONTAINERS}
else
	echo "NO RUNNING CONTAINERS FOUND"
fi