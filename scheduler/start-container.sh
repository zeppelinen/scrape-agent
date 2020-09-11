#!/usr/bin/env bash

ME=${HOSTNAME}
NETWORK_ID=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' $ME)
DOCKER_PREFIX=scrape-scheduled
IMAGE=$(docker inspect --format='{{.Config.Image}}' $ME)
MOUNTS=$(docker container inspect  -f '{{ range .Mounts }}{{ .Source }}:{{ .Destination }} {{ end }}' $ME)
ENV_FILE=/variables.env
APP_ROOT="/app"
CONTAINER_NAME="${1}-${3}"
COMMAND="python modules/${1}.py ${2} ${CONTAINER_NAME}"

for mount in ${MOUNTS}; do 
  MOUNT_LIST="${MOUNT_LIST} -v ${mount}"
  if [[ "${mount}" == *"key.json"* ]]; then
    DIR=$(dirname ${mount%:*})
  fi
done

DOCKER_CMD="docker run \
  --rm -d \
  --name ${DOCKER_PREFIX}-${CONTAINER_NAME} \
  --network ${NETWORK_ID} \
  --env-file ${ENV_FILE} \
  ${MOUNT_LIST} \
  ${IMAGE} ${COMMAND}"
# exec docker so that we return ID and code 0 if success
# or error code if failure
exec ${DOCKER_CMD}