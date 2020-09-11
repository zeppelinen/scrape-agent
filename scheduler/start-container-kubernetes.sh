#!/usr/bin/env bash
CONTAINER_NAME="${1}-${3}"
COMMAND="python modules/${1}.py ${2} ${CONTAINER_NAME}"

TEMPLATE=/tmp/${CONTAINER_NAME}
AFFINITY_LABEL="${1}-${3}"


exec cat ${APP_ROOT}/scheduler/container-template.yaml | sed -e "s/NAME/${CONTAINER_NAME}/" | sed -e "s/LABEL/${AFFINITY_LABEL}/g" > ${TEMPLATE}
echo "    args: [\"-c\", \"${COMMAND}\"]" >> ${TEMPLATE}

exec kubectl create -f ${TEMPLATE}
