#!/usr/bin/env bash
CONTAINER_NAME="${1}-${3}"
COMMAND="python modules/${1}.py ${2} ${CONTAINER_NAME}"

APP_ROOT=/app
TEMPLATE=/tmp/${CONTAINER_NAME}

exec cat ${APP_ROOT}/scheduler/job-template.yaml | sed -e "s/NAME/${CONTAINER_NAME}/" > ${TEMPLATE}
echo "        args: [\"-c\", \"${COMMAND}\"]" >> ${TEMPLATE}

exec kubectl create -f ${TEMPLATE}
