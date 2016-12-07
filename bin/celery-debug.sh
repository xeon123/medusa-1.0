#!/bin/bash
set -xv

export C_FORCE_ROOT="true"
HOST_NAME=`hostname`

MEDUSA_HOME=$HOME/Programs/medusa-1.0

echo "------------------------"
echo "Initialize celery at $HOST_NAME"
echo "------------------------"
celery worker -n ${HOST_NAME} -E --loglevel=DEBUG --concurrency=20 -f ./logs/celerydebug.log --config=celeryconfig -Q ${HOST_NAME}
