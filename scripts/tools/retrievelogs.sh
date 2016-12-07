#!/usr/bin/env bash
# set -xv

celery_path="/home/ubuntu/Programs/medusa-1.0/logs/celerydebug.log"
target_celery_path_nvirginia="celerydebug-nvirginia.log"
target_celery_path_california="celerydebug-california.log"
target_celery_path_ireland="celerydebug-ireland.log"
target_celery_path_oregon="celerydebug-oregon.log"

medusa_client_logs="~/Programs/medusa-1.0/webdatascan-process-*"
# medusa_client_logs="~/Programs/medusa-1.0/wordcount-process-*"

HOST_OREGON="medusa-oregon-1"
HOST_NVIRGINIA="medusa-nvirginia-1"
HOST_CALIFORNIA="medusa-california-1"
HOST_IRELAND="medusa-ireland-1"
HOST_RABBITMQ="medusa-ec2-rabbitmq"

PEM_NVIRGINIA=~/.ssh/ec2-medusa2-mpc-nvirginia.pem
PEM_CALIFORNIA=~/.ssh/ec2-medusa2-mpc-california.pem
PEM_IRELAND=~/.ssh/ec2-medusa2-mpc-ireland.pem
PEM_OREGON=~/.ssh/ec2-medusa2-mpc-oregon.pem

scp -i ${PEM_OREGON} ubuntu@${HOST_OREGON}:${celery_path} ${target_celery_path_oregon}
scp -i ${PEM_NVIRGINIA} ubuntu@${HOST_NVIRGINIA}:${celery_path} ${target_celery_path_nvirginia}
scp -i ${PEM_CALIFORNIA} ubuntu@${HOST_CALIFORNIA}:${celery_path} ${target_celery_path_california}
scp -i ${PEM_IRELAND} ubuntu@${HOST_IRELAND}:${celery_path} ${target_celery_path_ireland}

scp -i ${PEM_OREGON} ubuntu@${HOST_RABBITMQ}:${medusa_client_logs} .
