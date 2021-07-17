#!/usr/bin/env bash

if [ $# -eq 0 ]
then
  echo "No arguments provided."
elif [ "$1" = "--cluster" ]
then
  export AWS_ACCESS_KEY_ID="$4"
  export AWS_SECRET_ACCESS_KEY="$5"
  echo "Using AWS cluster..."
  ray up --no-config-cache -y config.yaml
  echo "Ray cluster started."
  ray exec --no-config-cache config.yaml " cd ~/app/ && python main.py $*"
  echo "Cleaning up..."
  ray down -y config.yaml
else
  echo "Running in local mode..."
  python main.py "$@"
fi
