#!/usr/bin/env bash
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

echo "Storing spark-base:$SPARK_VERSION"

newHash=$(docker inspect spark-base:$SPARK_VERSION --format {{.Id}})
if [[ -f "docker_images/spark-$SPARK_VERSION.tar" ]]; then
  if [[ $(< docker_images/spark-$SPARK_VERSION.hash) != "$newHash" ]]; then
    mkdir docker_images || true
    docker save -o docker_images/spark-$SPARK_VERSION.tar spark-base:$SPARK_VERSION
  else
    echo "Image already cached"
  fi
else
  mkdir docker_images || true
  docker save -o docker_images/spark-$SPARK_VERSION.tar spark-base:$SPARK_VERSION
fi

