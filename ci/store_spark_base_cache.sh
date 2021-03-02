#!/usr/bin/env bash
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

echo "Saving spark-base:$SPARK_VERSION to docker_images/spark-$SPARK_VERSION.tar..."

rm docker_images/spark.tar

newHash=$(docker inspect spark-base:$SPARK_VERSION --format {{.Id}})
if [[ -f "docker_images/spark-$SPARK_VERSION.tar" ]]; then
  if [[ $(< docker_images/spark-$SPARK_VERSION.hash) != "$newHash" ]]; then
    mkdir docker_images || true
    docker save -o docker_images/spark-$SPARK_VERSION.tar spark-base:$SPARK_VERSION
    echo "Image saved to docker_images/spark-$SPARK_VERSION.tar!"
  else
    echo "Image already cached!"
  fi
else
  mkdir docker_images || true
  docker save -o docker_images/spark-$SPARK_VERSION.tar spark-base:$SPARK_VERSION
  echo "Image saved to docker_images/spark-$SPARK_VERSION.tar!"
fi

