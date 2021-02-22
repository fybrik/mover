#!/usr/bin/env bash
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
set -x

echo "Loading spark-base:$SPARK_VERSION..."

mkdir docker_images || docker load -i docker_images/spark-$SPARK_VERSION.tar || true
docker inspect spark-base:$SPARK_VERSION --format {{.Id}} > docker_images/spark-$SPARK_VERSION.hash || true
