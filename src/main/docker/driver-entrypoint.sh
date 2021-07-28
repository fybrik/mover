#!/bin/bash
#
# (C) Copyright IBM Corporation 2020.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#


#set -ex

/opt/spark/bin/spark-submit --master \
          k8s://https://kubernetes.default.svc.cluster.local \
          --deploy-mode client \
          --driver-memory $SPARK_DRIVER_MEMORY \
          --driver-class-path "/app/classpath/$MAIN_JAR:/app/libs/*" \
          --driver-java-options "-Dlog4j.configuration=log4j.properties -Divy.home=/opt/spark/work-dir" \
          --class io.fybrik.mover.Transfer \
          --conf spark.kubernetes.allocation.batch.delay=1s \
          --conf spark.executor.extraClassPath=/app/libs/* \
          --conf spark.kubernetes.authenticate.caCertFile=/run/secrets/kubernetes.io/serviceaccount/ca.crt \
          --conf spark.kubernetes.namespace=$NAMESPACE \
          --conf spark.driver.host=$MY_POD_IP \
          --conf spark.driver.port=14536 \
          --conf spark.kubernetes.driver.pod.name=$MY_NODE_NAME \
          /app/classpath/$MAIN_JAR \
          $1
