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

FROM adoptopenjdk/openjdk11:alpine-jre
#FROM adoptopenjdk/openjdk11:ubi-minimal-jre
ARG SPARK_VERSION=3.0.2
#ARG HADOOP_VERSION=without-hadoop
ARG HADOOP_VERSION=hadoop2.7

ENV LANG=en_US.utf8

RUN apk update --no-cache && \
 apk add bash tini

# Download Spark and remove some of the unused dependencies like spark-mllib etc...
RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz && \
    mkdir -p /opt/spark && \
    cp -R /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/jars /opt/spark/jars && \
    cp -R /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/bin /opt/spark/bin && \
    cp -R /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/sbin /opt/spark/sbin && \
    cp -R /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}/kubernetes/dockerfiles/spark/entrypoint.sh /opt/ && \
    rm -rf /spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} && \
    rm spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz && \
    rm /opt/spark/jars/JTransforms-3.1.jar && \
    rm /opt/spark/jars/arpack_combined_all-0.1.jar && \
    rm /opt/spark/jars/breeze-macros_2.12-1.0.jar && \
    rm /opt/spark/jars/breeze_2.12-1.0.jar && \
    rm /opt/spark/jars/hive-beeline-2.3.7.jar && \
    rm /opt/spark/jars/hive-cli-2.3.7.jar && \
    rm /opt/spark/jars/hive-common-2.3.7.jar && \
    rm /opt/spark/jars/hive-exec-2.3.7-core.jar && \
    rm /opt/spark/jars/hive-jdbc-2.3.7.jar && \
    rm /opt/spark/jars/hive-llap-common-2.3.7.jar && \
    rm /opt/spark/jars/hive-metastore-2.3.7.jar && \
    rm /opt/spark/jars/hive-serde-2.3.7.jar && \
    rm /opt/spark/jars/hive-shims-0.23-2.3.7.jar && \
    rm /opt/spark/jars/hive-shims-2.3.7.jar && \
    rm /opt/spark/jars/hive-shims-common-2.3.7.jar && \
    rm /opt/spark/jars/hive-shims-scheduler-2.3.7.jar && \
    rm /opt/spark/jars/hive-storage-api-2.7.1.jar && \
    rm /opt/spark/jars/hive-vector-code-gen-2.3.7.jar && \
    rm /opt/spark/jars/core-1.1.2.jar && \
    rm /opt/spark/jars/opencsv-2.3.jar && \
    rm /opt/spark/jars/machinist_2.12-0.6.8.jar && \
    rm /opt/spark/jars/mesos-1.4.0-shaded-protobuf.jar && \
    rm /opt/spark/jars/metrics-graphite-4.1.1.jar && \
    rm /opt/spark/jars/spark-mllib-local_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-mllib_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-streaming_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-graphx_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-mesos_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spire-macros_2.12-0.17.0-M1.jar && \
    rm /opt/spark/jars/spire-platform_2.12-0.17.0-M1.jar && \
    rm /opt/spark/jars/spire-util_2.12-0.17.0-M1.jar && \
    rm /opt/spark/jars/spire_2.12-0.17.0-M1.jar

ENV SPARK_HOME /opt/spark

ENTRYPOINT [ "/opt/entrypoint.sh" ]
