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

FROM adoptopenjdk/openjdk8:alpine-jre
ARG SPARK_VERSION=2.4.8
ARG HADOOP_VERSION=2.7

ENV LANG=en_US.utf8

# Download Spark and remove some of the unused dependencies like spark-mllib etc...
RUN wget https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    mkdir -p /opt/spark && \
    cp -R /spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12/jars /opt/spark/jars && \
    cp -R /spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12/bin /opt/spark/bin && \
    cp -R /spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12/sbin /opt/spark/sbin && \
    cp -R /spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12/kubernetes/dockerfiles/spark/entrypoint.sh /opt/ && \
    rm -rf /spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12 && \
    rm spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    rm /opt/spark/jars/breeze-macros_2.12-0.13.2.jar && \
    rm /opt/spark/jars/breeze_2.12-0.13.2.jar && \
    rm /opt/spark/jars/spark-mllib-local_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-mllib_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-streaming_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-graphx_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spark-mesos_2.12-${SPARK_VERSION}.jar && \
    rm /opt/spark/jars/spire-macros_2.12-0.13.0.jar && \
    rm /opt/spark/jars/spire_2.12-0.13.0.jar && \
    rm /opt/spark/jars/opencsv-2.3.jar && \
    rm /opt/spark/jars/jtransforms-2.4.0.jar && \
    rm /opt/spark/jars/machinist_2.12-0.6.1.jar && \
    rm /opt/spark/jars/mesos-1.4.0-shaded-protobuf.jar && \
    rm /opt/spark/jars/metrics-graphite-3.1.5.jar && \
    rm /opt/spark/jars/core-1.1.2.jar && \
    rm /opt/spark/jars/arpack_combined_all-0.1.jar

RUN apk update --no-cache && \
 apk add bash tini && \
 ln -s /sbin/tini /usr/bin/tini

ENV SPARK_HOME /opt/spark

ENTRYPOINT [ "/opt/entrypoint.sh" ]
