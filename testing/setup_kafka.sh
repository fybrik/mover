# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

kubectl create ns kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait -n kafka deployment/strimzi-cluster-operator --for=condition=available --timeout 30s

#kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka
kubectl apply -f kafka-dev.yaml -n kafka
kubectl apply -f apicurio-registry.yaml -n kafka
# kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
