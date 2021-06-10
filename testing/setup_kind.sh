# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

kind create cluster --name kind -v 4 --retain --wait=1m --config ./kind-config.yaml --image=kindest/node:v1.16.9
kubectl config use-context kind-kind
for node in $(kind get nodes); do
    kubectl annotate node "${node}" "tilt.dev/registry=localhost:5000";
done
docker run -d --restart=always -p "5000:5000" --name "kind-registry" registry:2
docker network connect kind kind-registry

kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v0.13.1/cert-manager.yaml
kubectl wait --for=condition=available -n cert-manager deployment/cert-manager-webhook --timeout=180s
kubectl create ns mesh-for-data
kubectl apply -f movement_controller.yaml
