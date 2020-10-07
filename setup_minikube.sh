# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

minikube start --vm-driver=virtualbox --addons=registry --kubernetes-version v1.16.0 --memory=4000mb
minikube ssh --native-ssh=false "echo -e '127.0.0.1\timage-registry.openshift-image-registry.svc' | sudo tee -a /etc/hosts"

kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v0.13.1/cert-manager.yaml
kubectl wait --for=condition=available -n cert-manager deployment/cert-manager-webhook --timeout=180s
kubectl create ns the-mesh-for-data
kubectl apply -f movement_controller.yaml
