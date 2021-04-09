[![GitHub Actions Build](https://github.com/mesh-for-data/the-mesh-for-data-mover/actions/workflows/build.yml/badge.svg)](https://github.com/mesh-for-data/the-mesh-for-data-mover/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/mesh-for-data/the-mesh-for-data-mover/branch/master/graph/badge.svg)](https://codecov.io/gh/mesh-for-data/the-mesh-for-data-mover)


# Mover

This is a  collection of data movement capabilities intended
as a building block that can  be integrated into a  variety of different
control        planes.          They        are         used        in
[the-mesh-for-data](https://github.com/IBM/the-mesh-for-data), but can
be run as a stand-alone service.

The  mover copies  data  between any two supported data stores, for example S3 and Kafka,  and
applies  transformations.  It's  built with  extensibility in  mind so
that additional data stores or transformations can be used.  A description
of the current  data flows and data  types can be found  in the [mover
matrix](Mover-matrix.md).

## Using the latest image

The CI pipeline of the mover builds an updated image as new pull
requests are merged and on a schedule so that security
updates of the base image are applied.

The latest image can be found at: `ghcr.io/mesh-for-data/mover:latest`

### Setting up a minimal movement only the-mesh-for-data version

In order to test the mover image on a K8s cluster there is no need for
a complete installation of [the-mesh-for-data](https://github.com/IBM/the-mesh-for-data).
A cut-down version that only supports the movement components can be
installed using the [movement_controller.yaml](movement_controller.yaml) file.

This deployment will install a control component and the CRDs that allow the execution
of data movements. In order to install the yaml please follow these steps:

1. Make sure the cert-manager operator is installed. Either by installing via OpenShift UI or manually via `kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v0.13.1/cert-manager.yaml`
2. Create a namespace: `kubectl create ns the-mesh-for-data`
3. Run the install YAML file: `kubectl apply -f movement_controller.yaml`
4. When the BatchTransfers are not created in the `the-mesh-for-data` namespace please install the event creator role in that namespace: `kubectl apply -f EventCreatorRole.yaml` (please adapt namespace in file)
5. Run some example BatchTransfers (please adapt template): `kubectl apply -f batchtransfer.yaml`

If you want to test with your own image make sure to update either the environment variable `MOVER_IMAGE` in the controller deployment or the `spec.image` in the BatchTransfer definition.

## Manually building the images

The following notes sketch out how to manually build the images.
The Spark base image can be built locally with the following command:

```docker build -t spark-base:2.4.7 -f src/main/docker/spark/spark2.Dockerfile src/main/docker/spark```

After the base image is finished the mover image can be built locally using:
```mvn package jib:dockerBuild -DskipTests -Plocal-to-ghcr```

If a different image name and tag is preferred it can be specified with `-Djib.to.image=my_image:tag`.

Afterwards it can be pushed to a registry using the `docker push` command.

## Local testing with _kind_
### Setting up _kind_
*Tested on OSX with Docker 19.03.13 and kind 0.9.0*

Create the kind cluster:
```
kind create cluster --name kind -v 4 --retain --wait=1m --config ./kind-config.yaml --image=kindest/node:v1.16.9
```
Configure kind cluster to find registry:
```
kubectl config use-context kind-kind
for node in $(kind get nodes); do
    kubectl annotate node "${node}" "tilt.dev/registry=localhost:5000";
done
```
Create local registry:
```
docker run -d --restart=always -p "5000:5000" --name "kind-registry" registry:2
docker network connect kind kind-registry
```

### Building image and pushing to _kind_
```
docker build -t spark-base:2.4.7 -f src/main/docker/spark/spark2.Dockerfile src/main/docker/spark
mvn package jib:dockerBuild -DskipTests -Plocal-registry
docker push localhost:5000/mesh-for-data/mover:latest
```

### Testing with Kafka
In order to develop/test against a Kafka environment the `setup_kafka.sh` method can be used in
the kind environment. This will use the [strimzi operator](https://strimzi.io) to install a local Kafka
cluster that can be used to read/write data to. This will expose the kafka cluster on port 30092 on your
local machine and the schema registry for Kafka on port 30081. 
An entry has to be added to your local /etc/hosts file so that `kafka0` maps to `127.0.0.1`:
```
127.0.0.1 kafka0
``` 

### Running images

The BatchTransfer spec.image parameter has to be set to `localhost:5000/mesh-for-data/mover:latest`.

## Local testing with _minikube_
### Setting up the registry with _minikube_
*Tested with minikube v1.8.1.*

minikube setup:
`minikube start --vm-driver=virtualbox --addons=registry --kubernetes-version v1.16.0 --memory=4000mb`

Login to minikube using `minikube ssh` and run the following command to make sure that the image
registry is available for downloading images.

`echo -e "127.0.0.1\timage-registry.openshift-image-registry.svc" | sudo tee -a /etc/hosts`

Point Docker environment to minikube's `eval $(minikube docker-env)`

### Building image and pushing to minikube
minikube is running in a VM and has a docker instance. This can be used to build and load images.
Make sure that your docker client is using minikube's docker environment: `eval $(minikube docker-env)`

```
docker build -t spark-base:2.4.7 -f src/main/docker/spark/spark2.Dockerfile src/main/docker/spark
mvn package jib:dockerBuild -DskipTests -Pdev
```

### Running images

The BatchTransfer spec has to be adapted changing the image and imagePullPolicy.
The image pull policy has to be `IfNotPresent` as no image should be pulled but the already available
image in the local minikube docker should be used.
  
```
image: mover:latest
imagePullPolicy: "IfNotPresent"
```

## Testing with OpenShift

### Setting up with OpenShift

1. Make sure the cert-manager operator is installed. Either by installing via OpenShift UI or manually via `kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v0.13.1/cert-manager.yaml`
2. Install the minimal movement operator as described above
3. Make sure you can push images to your OpenShift. On the IBM Cloud an article on how to set up an external route can be found [here](https://cloud.ibm.com/docs/openshift?topic=openshift-registry#openshift_internal_registry)

### Building image and pushing to OpenShift

In OpenShift it's important to push the images to the project where they will be used. So if the BatchTransfers
are to be used in namespace `default` the image should be available internally at `image-registry.openshift-image-registry.svc:5000/default/mover:latest`.

Note: OpenShift has external and internal URLs. To push the image from your local machine to e.g. an OpenShift
in the cloud the external URL might be something like `image-registry-openshift-image-registry.demo-demo-deadbeef-0000.eu-de.containers.appdomain.cloud`
while the internal URL where the images will be reachable is `image-registry.openshift-image-registry.svc:5000.` The images
will be re-tagged automatically.

```
docker build -t spark-base:2.4.7 -f src/main/docker/spark/spark2.Dockerfile src/main/docker/spark
mvn package jib:dockerBuild -DskipTests -Djib.to.image=<your_registry>/<your_namespace>/mover:latest
docker push <your_registry>/<your_namespace>/mover:latest
```

### Running images

The BatchTransfer spec.image parameter has to be set to `image-registry.openshift-image-registry.svc:5000/<your_namespace>/mover:latest`.

Alternatively if you want to specify a default image for all BatchTransfers and StreamTransfers please change the `MOVER_IMAGE`
environment variable in the deployment of the controller.
```kubectl -n the-mesh-for-data edit deployment m4d-controller-manager```

## Running locally in the IDE

The app can be run locally via the [AppTest](src/test/scala/com/ibm/m4d/mover/AppTest.scala) suite as well. Extend it with another test
 that is using a configuration of your choosing. A correct configuration file has to be put in a path. An example template configuration
can be found [here](src/main/resources/test.conf.template)  

### Troubleshooting
When the job is not starting, shows errors about using uid ranges or shows permission errors like the following :

- `Caused by: java.nio.file.AccessDeniedException: ./mover-1.0-SNAPSHOT.jar`

Add the anyuid policy to your service account:
`oc adm policy add-scc-to-user anyuid -z default -n mover`

- When the job ends with 
```
[Transfer$] Could not send finished event to Kubernetes!
io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: POST
```
then the proper roles to create events are not defined in the namespace you are using. Please run `kubectl apply -f EventCreatorRole.yaml` 
