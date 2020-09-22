[![Build Status](https://travis-ci.com/IBM/the-mesh-for-data-mover.svg?branch=master)](https://travis-ci.com/IBM/the-mesh-for-data-mover)

# Mover

This is a collection of movement components. They are mostly used in [the-mesh-for-data](https://github.com/IBM/the-mesh-for-data).

It copies data between combinations of COS and Kafka and applies transformations.
It's build with extendability in mind so that custom data stores or transformations can be used.
A description of data flows and data types can be found in the [mover matrix](Mover-matrix.md).

## Using the latest image

The CI pipeline of the mover builds an image regularly as new pull requests are merged and on a schedule so that
possible security updates of the base image are applied.

The latest image can be found at: `ghcr.io/the-mesh-for-data/mover:latest`

## Manually building the images

The following notes sketch out how to manually build the images.
The Spark base image can be build locally with the following command:

```docker build -t ghcr.io/the-mesh-for-data/spark-base:2.4.6 -f src/main/docker/spark/Dockerfile src/main/docker/spark```

After the base image is build the mover image can be build using:
```mvn package jib:dockerBuild -DskipTests -Plocal-to-ibm-cloud```

This will create the image locally. If a different image name and tag is preferred it can be specified with `-Djib.to.image=my_image:tag`.

Afterwards it can be pushed to a registry using the `docker push` command.

### Local building for use with local kind registry

1. ```docker build -t localhost:5000/the-mesh-for-data/spark-base:2.4.6 -f src/main/docker/spark/Dockerfile src/main/docker/spark```
2. ```mvn package jib:dockerBuild -DskipTests -Plocal-registry```

### Setting up the registry in RedHat CodeReady Containers

1. Get the host name of where the registry resides:
   `REG_HOST=$(oc get route default-route -n openshift-image-registry --template='{{ .spec.host }}')`
2. Configure DOCKER_OPTS environment, e.g.
   `export DOCKER_OPTS="--insecure-registry $REG_HOST:443"`
3. Extract the certificate from the registry:
    `oc extract secret/router-ca --keys=tls.crt -n openshift-ingress-operator`
4. Add the certificate to the keychain on the Mac:
    `sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain tls.crt`
    On Linux, copy the file:
    `cp tls.crt /etc/docker/certs.d/default-route-openshift-image-registry.apps-crc.testing/`
5. Restart docker
6. login to docker:
   docker login -u kubeadmin -p $(oc whoami -t) $REG_HOST

### Setting up the registry with minikube

Minikube setup:
`minikube start --vm-driver=virtualbox --addons=registry --kubernetes-version v1.16.0 --memory=4000mb`

Login to minikube using `minikube ssh` and run the following command to make sure that the image
registry is available for downloading images.

`echo -e "127.0.0.1\timage-registry.openshift-image-registry.svc" | sudo tee -a /etc/hosts`

* Point Docker environment to minikube's `eval $(minikube docker-env)`

### Running locally

1. Compile `mvn package`
2. Build container `mvn jib:dockerBuild`
3. Run example pod `kubectl apply -f src/main/resources/k8s-example.yaml`

### Building the Spark base image
The base image for the maven jib plugin is configured to be docker://spark-base:2.4.5.
So this image has to be available in the local docker daemon.

1. Go to spark directory `cd src/main/docker/spark`
2. Build base image `docker build -t spark-base:2.4.5 .`

### Troubleshooting
When the job is not starting or job shows permission errors like the following or errors about using uid ranges:
possible errors: `Caused by: java.nio.file.AccessDeniedException: ./mover-1.0-SNAPSHOT.jar`

Add the anyuid policy to your service account:
oc adm policy add-scc-to-user anyuid -z default -n mover
