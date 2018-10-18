#!/bin/sh

export arch=$(uname -m)

if [ "$arch" = "armv7l" ] ; then
    echo "Build not supported on $arch, use cross-build."
    exit 1
fi

GIT_COMMIT=$(git rev-list -1 HEAD)
VERSION=$(git describe --all --exact-match `git rev-parse HEAD` | grep tags | sed 's/tags\///')

docker inspect "algorun-go-buildenv:latest" > /dev/null 2>&1 || docker build --no-cache -t algorun-go-buildenv -f ./Dockerfile-buildenv .
# docker build --no-cache -t algorun-go-buildenv -f ./Dockerfile-buildenv .

docker build --no-cache --build-arg VERSION=$VERSION --build-arg GIT_COMMIT=$GIT_COMMIT -t algohub/algo-runner-go:build .

docker create --name buildoutput algohub/algo-runner-go:build echo

docker cp buildoutput:/go/src/algo-runner-go/algo-runner-go ./algo-runner-go
# docker cp buildoutput:/go/src/algo-runner-go/algo-runner-go-armhf ./algo-runner-go-armhf
# docker cp buildoutput:/go/src/algo-runner-go/algo-runner-go-arm64 ./algo-runner-go-arm64
# docker cp buildoutput:/go/src/algo-runner-go/algo-runner-go.exe ./algo-runner-go.exe

docker rm buildoutput