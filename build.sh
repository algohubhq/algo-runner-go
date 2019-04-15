#!/bin/sh

GIT_COMMIT=$(git rev-list -1 HEAD)
VERSION=$(git describe --all --exact-match `git rev-parse HEAD` | grep tags | sed 's/tags\///')

docker build --build-arg VERSION=$VERSION --build-arg GIT_COMMIT=$GIT_COMMIT -t algohub/algo-runner:$GIT_COMMIT .

docker tag algohub/algo-runner:$GIT_COMMIT algohub/algo-runner:latest