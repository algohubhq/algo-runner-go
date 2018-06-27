FROM golang:1.10.3-alpine3.7 as buildenv
ARG VERSION
ARG GIT_COMMIT

RUN apk add --update --no-cache --repository http://nl.alpinelinux.org/alpine/edge/main/ \
      bash              \
      g++               \
      make              \
      pkgconfig         \
      openssl-dev       \
      gcc				\
      git 				\
      musl-dev          \
      zlib-dev
      
# install librdkafka
RUN git clone https://github.com/edenhill/librdkafka.git \
&& cd librdkafka \
&& ./configure --prefix /usr \
&& make \
&& make install

# Add Glide
RUN apk add --no-cache \
        ca-certificates \
        # https://github.com/Masterminds/glide#supported-version-control-systems
        git mercurial subversion bzr \
        openssh \
 && update-ca-certificates \
    \
 # Install build dependencies
 && apk add --no-cache --virtual .build-deps \
        curl make \
    \
 # Download and unpack Glide sources
 && curl -L -o /tmp/glide.tar.gz \
          https://github.com/Masterminds/glide/archive/v0.13.1.tar.gz \
 && tar -xzf /tmp/glide.tar.gz -C /tmp \
 && mkdir -p $GOPATH/src/github.com/Masterminds \
 && mv /tmp/glide-* $GOPATH/src/github.com/Masterminds/glide \
 && cd $GOPATH/src/github.com/Masterminds/glide \
    \
 # Build and install Glide executable
 && make install \
    \
 # Install Glide license
 && mkdir -p /usr/local/share/doc/glide \
 && cp LICENSE /usr/local/share/doc/glide/ \
    \
 # Cleanup unnecessary files
 && apk del .build-deps \
 && rm -rf /var/cache/apk/* \
           $GOPATH/src/* \
           /tmp/*

RUN mkdir -p /go/src/algo-runner-go
WORKDIR /go/src/algo-runner-go

COPY . /go/src/algo-runner-go

RUN glide install

# Stripping via -ldflags "-s -w" 
RUN CGO_ENABLED=1 GOOS=linux go build -tags static_all -a \
        -installsuffix cgo -o algo-runner-go .
    #&& GOARM=7 GOARCH=arm CGO_ENABLED=1 GOOS=linux go build -tags static_all -a \
    #    -installsuffix cgo -o algo-runner-go-armhf . \
    #&& GOARCH=arm64 CGO_ENABLED=1 GOOS=linux go build -tags static_all -a \
    #    -installsuffix cgo -o algo-runner-go-arm64 . \
    #&& GOOS=windows CGO_ENABLED=1 go build -tags static_all -a \
    #    -installsuffix cgo -o algo-runner-go.exe .