FROM alpine:3.2 AS sasl

ENV CYRUS_SASL_VERSION=2.1.27-rc7

RUN set -x \
 && mkdir -p /srv/saslauthd.d /tmp/cyrus-sasl /var/run/saslauthd \
 && apk add --update autoconf \
        automake \
        curl \
        db-dev \
        g++ \
        gcc \
        gzip \
        libtool \
        make \
        openssl-dev \
        tar \
        krb5-dev \
        perl \
        py-sphinx \
        python \
        man \
# Install cyrus-sasl from source
&& curl -fL https://github.com/cyrusimap/cyrus-sasl/archive/cyrus-sasl-$CYRUS_SASL_VERSION.tar.gz -o /tmp/cyrus-sasl.tgz \
&& tar -xzf /tmp/cyrus-sasl.tgz --strip=1 -C /tmp/cyrus-sasl \
&& cd /tmp/cyrus-sasl \
&& ./autogen.sh \
        --enable-static \
        --disable-shared \
        --prefix=/usr \
        --sysconfdir=/etc \
        --localstatedir=/var \
        --enable-plain \
        --enable-scram \
        --enable-krb4 \
        --enable-gssapi \
        --disable-cram \
        --disable-digest \
        --disable-ldapdb \
        --disable-ntlm \
        --disable-otp \
        --disable-anon \
        --disable-srp \
        --disable-sql \
        --disable-login \
        --with-gss_impl=heimdal \
        --with-devrandom=/dev/urandom \
        --with-ldap=/usr \
        --with-saslauthd=/var/run/saslauthd \
        --with-dblib=none \
        --mandir=/usr/share/man \
 && make -j1 \
 && make -j1 install \
# Clean up build-time packages
&& apk del --purge ${BUILD_DEPS} \
# Clean up anything else
&& rm -fr \
    /tmp/* \
    /var/tmp/* \
    /var/cache/apk/*

FROM golang:1.13.0-alpine3.10 AS algorun-go-buildenv

# The default librdkafka version is the latest stable release on GitHub. You can
# use the `--build-arg` argument for `docker build` to specify a different
# version to be installed.
#
# e.g.: docker build --build-arg LIBRDKAFKA_VERSION=4e7a46701ecce7297b2298885da980be7856e5f9
#
ARG LIBRDKAFKA_VERSION=v1.1.0

# Set the workdir to the full GOPATH of your project.
WORKDIR $GOPATH/src/algo-runner-go

# Install all dependencies required to build the project as a static binary.
RUN apk add -U \
    bash \
    build-base \
    coreutils \
    curl \
    git \
    libevent \
    lz4-dev \
    cyrus-sasl-dev \
    openssh \
    openssl \
    openssl-dev \
    krb5-dev \
    python \
    yajl-dev \
    zlib-dev

# Install `dep`, the official Golang package manager to install dependencies.
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

# Copy the sasl static build files from the previous step
COPY --from=sasl /usr/lib/libsasl2.a /usr/lib/libsasl2.a
COPY --from=sasl /usr/lib/sasl2/ /usr/lib/sasl2/

# Build librdkafka
RUN cd $(mktemp -d) \
 && curl -sL "https://github.com/edenhill/librdkafka/archive/$LIBRDKAFKA_VERSION.tar.gz" | \
    tar -xz --strip-components=1 -f - \
 && ./configure \
 && make -j \
 && make install

# Build a completely static binary, able to be used in a `scratch` container.
# RUN go build -o /tmp/algo-runner-go -tags static_all


FROM algorun-go-buildenv as static-build

RUN mkdir -p /go/src/algo-runner-go
WORKDIR /go/src/algo-runner-go
COPY . /go/src/algo-runner-go
RUN CGO_ENABLED=1 GOOS=linux go build -tags static_all -ldflags "${ldflags}" -a -installsuffix cgo -o algo-runner-go .

FROM minio/mc as mc

# Create the scratch container that only contains the algo-runner binary
FROM busybox as final

COPY --from=mc /usr/bin/mc /algo-runner/mc
COPY --from=static-build /go/src/algo-runner-go/algo-runner-go /algo-runner/algo-runner