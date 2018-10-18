FROM algorun-go-buildenv

ARG VERSION
ARG GIT_COMMIT

RUN mkdir -p /go/src/algo-runner-go
WORKDIR /go/src/algo-runner-go

COPY . /go/src/algo-runner-go

RUN dep ensure

# Stripping via -ldflags "-s -w" 
RUN CGO_ENABLED=1 GOOS=linux go build -tags static_all -ldflags "${ldflags}" -a -installsuffix cgo -o algo-runner-go .
# RUN GOARCH=arm64 CGO_ENABLED=1 GOOS=linux go build -tags static_all -a \
#        -installsuffix cgo -o algo-runner-go-arm64 .
# RUN GOOS=windows CGO_ENABLED=1 go build -tags static_all -a \
#        -installsuffix cgo -o algo-runner-go.exe .
# RUN GOARM=7 GOARCH=arm CC=arm-linux-gnueabi-gcc CGO_ENABLED=1 GOOS=linux go build -tags static_all -a \
#    -installsuffix cgo -o algo-runner-go-armhf .