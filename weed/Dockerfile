FROM golang:1.17-alpine as go-builder

RUN apk update && apk upgrade && \
    apk add --no-cache ca-certificates git mercurial

ARG PROJECT_NAME=weed
ARG BUILD_PATH=/src/main
ARG OUTPUT_PATH=/src/bin

WORKDIR /src

COPY go.mod go.sum ./

RUN go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct && go mod download
ADD weed.go /src/main/
COPY command ./ glog ./  operation ./ pb ./ security ./ sequence ./ server ./ stats ./ storage ./ tools ./ topology ./ util ./

RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ${OUTPUT_PATH}/${PROJECT_NAME} $BUILD_PATH

# =============================================================================
FROM alpine:3.9 AS final

ARG PROJECT_NAME=weed
ARG OUTPUT_PATH=/src/bin

COPY --from=go-builder ${OUTPUT_PATH}/${PROJECT_NAME} /usr/local/bin/${PROJECT_NAME}

RUN adduser -D ${PROJECT_NAME}
USER ${PROJECT_NAME}

COPY entrypoint.sh /entrypoint.sh

USER root

# ENTRYPOINT ["/usr/local/bin/zk-cluster-operator -c=/cloud-md-zk-config/config.yaml"]
