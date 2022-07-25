FROM golang:1.17-alpine as go-builder

RUN apk update && apk upgrade && \
    apk add --no-cache ca-certificates git mercurial

ARG PROJECT_NAME=seaweedfs
ARG BUILD_PATH=/src
ARG OUTPUT_PATH=/src/bin

WORKDIR /src

COPY go.mod go.sum ./
COPY weed.go ./
RUN go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct && go mod download

COPY command ./ glog ./  operation ./ pb ./ security ./ sequence ./ server ./ stats ./ storage ./ tools ./ topology ./ util ./

RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ${OUTPUT_PATH}/${PROJECT_NAME} $BUILD_PATH

# =============================================================================
FROM alpine:3.9 AS final

ARG PROJECT_NAME=seaweedfs
ARG OUTPUT_PATH=/src/bin

COPY --from=go-builder ${OUTPUT_PATH}/${PROJECT_NAME} /usr/local/bin/${PROJECT_NAME}
COPY --from=go-builder ${KEY_PATH} /usr/local/lib/jwt-private.key

RUN adduser -D ${PROJECT_NAME}
USER ${PROJECT_NAME}
USER root

# ENTRYPOINT ["/usr/local/bin/zk-cluster-operator -c=/cloud-md-zk-config/config.yaml"]
