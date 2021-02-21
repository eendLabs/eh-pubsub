FROM golang:stretch AS build-env
ENV GO111MODULE=on

COPY go.* /go-src/
WORKDIR /go-src/
RUN go mod download
RUN go get github.com/go-delve/delve/cmd/dlv