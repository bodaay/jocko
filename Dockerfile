FROM golang:1.23-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make
ADD . /go/src/github.com/bodaay/jocko
WORKDIR /go/src/github.com/bodaay/jocko
RUN GOOS=linux GOARCH=amd64 make build

FROM alpine:latest
COPY --from=build-base /go/src/github.com/bodaay/jocko/cmd/jocko/jocko /usr/local/bin/jocko
EXPOSE 9092 9093 9094 9095
VOLUME "/tmp/jocko"
CMD ["jocko", "broker"]
