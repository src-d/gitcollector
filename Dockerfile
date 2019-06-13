#============================
# Stage 1: build gitcollector
#============================
FROM golang:1.12.6-alpine3.9 AS builder

ENV GO111MODULE=on

COPY . /gitcollector

WORKDIR /gitcollector/cmd/gitcollector

RUN apk add --no-cache dumb-init=1.2.2-r1 git && go build -o /bin/gitcollector

#===================================================
# Stage 2: copy binary and set environment variables
#===================================================
FROM alpine:3.9.4

COPY --from=builder /bin/gitcollector /usr/bin/dumb-init /bin/

RUN apk add --no-cache ca-certificates

# volume where the repositories will be downloaded
VOLUME ["/library"]

ENV GITCOLLECTOR_LIBRARY=/library

WORKDIR /library
ENTRYPOINT ["/bin/dumb-init", "--"]
CMD ["gitcollector", "download"]
