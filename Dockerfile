FROM golang:1-alpine as build

ENV IMPORTPATH=github.com/codeformuenster/mosmix-processor

RUN apk --repository http://nl.alpinelinux.org/alpine/edge/testing --no-cache add libspatialite-dev build-base

WORKDIR /go/src/${IMPORTPATH}

COPY . ./

RUN go install ${IMPORTPATH}/cmd/mosmix-processor

FROM alpine:3.7

RUN apk --repository http://nl.alpinelinux.org/alpine/edge/testing --no-cache add libspatialite-dev ca-certificates

COPY --from=build /go/bin/mosmix-processor /usr/bin/

CMD ["/usr/bin/mosmix-processor"]
