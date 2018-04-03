FROM golang:1-alpine as build

ENV IMPORTPATH=github.com/codeformuenster/mosmix-processor
# ENV CGO_ENABLED 0

# from terranodo/spatialite-docker Dockerfile
#RUN echo "@edge http://nl.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories
#RUN echo "@edge-testing http://nl.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories
#RUN apk update
#
#RUN apk --no-cache add "libspatialite-dev@edge-testing" build-base
RUN apk --repository http://nl.alpinelinux.org/alpine/edge/testing --no-cache add libspatialite-dev build-base

WORKDIR /go/src/${IMPORTPATH}

COPY . ./

RUN go install ${IMPORTPATH}/cmd/mosmix-processor

FROM alpine:3.7

RUN apk --repository http://nl.alpinelinux.org/alpine/edge/testing --no-cache add libspatialite-dev ca-certificates

#COPY --from=build /usr/lib/mod_spatialite.so /usr/lib/mod_spatialite.so
COPY --from=build /go/bin/mosmix-processor /usr/local/bin/

CMD ["/usr/local/bin/mosmix-processor"]
