FROM golang:1-alpine as build

ENV IMPORTPATH=github.com/codeformuenster/mosmix-processor

WORKDIR /go/src/${IMPORTPATH}

COPY . ./

ENV CGO_ENABLED=0 GOOS=linux

RUN go build -a -installsuffix cgo -tags netgo -ldflags='-s -w -extldflags -static' -o /mosmix-processor cmd/mosmix-processor/main.go
RUN go build -a -installsuffix cgo -tags netgo -ldflags='-s -w -extldflags -static' -o /mosmix-check cmd/mosmix-check/main.go

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /mosmix-processor /mosmix-processor
COPY --from=build /mosmix-check /mosmix-check

VOLUME /tmp

ENTRYPOINT ["/mosmix-processor"]
