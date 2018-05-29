FROM golang:1-alpine as build

ENV IMPORTPATH=github.com/codeformuenster/mosmix-processor

WORKDIR /go/src/${IMPORTPATH}

RUN apk --no-cache add ca-certificates curl git && \
  curl -fsSL -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 && \
  chmod +x /usr/local/bin/dep

COPY Gopkg.toml Gopkg.lock ./

RUN dep ensure -vendor-only

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -tags netgo -ldflags='-s -w -extldflags -static' -o /mosmix-processor cmd/mosmix-processor/main.go

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /mosmix-processor /mosmix-processor

VOLUME /tmp

ENTRYPOINT ["/mosmix-processor"]
