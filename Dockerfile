FROM golang:1-alpine as build

ENV IMPORTPATH=github.com/codeformuenster/mosmix-processor

WORKDIR /go/src/${IMPORTPATH}

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main cmd/mosmix-processor/main.go
RUN apk --no-cache add ca-certificates
RUN cp main /mosmix-processor

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /mosmix-processor /mosmix-processor

VOLUME /tmp

ENTRYPOINT ["/mosmix-processor"]
