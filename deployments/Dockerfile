# fetching certificates
FROM alpine:latest as certprovider
RUN apk --no-cache add ca-certificates

# compile
FROM golang:1.11.5 as compiler
WORKDIR /go/src
COPY . . 

RUN if [ ! -f "go.mod" ]; then echo "\e[31mREPO NOT COPIED\e[0m" && exit 1; fi

ENV GO111MODULE=on
RUN go mod download
RUN make build

# prepare image with executable
FROM scratch
COPY --from=certprovider /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=compiler /go/src/build/eventagg /
ENTRYPOINT ["/eventagg"]
