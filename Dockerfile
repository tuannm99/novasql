# syntax=docker/dockerfile:1.7

############################
# Builder
############################
FROM golang:1.25 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/novasql-server ./cmd/server

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/novasql-client ./cmd/client


############################
# Runtime
############################
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

# binaries
COPY --from=builder /out/novasql-server /app/novasql-server
COPY --from=builder /out/novasql-client /app/novasql-client

# default config (override by mounting)
COPY novasql.yaml /app/novasql.yaml

# data dir inside container
VOLUME ["/data"]

# server uses tcp
EXPOSE 8866

# env defaults (override at runtime)
ENV NOVASQL_ADDR=0.0.0.0:8866
ENV NOVASQL_WORKDIR=/data

# IMPORTANT:
# cmd/server currently accepts: -config <path>
# and should read workdir from env or config depending on your current implementation.
ENTRYPOINT ["/app/novasql-server"]
CMD ["-config", "/app/novasql.yaml"]

