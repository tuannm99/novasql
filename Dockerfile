# syntax=docker/dockerfile:1.7

############################
# Builder
############################
FROM golang:1.25 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/novasql-server ./cmd/server

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/novasql-client ./cmd/client

# Prepare /data with correct owner for distroless nonroot (uid 65532)
RUN mkdir -p /out/data && chown -R 65532:65532 /out/data


############################
# Runtime
############################
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /out/novasql-server /app/novasql-server
COPY --from=builder --chown=nonroot:nonroot /out/novasql-client /app/novasql-client

# ensure /data exists and writable for nonroot
COPY --from=builder /out/data /data

# default config (override by mounting)
COPY --chown=nonroot:nonroot novasql.yaml /app/novasql.yaml

VOLUME ["/data"]
EXPOSE 8866

ENV NOVASQL_ADDR=0.0.0.0:8866
ENV NOVASQL_WORKDIR=/data

ENTRYPOINT ["/app/novasql-server"]
CMD ["-config", "/app/novasql.yaml"]

