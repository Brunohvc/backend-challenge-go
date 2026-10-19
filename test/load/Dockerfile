# syntax=docker/dockerfile:1

FROM golang:1.25.6-alpine@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS build
ENV GOTOOLCHAIN=local CGO_ENABLED=0
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -tags load -o /out/load ./test/load

FROM scratch
COPY --from=build /out/load /load
ENTRYPOINT ["/load"]
