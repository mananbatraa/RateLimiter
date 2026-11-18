# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ratelimiter ./cmd/ratelimiter

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/ratelimiter /app/ratelimiter
COPY config/rules.yaml /app/config/rules.yaml
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/ratelimiter"]
