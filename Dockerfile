# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO disabled: modernc.org/sqlite is pure Go, so we can produce a static
# binary that runs in a minimal/distroless runtime image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/monitor ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app

COPY --from=build /out/monitor /app/monitor
COPY --from=build /src/web/static /app/web/static

# Cloud Run / container platforms inject PORT; default kept in sync with
# internal/config's default of 8080. DB_PATH should point at a mounted
# volume in production so history survives restarts/redeploys.
ENV PORT=8080
ENV DB_PATH=/data/monitor.db
VOLUME ["/data"]

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/app/monitor"]
