FROM golang:alpine as build

WORKDIR /app
COPY . .

RUN apk add --no-cache musl-dev pkgconfig alpine-sdk zeromq-dev libzmq-static libsodium-static
RUN CGO_LDFLAGS="$CGO_LDFLAGS -lstdc++ -lm -lzmq -lsodium" \
  CGO_ENABLED=1 \
  GOOS=linux \
  go build -v -a --ldflags '-extldflags "-static" -v'

FROM scratch AS export
COPY --from=build /app/scaled-scheduler-go .
