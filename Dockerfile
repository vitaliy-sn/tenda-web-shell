# ixdx/tendashell:latest
FROM golang:1.27-trixie AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o tendashell .


FROM debian:bookworm
COPY --from=builder /app/tendashell /app/tendashell
CMD ["/app/tendashell"]
