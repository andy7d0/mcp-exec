# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /mcp-executor .

# Replace this with your actual toolbox image.
FROM alpine:3.21 
#your-toolbox-image

RUN apk add --no-cache tini bash procps coreutils

COPY --from=build /mcp-executor /usr/local/bin/mcp-executor

#ENTRYPOINT ["/usr/local/bin/mcp-executor"]
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/mcp-executor"]
