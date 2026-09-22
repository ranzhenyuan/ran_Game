# 多阶段构建（工具链锁 1.24.4，与 go.mod/go.sum 一致）。
# 构建：docker build -f deploy/Dockerfile -t rangame/server:latest .
# 运行镜像用 alpine：preStop 需要 curl 调 /admin/drain（distroless 无 shell）。
FROM golang:1.24.4-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS=-trimpath \
    GOTOOLCHAIN=local

WORKDIR /src
COPY go.mod go.sum ./
# 依赖层缓存：go.mod/go.sum 未变时不重拉
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download
COPY . .
RUN go build -ldflags "-s -w" -o /out/server ./cmd/server

FROM alpine:3.20

# curl: preStop 排水钩子；ca-certificates: WS TLS/Redis TLS；tzdata: Asia/Shanghai 日志时间
RUN apk add --no-cache curl ca-certificates tzdata && \
    addgroup -S app && adduser -S app -G app

COPY --from=builder /out/server /usr/local/bin/server

USER app
EXPOSE 7000 7001 7100 6060

ENTRYPOINT ["/usr/local/bin/server"]
CMD ["-config", "/etc/server/server.yaml"]
