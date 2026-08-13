#!/usr/bin/env bash
#
# 容器版一键压测：启动一次性 memcached 容器，跑 mc-bench，结束后清理。
# 同时兼容 Linux 和 macOS（Docker Desktop）：显式端口映射，无 GNU 专有工具。
# 注意：Mac 上 amd64 镜像走模拟 + 端口转发，吞吐只有本机直跑的零头，
# 只适合功能验证；要真实性能数字请用 bench_local.sh（原生 memcached）。
#
# 模式（MODE 环境变量）：
#   direct   （默认）SDK direct::DirectClient 直连 memcached 容器
#   sidecar  用假 sock 文件模拟 mesh 发布，走完整 sidecar 链路
#   shards   N 个容器当分片，走 direct::Shards 客户端路由
#   service  master + slave_l1 + slave 三容器，走 direct::HaClient
#            主从拓扑客户端（WRITE_SLAVE=1 双写 slave 层）
#
# 用法示例（更多故障注入示例见 bench_local.sh 头部注释，参数完全一致）：
#
#   基本：
#     ./bench.sh --concurrency 64 --ops 1000000 get
#     ./bench.sh -c 128 -d 60 getmulti
#     MODE=sidecar ./bench.sh --ops 1000000 get
#     MODE=shards SHARDS=4 ./bench.sh --ops 1000000 get
#     MODE=service ./bench.sh --ops 1000000 get
#     MODE=service WRITE_SLAVE=1 ./bench.sh --ops 1000000 get
#
#   故障注入：
#     ./bench.sh --ops 1000000 get --slow-rate 0.0001 --slow-ms 200
#     ./bench.sh --ops 1000000 get --timeout-rate 0.0001 --timeout-ms 5000
#     ./bench.sh --ops 1000000 get --reset-rate 0.0005
#     ./bench.sh -d 40 get --outage-ms 3000 --outage-interval-ms 20000
#     ./bench.sh --ops 1000000 get --cpu-stall-rate 0.002 --cpu-stall-ms 20
#
#   大 value 与正确性：
#     ./bench.sh --ops 1000000 get --big-value-rate 0.1 --big-value-size 1024
#     ./bench.sh --ops 1000000 getmulti --verify
#
#   压测矩阵（一键全场景）：
#     MATRIX=1 ./bench.sh
#
# 环境变量：
#   MODE    direct | sidecar | shards | service（默认 direct）
#   MATRIX  1 = 跑完整压测矩阵（默认关）
#   IMAGE   memcached 镜像（默认 memcached:1.6-alpine，arm64 原生）
#   MC_PORT    容器映射到宿主机的端口（默认 21311；shards 占用 MC_PORT..MC_PORT+N-1，
#           service 占用 MC_PORT(master)、MC_PORT+1(slave_l1)、MC_PORT+2(slave)）
#   SHARDS  分片数（默认 4，仅 shards 模式）
#   WRITE_SLAVE 1 = service 模式双写 slave 层（默认关）
#   MEM     每个容器的内存上限 MB（默认 4096）
#   OPS     矩阵模式每场的操作数（默认 500000）
#   CARGO   cargo 路径（默认 cargo）

set -euo pipefail

MODE="${MODE:-direct}"
MATRIX="${MATRIX:-0}"
IMAGE="${IMAGE:-memcached:1.6-alpine}"
MC_PORT="${MC_PORT:-21311}"
SHARDS="${SHARDS:-4}"
WRITE_SLAVE="${WRITE_SLAVE:-0}"
MEM="${MEM:-4096}"
OPS="${OPS:-500000}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
CARGO="${CARGO:-cargo}"
NAME="mc-bench-$$"
# 裸 mktemp -d 是 GNU 写法；显式模板在 GNU/macOS 下都可用。
SOCK_DIR="$(mktemp -d /tmp/mc-bench-socks.XXXXXX)"

cleanup() {
  docker stop $(docker ps -q --filter "name=^${NAME}") >/dev/null 2>&1 || true
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# 启动一个容器并把端口映射到宿主机，等待 memcached 就绪。
start_memcached() {
  local port="$1" name="$2"
  echo "starting $IMAGE on port $port (container $name)..."
  docker run -d --rm --name "$name" -p "$port:$port" "$IMAGE" \
    -p "$port" -m "$MEM" -U 0 >/dev/null

  echo -n "waiting for memcached on 127.0.0.1:$port ..."
  local ready=0
  for _ in {1..50}; do
    # timeout(1) 是 GNU coreutils，macOS 没有；/dev/tcp 连接被拒会立即返回，
    # 循环次数兜底总等待。
    if bash -c ": > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      ready=1
      break
    fi
    echo -n "."
    sleep 0.1
  done
  if (( ready != 1 )); then
    echo " not ready"
    echo "error: memcached 未就绪，容器日志：" >&2
    docker logs "$name" >&2 || true
    exit 1
  fi
  echo " ready"
  echo
}

# 按模式准备环境并生成目标参数（写入全局 TARGET_ARGS）。
TARGET_ARGS=()
prepare() {
  case "$MODE" in
    direct)
      start_memcached "$MC_PORT" "$NAME"
      TARGET_ARGS=(--direct "127.0.0.1:$MC_PORT")
      ;;
    sidecar)
      start_memcached "$MC_PORT" "$NAME"
      # 模拟 mesh 发布 sock 注册文件：
      #   <以 + 分隔的服务路径>+all:<namespace>@mc:<port>@cs
      local sock="$SOCK_DIR/config.example.com+3+config+v1+${GROUP}+all:${NAMESPACE}@mc:${MC_PORT}@cs"
      touch "$sock"
      echo "sidecar 模式: 已发布 sock 文件 $(basename "$sock")"
      echo
      TARGET_ARGS=(--namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR")
      ;;
    shards)
      local addrs=""
      for i in $(seq 0 $((SHARDS - 1))); do
        local port=$((MC_PORT + i))
        start_memcached "$port" "$NAME-$i"
        addrs="${addrs:+$addrs,}127.0.0.1:$port"
      done
      echo "shards 模式: $SHARDS 个容器 ($addrs)"
      TARGET_ARGS=(--shards "$addrs")
      ;;
    service)
      local master="$MC_PORT" l1="$((MC_PORT + 1))" slave="$((MC_PORT + 2))"
      start_memcached "$master" "$NAME-master"
      start_memcached "$l1" "$NAME-slave-l1"
      start_memcached "$slave" "$NAME-slave"
      echo "service 模式: master=$master slave_l1=$l1 slave=$slave write_slave=$WRITE_SLAVE"
      TARGET_ARGS=(--masters "127.0.0.1:$master" --slave-l1 "127.0.0.1:$l1"
        --slaves "127.0.0.1:$slave")
      if [[ "$WRITE_SLAVE" == "1" ]]; then
        TARGET_ARGS+=(--write-slave)
      fi
      ;;
    *)
      echo "error: 未知 MODE '$MODE'（direct | sidecar | shards | service）" >&2
      exit 2
      ;;
  esac
}

# 跑一场：run_one <说明> <mc-bench 参数...>
run_one() {
  local title="$1"; shift
  echo
  echo "########## $title ##########"
  "$CARGO" run --release -p mc-bench -- "${TARGET_ARGS[@]}" "$@"
}

run_matrix() {
  echo "== 压测矩阵（MODE=$MODE, 每场 $OPS ops / outage 场为时长模式）=="
  run_one "基线 GET"         --ops "$OPS" get
  run_one "基线 GETMULTI"    --ops "$OPS" getmulti
  run_one "基线 SET"         --ops "$OPS" set
  run_one "大 value 10%x10k" --ops "$OPS" --big-value-rate 0.1 --big-value-size 10240 get
  run_one "偶发慢请求 0.1%x200ms" --ops "$OPS" --slow-rate 0.001 --slow-ms 200 get
  run_one "偶发超时 0.01%x5s"   --ops "$OPS" --timeout-rate 0.0001 --timeout-ms 5000 get
  run_one "偶发断连 0.05%"      --ops "$OPS" --reset-rate 0.0005 get
  run_one "SDK 侧 CPU 过载 0.2%x20ms" --ops "$OPS" --cpu-stall-rate 0.002 --cpu-stall-ms 20 get
  run_one "周期性不可用 3s/10s" -d 25 --outage-ms 3000 --outage-interval-ms 10000 get
  run_one "正确性校验 GET"      --ops "$OPS" --verify get
}

prepare

if [[ "$MATRIX" == "1" ]]; then
  run_matrix
else
  "$CARGO" run --release -p mc-bench -- "${TARGET_ARGS[@]}" "$@"
fi
