#!/usr/bin/env bash
#
# 本机压测脚本（不依赖 docker）：启动原生 memcached，跑 mc-bench，
# 结束后自动清理。没有容器端口转发和平台模拟开销，数字反映 SDK 真实水平。
#
# 模式（MODE 环境变量）：
#   direct   （默认）SDK direct::DirectClient 直连 memcached
#   sidecar  用假 sock 文件模拟 mesh 发布，走完整 sidecar 链路
#   replay   memcache::replay 单连接客户端（仅 GET）
#   shards   N 个原生 memcached 当分片，走 direct::Shards 客户端路由
#   service  master + slave_l1 + slave 三实例，走 direct::HaClient
#            主从拓扑客户端（写 master、读逐级回退；WRITE_SLAVE=1 双写）
#   service-yaml
#            从 cache-service YAML 构建拓扑（生产路径：
#            CacheServiceConfig → HaConfig::from_namespace → HaClient）；
#            默认用 tests/fixtures/cache_service_local.yaml（test.local：
#            master=MC_PORT, master_l1=MC_PORT+1, slave=MC_PORT+2,
#            slave_l1=MC_PORT+3，共 4 实例）
#
# 用法示例：
#
#   基本：
#     ./bench_local.sh --ops 1000000 get                      # direct 模式
#     ./bench_local.sh -c 64 -d 60 getmulti                   # 60 秒时长模式，4 key 多取
#     MODE=sidecar ./bench_local.sh --ops 1000000 get         # sidecar（假 sock 文件）
#     MODE=replay  ./bench_local.sh --ops 100000              # replay（GET）
#     MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 get # N 个本机 memcached 分片
#     MODE=service ./bench_local.sh --ops 1000000 get         # master/slave 拓扑
#     MODE=service WRITE_SLAVE=1 ./bench_local.sh --ops 1000000 get  # 双写 slave 层
#     MODE=service-yaml ./bench_local.sh --ops 1000000 get    # YAML 拓扑（test.local）
#     MODE=service-yaml NS=test.sharded WRITE_SLAVE=1 ./bench_local.sh --ops 1000000 get
#
#   故障注入（本地代理按帧注入）：
#     ./bench_local.sh --ops 1000000 get --slow-rate 0.0001 --slow-ms 200
#         # 万分之一请求慢 200ms（尾延迟）
#     ./bench_local.sh --ops 1000000 get --timeout-rate 0.0001 --timeout-ms 5000
#         # 万分之一请求挂起 5s，超过 SDK op_timeout(400ms)，走超时路径
#     ./bench_local.sh --ops 1000000 get --reset-rate 0.0005
#         # 万分之五连接被重置（断连重连 / opaque 失序检测路径）
#     ./bench_local.sh -d 40 get --outage-ms 3000 --outage-interval-ms 20000
#         # 每 20 秒一次 3 秒完全不可用（连接池排空 + 保底重建自愈）
#     ./bench_local.sh --ops 1000000 get --cpu-stall-rate 0.002 --cpu-stall-ms 20
#         # 千分之二请求前 SDK 侧自旋 20ms（模拟 CPU 过载/GC 停顿）
#     MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 get \
#         --slow-rate 0.001 --slow-ms 100 --fault-shard 0
#         # 只慢 0 号分片（多分片下单分片故障）
#
#   大 value 与正确性：
#     ./bench_local.sh --ops 1000000 get --big-value-rate 0.1 --big-value-size 1024
#         # 10% 的 key 是 1k 大 value
#     ./bench_local.sh --ops 1000000 get --verify
#         # 逐条校验回复（种子始终写 key||padding 自描述内容，--verify 只是校验开关）
#
#   压测矩阵（一键全场景）：
#     MATRIX=1 ./bench_local.sh                 # direct 模式跑全套场景
#     MATRIX=1 MODE=service ./bench_local.sh    # 主从拓扑跑全套场景
#
# 环境变量：
#   MODE        direct | sidecar | replay | shards | service（默认 direct）
#   MATRIX      1 = 跑完整压测矩阵（默认关）
#   MC_PORT        master/单实例端口（默认 21311；shards 占用 MC_PORT..MC_PORT+SHARDS-1，
#               service 占用 MC_PORT(master)、MC_PORT+1(slave_l1)、MC_PORT+2(slave)）
#   SHARDS      分片数（默认 4，仅 shards 模式）
#   WRITE_SLAVE 1 = service 模式双写 slave 层（默认关）
#   NS          service-yaml 模式的命名空间（默认 test.local）
#   YAML        service-yaml 模式的配置文件（默认
#               ../../tests/fixtures/cache_service_local.yaml）
#   OPS         矩阵模式每场的操作数（默认 500000）
#   MEMCACHED   memcached 路径（默认 PATH 里的 memcached）
#   MEM         每个实例的内存上限 MB（默认 4096，避免大 key 池被 LRU 驱逐）
#   CARGO       cargo 路径（默认 cargo）
#   KEEP_MC=1   压测结束后保留 memcached（方便手动查 key）
#
# 压测后查看 key（需 KEEP_MC=1）：
#   printf 'get 0000000000000000kkkkkkkkkkkkkkkk\r\n' | nc 127.0.0.1 21311

set -euo pipefail
export LC_ALL=en_US.UTF-8

MODE="${MODE:-direct}"
MATRIX="${MATRIX:-0}"
MC_PORT="${MC_PORT:-21311}"
SHARDS="${SHARDS:-4}"
WRITE_SLAVE="${WRITE_SLAVE:-0}"
OPS="${OPS:-500000}"
MEMCACHED="${MEMCACHED:-memcached}"
MEM="${MEM:-4096}"
CARGO="${CARGO:-cargo}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
NS="${NS:-test.local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YAML="${YAML:-$SCRIPT_DIR/../../tests/fixtures/cache_service_local.yaml}"
# 每次运行独立的 sock 目录：旧目录里的残留 sock 文件会赢得发现、指向死端口。
SOCK_DIR="$(mktemp -d /tmp/mc-bench-socks.XXXXXX)"

started_ports=()
cleanup() {
  if [[ "${KEEP_MC:-0}" != "1" ]]; then
    for p in ${started_ports[@]+"${started_ports[@]}"}; do
      kill "$(cat "/tmp/mc-bench-local-$p.pid")" 2>/dev/null || true
      rm -f "/tmp/mc-bench-local-$p.pid"
    done
  fi
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# 启动（或复用）指定端口的 memcached。
ensure_memcached() {
  local port="$1"
  if bash -c ": > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    echo "memcached already running on 127.0.0.1:$port (leaving it as-is)"
    return
  fi
  echo "starting memcached on port $port..."
  "$MEMCACHED" -p "$port" -l 127.0.0.1 -U 0 -m "$MEM" -d \
    -P "/tmp/mc-bench-local-$port.pid"
  started_ports+=("$port")
  local ready=0
  for _ in {1..50}; do
    if bash -c ": > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 0.1
  done
  if (( ready != 1 )); then
    echo "error: memcached 未就绪（端口 $port）" >&2
    exit 1
  fi
}

# 按模式准备环境并生成目标参数（写入全局 TARGET_ARGS）。
TARGET_ARGS=()
prepare() {
  case "$MODE" in
    direct)
      ensure_memcached "$MC_PORT"
      TARGET_ARGS=(--direct "127.0.0.1:$MC_PORT")
      ;;
    sidecar)
      ensure_memcached "$MC_PORT"
      # 模拟 mesh 发布 sock 注册文件：
      #   <以 + 分隔的服务路径>+all:<namespace>@mc:<port>@cs
      local sock="$SOCK_DIR/config.example.com+3+config+v1+${GROUP}+all:${NAMESPACE}@mc:${MC_PORT}@cs"
      touch "$sock"
      echo "sidecar 模式: 已发布 sock 文件 $(basename "$sock")"
      TARGET_ARGS=(--namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR")
      ;;
    replay)
      ensure_memcached "$MC_PORT"
      TARGET_ARGS=(--replay "127.0.0.1:$MC_PORT")
      ;;
    shards)
      local addrs=""
      for i in $(seq 0 $((SHARDS - 1))); do
        local port=$((MC_PORT + i))
        ensure_memcached "$port"
        addrs="${addrs:+$addrs,}127.0.0.1:$port"
      done
      echo "shards 模式: $SHARDS 个分片 ($addrs)"
      TARGET_ARGS=(--shards "$addrs")
      ;;
    service)
      local master="$MC_PORT" l1="$((MC_PORT + 1))" slave="$((MC_PORT + 2))"
      ensure_memcached "$master"
      ensure_memcached "$l1"
      ensure_memcached "$slave"
      echo "service 模式: master=$master slave_l1=$l1 slave=$slave write_slave=$WRITE_SLAVE"
      TARGET_ARGS=(--masters "127.0.0.1:$master" --slave-l1 "127.0.0.1:$l1"
        --slaves "127.0.0.1:$slave")
      if [[ "$WRITE_SLAVE" == "1" ]]; then
        TARGET_ARGS+=(--write-slave)
      fi
      ;;
    service-yaml)
      # test.local 拓扑：master / master_l1 / slave / slave_l1 各占一个端口。
      for i in 0 1 2 3; do
        ensure_memcached "$((MC_PORT + i))"
      done
      local yaml="$YAML"
      if [[ "$MC_PORT" != "21311" ]]; then
        # 配置文件的端口按默认布局写死；端口偏移时生成一份临时副本。
        yaml="$SOCK_DIR/cache_service_local.yaml"
        sed -e "s/21311/$MC_PORT/g" \
            -e "s/21312/$((MC_PORT + 1))/g" \
            -e "s/21313/$((MC_PORT + 2))/g" \
            -e "s/21314/$((MC_PORT + 3))/g" "$YAML" > "$yaml"
      fi
      echo "service-yaml 模式: ns=$NS yaml=$yaml write_slave=$WRITE_SLAVE"
      TARGET_ARGS=(--service-yaml "$yaml" --ns "$NS")
      if [[ "$WRITE_SLAVE" == "1" ]]; then
        TARGET_ARGS+=(--write-slave)
      fi
      ;;
    *)
      echo "error: 未知 MODE '$MODE'（direct | sidecar | replay | shards | service | service-yaml）" >&2
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
