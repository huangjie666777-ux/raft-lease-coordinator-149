# raft-lease-coordinator

三节点租约锁后端：每节点一个独立进程与数据目录，基于 HashiCorp Raft 1.7.3
做真实 TCP 复制，HTTP 提供租约锁 API。无前端、无动态成员管理。

## 架构与职责划分

| 文件 | 职责 |
| --- | --- |
| `internal/lease/fsm.go` | 租约状态机：acquire/renew/release 判定、防护令牌（fencing token）高水位、快照与恢复 |
| `internal/lease/node.go` | Raft 节点：TCP 传输、Bolt 日志/稳定存储、文件快照、仅首次引导、命令提案与提交超时 |
| `internal/lease/http.go` | HTTP API：请求校验、leader 判定与 leader 地址回传、查询 |
| `cmd/lockd/main.go` | 进程入口：配置解析、启动、信号处理与网络/存储关闭 |

关键设计：

- **判定时刻由 leader 写入命令**：leader 在提案时把 `now` 盖进 `Command.NowNanos`，
  副本重放只使用该值，从不自行读时钟；所有操作按已提交日志顺序在 FSM 中判定。
- **防护令牌**：每资源单调递增，`tokens[resource]` 高水位独立保存，
  释放/到期/删除锁都不会清除；快照同时包含当前租约与令牌高水位。
- **仅首次初始化**：数据目录已有 Raft 状态（`HasExistingState`）时直接恢复，
  绝不重新 `BootstrapCluster`；三名投票成员为静态配置。
- **leader 处理写操作**：非 leader 返回 `{"error":"not leader","leader":"<http addr>"}`；
  多数派提交并应用后才响应；提交超时返回 `result unconfirmed`（结果未确认，
  客户端应查询后再决定重试）。多数派丢失时 leader 会退位并拒绝授锁。
- **持久化**：日志、任期、投票存于 `raft.db`（BoltDB），快照存于数据目录。

## 构建与测试

```sh
go build -o bin/lockd ./cmd/lockd
go test ./...
```

## 启动三节点（同机）

```sh
PEERS="node1=127.0.0.1:7201@127.0.0.1:8201,node2=127.0.0.1:7202@127.0.0.1:8202,node3=127.0.0.1:7203@127.0.0.1:8203"
./bin/lockd -id node1 -raft-addr 127.0.0.1:7201 -http-addr 127.0.0.1:8201 -data-dir build/data/node1 -peers "$PEERS" &
./bin/lockd -id node2 -raft-addr 127.0.0.1:7202 -http-addr 127.0.0.1:8202 -data-dir build/data/node2 -peers "$PEERS" &
./bin/lockd -id node3 -raft-addr 127.0.0.1:7203 -http-addr 127.0.0.1:8203 -data-dir build/data/node3 -peers "$PEERS" &
```

`-peers` 格式：`id=raftAddr@httpAddr` 逗号分隔，必须恰好是三名投票成员且包含自身 ID。
重启同一数据目录不会重新引导。

## HTTP API

- `POST /acquire` `{"resource":"orders","holder":"alice","ttl_seconds":30}`
  TTL 取值 1–300 秒；无有效租约才成功，返回 `{"ok":true,"holder","token","expires_at_nanos"}`。
- `POST /renew` `{"resource","holder","token","ttl_seconds"}`
  持有人与令牌必须匹配且租约未过期；截止时间从判定时刻重算。
- `POST /release` `{"resource","holder","token"}` 匹配才释放；令牌高水位保留。
- `GET /lock?resource=orders` 查询；到期锁返回 `{"held":false,...}`，
  并附 `fencing_token_bound`（该资源历史令牌上界）。

错误：409 表示锁冲突/非 leader（带 `leader` 地址）；503 表示提交结果未确认。

## 演示过的行为

- 8 路并发争用同一资源，仅一方获得令牌，其余收到持有者冲突错误。
- 错误令牌续租/释放被拒；过期持有者的请求不影响新持有人。
- 释放或到期后重取获得更大令牌；重启与快照恢复后令牌上界不丢。
- 杀掉 leader 后剩余两节点选出新 leader 继续服务；不足多数派时拒绝授锁。
