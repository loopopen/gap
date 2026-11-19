# TODO：v1.0 发布清单

目标：在创建 `v1.0.0` tag 前，冻结 GAP v1 的公开 API、持久化 schema、消息状态语义和 Runtime 生命周期。所有“v1.0 阻断项”和“发布门禁”完成前，不发布正式版。

当前仍处于 beta 阶段；阻断项中的接口和表结构调整直接进入 v1 初始基线，不为 beta 版本保留 migration。v1 发布后，公开 API、消息状态和数据库结构的变更必须提供向后兼容或迁移方案。

## 当前已完成的基线

- [x] **Storage v1 主体接口和持久化模型**

  - [x] 将 broker name 固化为可索引的稳定路由键；broker type 仅作为消息元数据，Claim 不依赖 JSON headers。
  - [x] 为租约续期、按路由批量 Claim、终态保留和分批清理定义 Storage 方法。
  - [x] MySQL、PostgreSQL 和 GORM 使用同一套状态、租约、路由和清理语义。
  - [x] 固化 broker name、lease token、lease deadline、next attempt、expired deadline 及 Claim/Cleanup 索引。
  - [x] `ExpiredAt` 作为终态清理截止时间持久化，并可在 Model/Envelope 之间映射。

- [x] **内置 Storage 统一契约测试**

  - [x] 共享同一套 Harness，覆盖 Published/Received 路由、租约续期、token fencing、重试耗尽、终态清理和事务提交/回滚。
  - [x] 契约测试运行于真实 MySQL、PostgreSQL、GORM/MySQL 和 GORM/PostgreSQL。
  - [x] 每个用例使用独立 schema 或表前缀，测试结束后删除数据库容器和临时数据。
  - [x] `make test-storage` 可在本地运行，GitHub Actions 已接入该命令。

- [x] **租约和路由的基础能力**

  - [x] 为长时间运行的 Producer/Handler 续租。
  - [x] 使用数据库时间计算持久化租约和过期时间。
  - [x] 保持 `LeaseToken` fencing，旧租约持有者不能覆盖新租约结果。
  - [x] Published 按 broker name Claim；Received 按 consumer group 和 broker name Claim。
  - [x] 不仅因当前实例缺少 Sender/Handler 就将消息写入终态 `Failed`。
  - [x] 文档明确 at-least-once 语义、Handler 幂等要求以及基础保留/清理行为。

## v1.0 阻断项

### 1. 让租约完整覆盖真实执行过程

目标：事务等待、内存队列、并发信号量和同批次前序消息不能消耗掉任务的有效执行租约。

- [ ] 在任务获得真正执行资源前重新验证租约，必要时领取或刷新租约。
- [ ] 确保消息在队列、并发信号量和已领取批次中等待时仍持续续租。
- [ ] 让 Claim 数量与可立即执行的并发容量匹配，或并发消费已领取批次。
- [ ] 删除 Published/Received 单条执行失败后的固定 10 秒等待，仅对 Claim 失败做可取消退避。
- [ ] 让事务内发布消息的有效租约从提交/可调度时开始，或在事务提交后立即刷新。
- [ ] 明确续租失败和执行中丢失租约时的停止、重试和状态更新行为。

验收：

- [ ] 长事务、队列积压、长 Handler 和最后一次重试不会导致同一消息并发执行或被提前标记为 `Failed`。
- [ ] 增加续租、过期重领、执行中失去租约、旧 token fencing、队列积压和长事务的并发测试。

### 2. 完成多实例路由语义

- [ ] 明确“当前实例暂未注册路由”和“路由在整个部署中不存在”的不同处理方式。
- [ ] 确保滚动发布、模块化部署和同 group 不同 broker 的实例只领取自己能处理的消息。
- [ ] 增加 Published、Received、同 group 不同 broker、路由动态注册和滚动发布的多实例集成测试。

### 3. 冻结 Runtime、WorkerID 和连接池生命周期

- [ ] 明确 Snowflake generator 是 Runtime 级还是进程级。
- [ ] 如果保持进程级单例，后续 Runtime 指定不同 `WorkerID` 时必须明确报错，不能由 `sync.Once` 静默忽略。
- [ ] 将驱动/Dialector 身份以及影响连接、命名和 schema 的配置纳入连接池缓存 key。
- [ ] 区分 GAP 创建的连接池和调用方注入的 `*sql.DB`/`*gorm.DB`。
- [ ] 为共享池增加引用计数或等价的租借/释放机制。
- [ ] Runtime 关闭时幂等释放自己的池引用；最后一个持有者关闭 GAP 创建的池。
- [ ] 调用方传入的 `*sql.DB`/`*gorm.DB` 在 Runtime 关闭后仍可用。

验收：

- [ ] 多 Runtime 使用不同 WorkerID 时不会产生静默且不可预测的行为。
- [ ] 相同 DSN、不同 Dialector/config 不会错误共享连接池。
- [ ] 一个 Runtime 关闭不会中断其他 Runtime，最后一个持有者关闭后不泄漏连接池。

### 4. 完成 Storage schema 和导出 API 的 v1 收尾审核

- [ ] 在真实 MySQL/PostgreSQL 上断言 GORM 生成的最终列类型、可空性、默认值和索引与原生 Storage 一致。
- [ ] 明确 GORM `Payload` 的 MySQL `LONGTEXT` / PostgreSQL `TEXT` 语义，防止自定义 Dialector `DefaultStringSize` 将其降级为 `VARCHAR`。
- [ ] 在 Storage 契约测试中增加大 Payload 写入以及 `ExpiredAt` 终态回读断言。
- [ ] 审核所有导出 API 和导出持久化类型，删除不准备在 v1 长期维护的入口。
- [ ] 确定 `ExpiredAt` 是否通过 Dashboard/API 对外展示；如果展示，补齐 DTO 和 API 测试。

## v1.0 发布门禁

### 5. 完成关键集成、竞态和泄漏验证

- [ ] 使用真实 MySQL、PostgreSQL 和至少一种 GORM Dialector 验证完整发布、消费、重试、清理和关闭流程；不仅验证 Storage 方法。
- [ ] 使用至少一种真实 Broker 验证生产、消费、失败重试和优雅关闭。
- [ ] 增加租约、路由隔离、连接池所有权和多 Runtime WorkerID 的集成测试。
- [ ] 对核心 runtime、pump、storage 和 RabbitMQ 连接池运行 `go test -race`。
- [ ] 检查关键启动/关闭、续租、Broker 断线和测试失败路径没有 goroutine 或数据库连接泄漏。
- [x] CI 已配置执行 `make test-storage`。
- [ ] 当前候选提交的 CI、race 和真实依赖测试全部通过。

### 6. 固化并记录 v1 兼容性承诺

- [x] 文档明确 GAP 采用 at-least-once 语义，Handler 必须幂等。
- [ ] 文档明确支持的多实例部署和路由模型。
- [ ] 文档明确租约、续租、重试、终态、保留和清理语义。
- [ ] 文档明确数据库 schema 升级策略以及 v1 后的 migration 责任。
- [ ] 文档明确根模块与 Broker/Storage 子模块的版本兼容策略。
- [ ] 为准备长期维护的导出 API 补全稳定性说明。

### 7. 加固 release 流程

- [ ] release 仅允许从 `main` 执行，并确认本地 `main` 与 `origin/main` 一致。
- [ ] 发布开始前记录原始 HEAD、当前分支和已有 tags。
- [ ] 失败时输出可验证的恢复命令，或安全回滚本地 release commit/tags。
- [ ] 按 SemVer 收紧版本校验，拒绝 `v1.0.0-01` 等非法版本，并增加边界测试。
- [ ] 增加不创建 commit、tag 或远端变更的 release dry run。
- [x] 保持远端 atomic push，根模块和所有 Broker/Storage 子模块 tags 一次性发布。

### 8. 执行最终 v1.0 发布检查

- [ ] 所有变更已审核、提交并合并到干净的 `main`，且与 `origin/main` 一致。
- [ ] `make generate` 后工作区无差异。
- [ ] `make tidy` 后工作区无差异。
- [ ] `make test-isolated` 全部通过。
- [ ] `make test-storage` 全部通过。
- [ ] CI、race、真实 Broker/Storage 集成测试和泄漏检查全部通过。
- [ ] 更新 README、README.zh-cn、CHANGELOG 和升级说明。
- [ ] 将根模块和所有 Broker/Storage 子模块版本统一更新为 `v1.0.0`。
- [ ] 执行 release dry run，核对 release commit、模块依赖、分支和全部 tags。
- [ ] 创建并 atomic push `v1.0.0` 及所有 Broker/Storage 子模块 tags。

## Post-v1：不阻断首次正式发布

- [ ] 扩大真实 Broker/Storage 组合测试矩阵。
- [ ] 增加 RabbitMQ 连接池长时间并发压力测试。
- [ ] 持续补充故障注入、性能基准和 goroutine/连接泄漏回归测试。
- [ ] 评估是否在 schema 中增加 lease token/deadline 成对以及 status/expired deadline 一致性约束。
- [ ] 根据用户反馈增加可观测性指标和运维诊断能力。
