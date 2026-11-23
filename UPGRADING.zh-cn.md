# 升级到 v1.0.0

这份说明面向已经在使用 v1.0.0-beta.7 的应用。

## 模块

同一次升级里，把核心模块和正在使用的 Broker、Storage 子模块都改到 `v1.0.0`：

```bash
go get github.com/loopopen/gap@v1.0.0
go get github.com/loopopen/gap/broker/xrabbitmq@v1.0.0
go get github.com/loopopen/gap/storage/xgorm@v1.0.0
```

把路径换成应用实际导入的集成。不同版本的核心模块和 Broker 或 Storage 子模块不能混用。

升级后重新运行 `gapc`，并提交生成的处理函数。

## 数据库

不需要迁移。v1.0.0 自带的 `CREATE TABLE` 就是 v1.0.0-beta.7 的表结构。打开存储时只执行 `CREATE TABLE IF NOT EXISTS`，不会修改已经存在的表。

v1.0.0 之后，如果某个 Storage 模块要改自己的表，由该模块带上迁移，应用在升级这个模块时执行。

## Worker ID

从 `gap.New` 里去掉 `gap.WorkerID`。进程需要固定 Snowflake 节点时，调用 `gap.SetWorkerID`：

```go
if err := gap.SetWorkerID(workerID); err != nil {
    return err
}
```

后一次成功的调用会换掉当前节点。被拒绝的调用不会替换当前节点。如果从未调用 `SetWorkerID`，第一次 `gap.New` 会选定节点。

## GORM

去掉 `xgorm.DB`。用 `xgorm.DSN` 打开存储，业务事务继续使用应用自己的 `*gorm.DB`：

```go
gap.New(
    gap.DoInTx(db),
    xgorm.UseGorm(xgorm.DSN(dsn, mysql.Open)),
    xrabbitmq.UseRabbitMQ(xrabbitmq.URL(url)),
)
```

GAP 的连接池和应用的连接池是连向同一个数据库的两套连接。

## 消费组

`gap.HandleTopic`，以及其他没有写消费组的处理函数，现在使用 `gap.DefaultGroup`，默认值是 `default`。过去以空字符串存下的接收记录，不会被这些处理函数认领。显式传入的消费组，包括 `gapc` 生成的消费组，保持原样。启动前设置 `gap.DefaultGroup`，可以把省略时使用的组名换成别的名字。
