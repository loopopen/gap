module examples/rabbitmq-mysql-example

go 1.24.0

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/loopopen/gap/broker/xrabbitmq/v2 v2.0.0
	github.com/loopopen/gap/storage/xmysql/v2 v2.0.0
	github.com/loopopen/gap/v2 v2.0.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/loopopen/shoot v0.9.0 // indirect
	github.com/rabbitmq/amqp091-go v1.15.0 // indirect
)

replace github.com/loopopen/gap/v2 => ../../../gap

replace github.com/loopopen/gap/broker/xrabbitmq/v2 => ../../broker/xrabbitmq

replace github.com/loopopen/gap/storage/xmysql/v2 => ../../storage/xmysql
