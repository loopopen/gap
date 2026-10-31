module examples/rabbitmq-mysql-example

go 1.24.0

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/loopopen/gap v1.0.0
	github.com/loopopen/gap/broker/xrabbitmq v1.0.0
	github.com/loopopen/gap/storage/xmysql v1.0.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/loopopen/shoot v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.15.0 // indirect
)

replace github.com/loopopen/gap => ../..

replace github.com/loopopen/gap/broker/xrabbitmq => ../../broker/xrabbitmq

replace github.com/loopopen/gap/storage/xmysql => ../../storage/xmysql
