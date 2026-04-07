module examples/rabbitmq-mysql-example

go 1.25.0

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/lopolopen/gap v0.1.0-beta.1
	github.com/lopolopen/gap/broker/xrabbitmq v0.0.1-alpha.1
	github.com/lopolopen/gap/storage/xmysql v0.0.1-alpha.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/lopolopen/shoot v0.7.1 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
)

replace github.com/lopolopen/gap => ../../../gap

replace github.com/lopolopen/gap/storage/xmysql => ../../../gap/storage/xmysql

replace github.com/lopolopen/gap/broker/xrabbitmq => ../../../gap/broker/xrabbitmq
