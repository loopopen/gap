module examples/rabbitmq-mysql-example

go 1.24.6

require (
	github.com/go-sql-driver/mysql v1.8.1
	github.com/lopolopen/gap v0.0.2-alpha.1
	github.com/lopolopen/gap/broker/xrabbitmq v0.0.1-alpha.1
	github.com/lopolopen/gap/storage/xmysql v0.0.1-alpha.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/lopolopen/shoot v0.7.1 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
)

// replace github.com/lopolopen/gap => ../../../gap

// replace github.com/lopolopen/gap/storage/xmysql => ../../../gap/storage/xmysql

// replace github.com/lopolopen/gap/broker/xrabbitmq => ../../../gap/broker/xrabbitmq
