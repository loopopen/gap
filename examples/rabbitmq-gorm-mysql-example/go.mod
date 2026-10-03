module examples/rabbitmq-gorm-mysql-example

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/loopopen/gap v0.2.0-alpha.1
	github.com/loopopen/gap/broker/xrabbitmq v0.0.0
	github.com/loopopen/gap/storage/xgorm v0.0.0
	gorm.io/driver/mysql v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/loopopen/shoot v0.9.0-beta.1 // indirect
	github.com/rabbitmq/amqp091-go v1.13.0 // indirect
	golang.org/x/text v0.39.0 // indirect
)

replace github.com/loopopen/gap => ../../../gap

replace github.com/loopopen/gap/storage/xgorm => ../../../gap/storage/xgorm

replace github.com/loopopen/gap/broker/xrabbitmq => ../../../gap/broker/xrabbitmq
