module examples/rabbitmq-gorm-mysql-example

go 1.24.0

require (
	github.com/google/uuid v1.6.0
	github.com/loopopen/gap v1.0.0
	github.com/loopopen/gap/broker/xrabbitmq v1.0.0
	github.com/loopopen/gap/storage/xgorm v1.0.0
	gorm.io/driver/mysql v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/loopopen/shoot v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.15.0 // indirect
	golang.org/x/text v0.20.0 // indirect
)

replace github.com/loopopen/gap => ../..

replace github.com/loopopen/gap/broker/xrabbitmq => ../../broker/xrabbitmq

replace github.com/loopopen/gap/storage/xgorm => ../../storage/xgorm
