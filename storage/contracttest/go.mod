module github.com/loopopen/gap/storage/contracttest

go 1.24.0

require (
	github.com/loopopen/gap v1.0.0
	github.com/loopopen/gap/broker/xrabbitmq v1.0.0-beta.7
	github.com/loopopen/gap/storage/xgorm v1.0.0
	github.com/loopopen/gap/storage/xmysql v1.0.0
	github.com/loopopen/gap/storage/xpostgres v1.0.0
	gorm.io/driver/mysql v1.6.0
	gorm.io/driver/postgres v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.4 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/loopopen/shoot v1.1.0 // indirect
	github.com/rabbitmq/amqp091-go v1.15.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace github.com/loopopen/gap => ../..

replace github.com/loopopen/gap/broker/xrabbitmq => ../../broker/xrabbitmq

replace github.com/loopopen/gap/storage/xgorm => ../xgorm

replace github.com/loopopen/gap/storage/xmysql => ../xmysql

replace github.com/loopopen/gap/storage/xpostgres => ../xpostgres
