module examples/kafka-gorm-postgres-example

go 1.25.0

require (
	github.com/loopopen/gap v0.2.0-alpha.1
	github.com/loopopen/gap/broker/xkafka v0.0.0
	github.com/loopopen/gap/storage/xgorm v0.0.0
	gorm.io/driver/postgres v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.6.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/loopopen/shoot v0.9.0-beta.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/segmentio/kafka-go v0.4.50 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/text v0.39.0 // indirect
)

replace github.com/loopopen/gap => ../../../gap

replace github.com/loopopen/gap/storage/xgorm => ../../../gap/storage/xgorm

replace github.com/loopopen/gap/broker/xkafka => ../../../gap/broker/xkafka
