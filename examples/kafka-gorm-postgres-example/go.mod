module examples/kafka-gorm-postgres-example

go 1.24.0

require (
	github.com/loopopen/gap v1.0.0
	github.com/loopopen/gap/broker/xkafka v1.0.0
	github.com/loopopen/gap/storage/xgorm v1.0.0
	gorm.io/driver/postgres v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.6.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/loopopen/shoot v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/segmentio/kafka-go v0.4.50 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.23.0 // indirect
)

replace github.com/loopopen/gap => ../..

replace github.com/loopopen/gap/broker/xkafka => ../../broker/xkafka

replace github.com/loopopen/gap/storage/xgorm => ../../storage/xgorm
