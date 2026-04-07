module examples/kafka-mysql-example

go 1.25.0

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/lopolopen/gap v0.1.0-beta.1
	github.com/lopolopen/gap/broker/xkafka v0.0.1-alpha.1
	github.com/lopolopen/gap/storage/xmysql v0.0.1-alpha.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/lopolopen/shoot v0.7.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/segmentio/kafka-go v0.4.50 // indirect
	golang.org/x/text v0.35.0 // indirect
)

replace github.com/lopolopen/gap => ../../../gap

replace github.com/lopolopen/gap/storage/xmysql => ../../../gap/storage/xmysql

replace github.com/lopolopen/gap/broker/xkafka => ../../../gap/broker/xkafka
