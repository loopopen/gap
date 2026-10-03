module examples/fiber-rabbitmq-mysql-example

go 1.25.0

require (
	github.com/gofiber/fiber/v2 v2.52.12
	github.com/loopopen/gap v0.2.0-alpha.1
	github.com/loopopen/gap/broker/xrabbitmq v0.0.0
	github.com/loopopen/gap/storage/xmysql v0.0.0
	golang.org/x/sync v0.21.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/loopopen/shoot v0.9.0-beta.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/rabbitmq/amqp091-go v1.13.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.51.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)

replace github.com/loopopen/gap => ../../../gap

replace github.com/loopopen/gap/storage/xmysql => ../../../gap/storage/xmysql

replace github.com/loopopen/gap/broker/xrabbitmq => ../../../gap/broker/xrabbitmq
