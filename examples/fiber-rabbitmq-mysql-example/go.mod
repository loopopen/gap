module examples/fiber-rabbitmq-mysql-example

go 1.25.0

require (
	github.com/gofiber/fiber/v2 v2.52.12
	github.com/lopolopen/gap v0.1.0-beta.1
	github.com/lopolopen/gap/broker/xrabbitmq v0.1.0-beta.1
	golang.org/x/sync v0.20.0
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/lopolopen/shoot v0.7.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.51.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

// replace github.com/lopolopen/gap => ../../../gap

// replace github.com/lopolopen/gap/storage/xmysql => ../../../gap/storage/xmysql

// replace github.com/lopolopen/gap/broker/xrabbitmq => ../../../gap/broker/xrabbitmq
