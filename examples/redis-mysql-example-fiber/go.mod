module examples/redis-mysql-example-fiber

go 1.24.0

require (
	github.com/gofiber/fiber/v2 v2.52.12
	github.com/loopopen/gap v1.0.0
	github.com/loopopen/gap/broker/xredisstreams v1.0.0
	github.com/loopopen/gap/storage/xmysql v1.0.0
	github.com/valyala/fasthttp v1.51.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/loopopen/shoot v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/redis/go-redis/v9 v9.7.3 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
)

replace github.com/loopopen/gap => ../..

replace github.com/loopopen/gap/broker/xredisstreams => ../../broker/xredisstreams

replace github.com/loopopen/gap/storage/xmysql => ../../storage/xmysql
