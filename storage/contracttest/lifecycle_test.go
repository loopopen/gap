package contracttest

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap"
	"github.com/loopopen/gap/broker/xrabbitmq"
	"github.com/loopopen/gap/storage/xmysql"
)

type lifecycleMessage struct {
	Name string `json:"name"`
}

func TestContractRabbitMQPublishConsumeRetryShutdown(t *testing.T) {
	dsn := storagetestDSN(t, "GAP_TEST_MYSQL_DSN")
	url := storagetestDSN(t, "GAP_TEST_RABBITMQ_URL")
	if err := gap.SetWorkerID(7); err != nil {
		t.Fatal(err)
	}

	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	schema := "gap_rmq_life"
	_, _ = admin.Exec("DROP TABLE IF EXISTS `gap_rmq_life_published`, `gap_rmq_life_received`")
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP TABLE IF EXISTS `gap_rmq_life_published`, `gap_rmq_life_received`")
	})

	connectionsBefore := mysqlThreads(t, admin)
	goroutinesBefore := runtime.NumGoroutine()

	group := "contract-rabbit-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "contract.hello"
	g := gap.New(
		gap.ServiceName("contract-rabbit"),
		gap.PumpIntervalSeconds(1),
		gap.RetryIntervalSeconds(1),
		gap.LeaseTTLSeconds(30),
		gap.CleanupEnabled(false),
		xmysql.UseMySQL(
			xmysql.Schema(schema),
			xmysql.DSN(dsn),
		),
	)

	var calls atomic.Int32
	received := make(chan lifecycleMessage, 1)
	g.Subscribe(
		xrabbitmq.UseRabbitMQ(xrabbitmq.URL(url)),
		gap.HandleTopicWithinGroup(group, topic, func(_ context.Context, msg lifecycleMessage, _ map[string]string) error {
			if calls.Add(1) == 1 {
				return errors.New("try again")
			}
			received <- msg
			return nil
		}),
	)

	pub := gap.NewPublisher[lifecycleMessage](g, xrabbitmq.UseRabbitMQ(xrabbitmq.URL(url)))
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 10*time.Second)
	err = pub.Publish(publishCtx, topic, lifecycleMessage{Name: "ada"})
	cancelPublish()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.Name != "ada" {
			t.Fatalf("message = %+v", msg)
		}
	case <-time.After(25 * time.Second):
		t.Fatalf("message was not handled after retry, attempts = %d", calls.Load())
	}
	if calls.Load() < 2 {
		t.Fatalf("handler calls = %d, want a failed attempt and a retry", calls.Load())
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	err = g.Shutdown(shutdownCtx)
	cancelShutdown()
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool {
		return mysqlThreads(t, admin) <= connectionsBefore
	}, "GAP database connections remained after shutdown")
	waitFor(t, 5*time.Second, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= goroutinesBefore
	}, "goroutines remained after shutdown")
}

func storagetestDSN(t *testing.T, env string) string {
	t.Helper()
	if dsn := os.Getenv(env); dsn != "" {
		return dsn
	}
	if os.Getenv("GAP_STORAGE_CONTRACT") == "1" {
		t.Fatalf("%s is required when GAP_STORAGE_CONTRACT=1", env)
	}
	t.Skipf("set %s to run the broker lifecycle test", env)
	return ""
}

func mysqlThreads(t *testing.T, db *sql.DB) int {
	t.Helper()
	var name, value string
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Threads_connected'").Scan(&name, &value); err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, limit time.Duration, ready func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if ready() {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("%s\n%s", message, buf[:n])
		}
		time.Sleep(50 * time.Millisecond)
	}
}
