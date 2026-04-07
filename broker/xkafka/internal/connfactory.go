package internal

import (
	"errors"
	"log/slog"
	"net"
	"strconv"

	"github.com/lopolopen/gap/internal/gap"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type ConnFactory struct {
	gapOpts *gap.Options
	opts    *Options
}

func NewConnFactory(opts *Options) *ConnFactory {
	return &ConnFactory{
		opts: opts,
	}
}

func (f *ConnFactory) CreaterDialer() *kafka.Dialer {
	var dialer *kafka.Dialer
	usr := f.opts.UserName
	pwd := f.opts.Password
	if usr != "" && pwd != "" {
		dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: usr,
				Password: pwd,
			},
		}
	} else {
		dialer = kafka.DefaultDialer
	}
	return dialer
}

func (f *ConnFactory) CreateConn(ctrl bool) (*kafka.Conn, error) {
	dialer := f.CreaterDialer()
	for _, broker := range f.opts.Brokers {
		conn, err := dialer.Dial("tcp", broker)
		if err != nil {
			slog.Debug("failed to dial broker",
				slog.String("broker", broker),
				slog.Any("err", err),
			)
			continue
		}
		if ctrl {
			c, _ := conn.Controller()
			conn, _ = kafka.Dial("tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
		}
		return conn, nil
	}
	return nil, errors.New("failed to dial broker")
}
