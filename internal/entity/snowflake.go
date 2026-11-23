package entity

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bwmarrin/snowflake"
)

var (
	sfMu   sync.Mutex
	sfNode atomic.Pointer[snowflake.Node]
)

// InitSnowflake sets the process-wide Snowflake node.
// The latest successful call replaces the current node.
func InitSnowflake(node int64) error {
	sfMu.Lock()
	defer sfMu.Unlock()
	return initSnowflakeLocked(node)
}

// EnsureSnowflake initializes the process-wide node on the first call.
// Later calls keep that node and do not invoke resolve.
func EnsureSnowflake(resolve func() (int64, error)) error {
	if resolve == nil {
		return errors.New("gap: snowflake node resolver is nil")
	}
	sfMu.Lock()
	defer sfMu.Unlock()
	if sfNode.Load() != nil {
		return nil
	}
	node, err := resolve()
	if err != nil {
		return err
	}
	return initSnowflakeLocked(node)
}

func initSnowflakeLocked(node int64) error {
	created, err := snowflake.NewNode(node)
	if err != nil {
		return fmt.Errorf("gap: snowflake worker id %d: %w", node, err)
	}
	sfNode.Store(created)
	slog.Debug(fmt.Sprintf("snowflake picks node %d", node))
	return nil
}
