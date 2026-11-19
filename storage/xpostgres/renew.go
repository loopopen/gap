package xpostgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

// RenewPublishedLeases implements [storage.Storage].
func (s *Storage) RenewPublishedLeases(ctx context.Context, until time.Time, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, "published", until, leases)
}

// RenewReceivedLeases implements [storage.Storage].
func (s *Storage) RenewReceivedLeases(ctx context.Context, until time.Time, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, "received", until, leases)
}

func (s *Storage) renewLeases(ctx context.Context, base string, until time.Time, leases []storage.Lease) ([]uint, error) {
	leases = ownedLeases(leases)
	if len(leases) == 0 {
		return nil, nil
	}
	table, err := s.quotedTable(base)
	if err != nil {
		return nil, err
	}
	statusType, err := s.statusType()
	if err != nil {
		return nil, err
	}
	query, args := renewLeaseSQL(table, statusType, until, enum.StatusPending, leases)
	rows, err := s.queryer().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	renewed := make([]uint, 0, len(leases))
	for rows.Next() {
		var id uint
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		renewed = append(renewed, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return renewed, nil
}

func renewLeaseSQL(table, statusType string, until time.Time, status enum.Status, leases []storage.Lease) (string, []any) {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(table)
	b.WriteString(` AS m SET "lease_until" = $1 FROM (VALUES `)
	args := make([]any, 0, len(leases)*2+2)
	args = append(args, until)
	for i, lease := range leases {
		if i > 0 {
			b.WriteString(", ")
		}
		idPos := i*2 + 2
		fmt.Fprintf(&b, "($%d::bigint, $%d::varchar)", idPos, idPos+1)
		args = append(args, lease.ID, lease.LeaseToken)
	}
	statusPos := len(leases)*2 + 2
	fmt.Fprintf(&b, `) AS v(id, token) WHERE m."id" = v.id AND m."lease_token" = v.token AND m."status" = $%d::%s RETURNING m."id"`, statusPos, statusType)
	args = append(args, status)
	return b.String(), args
}

func ownedLeases(leases []storage.Lease) []storage.Lease {
	owned := make([]storage.Lease, 0, len(leases))
	for _, lease := range leases {
		if lease.ID == 0 || lease.LeaseToken == "" {
			continue
		}
		owned = append(owned, lease)
	}
	return owned
}
