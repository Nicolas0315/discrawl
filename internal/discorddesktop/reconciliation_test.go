package discorddesktop

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/store"
)

func TestReconciliationQueueDeduplicatesAndStoresMetadataOnly(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC)
	receipt, err := reconcileQueue(ctx, s, unresolvedMessages{
		"333333333333333346": "111111111111111121",
	}, nil, now)
	require.NoError(t, err)
	require.Equal(t, ReconciliationReceipt{Queued: 1, Attempted: 1, Resolved: 0, Remaining: 1}, receipt)

	receipt, err = reconcileQueue(ctx, s, unresolvedMessages{
		"333333333333333346": "111111111111111121",
	}, nil, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, ReconciliationReceipt{Queued: 0, Attempted: 1, Resolved: 0, Remaining: 1}, receipt)

	raw, err := s.GetSyncState(ctx, wiretapReconciliationQueueScope)
	require.NoError(t, err)
	require.NotContains(t, raw, "payload")
	require.NotContains(t, raw, "content")
	require.NotContains(t, raw, "Cache_Data")
	var queue reconciliationQueue
	require.NoError(t, json.Unmarshal([]byte(raw), &queue))
	require.Len(t, queue.Items, 1)
	require.Equal(t, 2, queue.Items["333333333333333346"].Attempts)

	receipt, err = reconcileQueue(ctx, s, nil, map[string]struct{}{
		"333333333333333346": {},
	}, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, ReconciliationReceipt{Queued: 0, Attempted: 0, Resolved: 1, Remaining: 0}, receipt)

	raw, err = s.GetSyncState(ctx, wiretapReconciliationReceiptScope)
	require.NoError(t, err)
	require.NotContains(t, raw, "333333333333333346")
	require.NotContains(t, raw, "111111111111111121")
}

func TestReconciliationQueueDoesNotCountSameRunResolutionAsQueued(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	receipt, err := reconcileQueue(ctx, s,
		unresolvedMessages{"333333333333333346": "111111111111111121"},
		map[string]struct{}{"333333333333333346": {}},
		time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC),
	)
	require.NoError(t, err)
	require.Equal(t, ReconciliationReceipt{}, receipt)
}
