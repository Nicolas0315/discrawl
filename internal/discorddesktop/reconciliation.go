package discorddesktop

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openclaw/discrawl/internal/store"
)

const (
	wiretapReconciliationQueueScope   = "wiretap:reconciliation_queue:v1"
	wiretapReconciliationReceiptScope = "wiretap:reconciliation_receipt:v1"
)

type ReconciliationReceipt struct {
	Queued    int `json:"queued"`
	Attempted int `json:"attempted"`
	Resolved  int `json:"resolved"`
	Remaining int `json:"remaining"`
}

type reconciliationItem struct {
	ChannelID   string    `json:"channel_id"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastTriedAt time.Time `json:"last_tried_at"`
	Attempts    int       `json:"attempts"`
}

type reconciliationQueue struct {
	Items map[string]reconciliationItem `json:"items"`
}

type reconciliationReceiptRecord struct {
	ReconciliationReceipt
	RecordedAt time.Time `json:"recorded_at"`
}

func reconcileQueue(
	ctx context.Context,
	st *store.Store,
	unresolved unresolvedMessages,
	imported map[string]struct{},
	now time.Time,
) (ReconciliationReceipt, error) {
	queue, err := loadReconciliationQueue(ctx, st)
	if err != nil {
		return ReconciliationReceipt{}, err
	}
	receipt := ReconciliationReceipt{}
	for messageID, channelID := range unresolved {
		item, exists := queue.Items[messageID]
		if !exists {
			item.FirstSeenAt = now.UTC()
			receipt.Queued++
		}
		item.ChannelID = channelID
		item.LastTriedAt = now.UTC()
		item.Attempts++
		queue.Items[messageID] = item
		receipt.Attempted++
	}
	for messageID := range imported {
		if _, exists := queue.Items[messageID]; !exists {
			continue
		}
		delete(queue.Items, messageID)
		receipt.Resolved++
	}
	receipt.Remaining = len(queue.Items)
	if err := saveReconciliationState(ctx, st, queue, receipt, now); err != nil {
		return ReconciliationReceipt{}, err
	}
	return receipt, nil
}

func loadReconciliationQueue(ctx context.Context, st *store.Store) (reconciliationQueue, error) {
	queue := reconciliationQueue{Items: map[string]reconciliationItem{}}
	raw, err := st.GetSyncState(ctx, wiretapReconciliationQueueScope)
	if err != nil {
		return queue, fmt.Errorf("load wiretap reconciliation queue: %w", err)
	}
	if raw == "" {
		return queue, nil
	}
	if err := json.Unmarshal([]byte(raw), &queue); err != nil {
		return queue, fmt.Errorf("decode wiretap reconciliation queue: %w", err)
	}
	if queue.Items == nil {
		queue.Items = map[string]reconciliationItem{}
	}
	return queue, nil
}

func saveReconciliationState(
	ctx context.Context,
	st *store.Store,
	queue reconciliationQueue,
	receipt ReconciliationReceipt,
	now time.Time,
) error {
	queueBody, err := json.Marshal(queue)
	if err != nil {
		return fmt.Errorf("encode wiretap reconciliation queue: %w", err)
	}
	receiptBody, err := json.Marshal(reconciliationReceiptRecord{
		ReconciliationReceipt: receipt,
		RecordedAt:            now.UTC(),
	})
	if err != nil {
		return fmt.Errorf("encode wiretap reconciliation receipt: %w", err)
	}
	if err := st.SetSyncState(ctx, wiretapReconciliationQueueScope, string(queueBody)); err != nil {
		return fmt.Errorf("save wiretap reconciliation queue: %w", err)
	}
	if err := st.SetSyncState(ctx, wiretapReconciliationReceiptScope, string(receiptBody)); err != nil {
		return fmt.Errorf("save wiretap reconciliation receipt: %w", err)
	}
	return nil
}
