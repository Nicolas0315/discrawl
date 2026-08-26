package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/discrawl/internal/discorddesktop"
)

func TestPrintHumanWiretapIncludesReconciliationReceipt(t *testing.T) {
	var out bytes.Buffer
	err := printHuman(&out, discorddesktop.Stats{
		Reconciliation: discorddesktop.ReconciliationReceipt{
			Queued:    2,
			Attempted: 870,
			Resolved:  185,
			Remaining: 685,
		},
	})
	require.NoError(t, err)
	require.Contains(t, out.String(), "reconciliation_queued=2\n")
	require.Contains(t, out.String(), "reconciliation_attempted=870\n")
	require.Contains(t, out.String(), "reconciliation_resolved=185\n")
	require.Contains(t, out.String(), "reconciliation_remaining=685\n")
}
