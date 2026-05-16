package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// fixture holds the IDs of the shared parent rows inserted for one test run.
type fixture struct {
	CampaignID    uuid.UUID
	RunID         uuid.UUID
	UserID        uuid.UUID
	ChannelID     uuid.UUID
	UserChannelID uuid.UUID
	ChannelCode   string // unique per fixture to avoid UK conflicts on channels.code
}

// setupFixture inserts all rows required by FK constraints and registers
// cleanup on tb. Call once per test/benchmark function — each call creates
// its own campaign_id so concurrent tests do not interfere.
func setupFixture(tb testing.TB) *fixture {
	tb.Helper()
	ctx := context.Background()

	fix := &fixture{
		CampaignID:    uuid.New(),
		RunID:         uuid.New(),
		UserID:        uuid.New(),
		ChannelID:     uuid.New(),
		UserChannelID: uuid.New(),
		ChannelCode:   fmt.Sprintf("lt-%s", uuid.New()),
	}

	if _, err := dbPool.Exec(ctx,
		`INSERT INTO users (id, region_id, status) VALUES ($1, 'default', 'active')`,
		fix.UserID,
	); err != nil {
		tb.Fatalf("setup: insert user: %v", err)
	}

	// queue_group CHECK: ('email','sms','push','messenger') — use 'email'.
	if _, err := dbPool.Exec(ctx,
		`INSERT INTO channels (id, code, display_name, state, adapter_name, queue_group)
		 VALUES ($1, $2, 'Load-test channel', 'enabled', 'stub', 'email')`,
		fix.ChannelID, fix.ChannelCode,
	); err != nil {
		tb.Fatalf("setup: insert channel: %v", err)
	}

	if _, err := dbPool.Exec(ctx,
		`INSERT INTO user_channels (id, user_id, channel_id, address, status)
		 VALUES ($1, $2, $3, 'lt@example.com', 'active')`,
		fix.UserChannelID, fix.UserID, fix.ChannelID,
	); err != nil {
		tb.Fatalf("setup: insert user_channel: %v", err)
	}

	msgSnap, _ := json.Marshal(map[string]string{"text": "load test"})
	selJSON, _ := json.Marshal(map[string]string{"type": "all"})
	if _, err := dbPool.Exec(ctx,
		`INSERT INTO campaigns
		     (id, manager_id, name, status, message_snapshot, recipient_selector,
		      selected_channel_codes, priority)
		 VALUES ($1, $2, 'Load Test', 'running', $3, $4, ARRAY[$5::text], 'normal')`,
		fix.CampaignID, uuid.New(), msgSnap, selJSON, fix.ChannelCode,
	); err != nil {
		tb.Fatalf("setup: insert campaign: %v", err)
	}

	if _, err := dbPool.Exec(ctx,
		`INSERT INTO campaign_region_runs (id, campaign_id, region_id, status)
		 VALUES ($1, $2, 'default', 'fanout_completed')`,
		fix.RunID, fix.CampaignID,
	); err != nil {
		tb.Fatalf("setup: insert campaign_region_run: %v", err)
	}

	tb.Cleanup(func() { cleanupFixture(ctx, fix) })
	return fix
}

// seedTasks bulk-inserts n delivery_tasks with status='queued' and bumps
// campaign_stats.queued by n. Returns the task IDs in insertion order.
func seedTasks(tb testing.TB, ctx context.Context, fix *fixture, n int) []uuid.UUID {
	tb.Helper()

	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = uuid.New()
	}

	if _, err := dbPool.Exec(ctx,
		`INSERT INTO campaign_stats (campaign_id, total_tasks, queued)
		 VALUES ($1, $2, $2)
		 ON CONFLICT (campaign_id) DO UPDATE
		     SET total_tasks = campaign_stats.total_tasks + $2,
		         queued      = campaign_stats.queued      + $2`,
		fix.CampaignID, n,
	); err != nil {
		tb.Fatalf("seed: upsert campaign_stats: %v", err)
	}

	msgSnap, _ := json.Marshal(map[string]string{"text": "lt msg"})
	batch := &pgx.Batch{}
	for i, id := range ids {
		batch.Queue(
			`INSERT INTO delivery_tasks
			     (id, campaign_id, campaign_region_run_id, region_id,
			      user_id, user_channel_id, channel_id,
			      channel_code, queue_group,
			      recipient_address_snapshot, message_snapshot,
			      idempotency_key, status, priority,
			      attempt_count, max_attempts, available_at)
			 VALUES
			     ($1,$2,$3,'default',$4,$5,$6,$7,'email','lt@example.com',$8,$9,
			      'queued','normal',0,5,NOW())`,
			id, fix.CampaignID, fix.RunID,
			fix.UserID, fix.UserChannelID, fix.ChannelID,
			fix.ChannelCode, msgSnap,
			fmt.Sprintf("lt:%s:%d", fix.CampaignID, i),
		)
	}

	br := dbPool.SendBatch(ctx, batch)
	defer br.Close()
	for range ids {
		if _, err := br.Exec(); err != nil {
			tb.Fatalf("seed: insert task: %v", err)
		}
	}

	return ids
}

// makeMessage returns a JSON DeliveryTaskCreated payload suitable for Service.Process.
func makeMessage(taskID, campaignID, runID uuid.UUID, channelCode string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"event_type":             "DeliveryTaskCreated",
		"task_id":                taskID,
		"campaign_id":            campaignID,
		"campaign_region_run_id": runID,
		"region_id":              "default",
		"channel_code":           channelCode,
		"queue_group":            "email",
		"priority":               "normal",
	})
	return b
}

// cleanupFixture deletes all rows created for a fixture in reverse FK order.
func cleanupFixture(ctx context.Context, fix *fixture) {
	dbPool.Exec(ctx, `DELETE FROM dlq_items        WHERE campaign_id = $1`, fix.CampaignID)           //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM outbox_events     WHERE payload->>'campaign_id' = $1`, fix.CampaignID.String()) //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM delivery_attempts WHERE campaign_id = $1`, fix.CampaignID)           //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM delivery_tasks    WHERE campaign_id = $1`, fix.CampaignID)           //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM campaign_stats    WHERE campaign_id = $1`, fix.CampaignID)           //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM campaign_region_runs WHERE campaign_id = $1`, fix.CampaignID)        //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM campaigns         WHERE id = $1`, fix.CampaignID)                    //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM user_channels     WHERE id = $1`, fix.UserChannelID)                 //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM channels          WHERE id = $1`, fix.ChannelID)                     //nolint:errcheck
	dbPool.Exec(ctx, `DELETE FROM users             WHERE id = $1`, fix.UserID)                        //nolint:errcheck
}
