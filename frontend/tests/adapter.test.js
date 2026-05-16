import assert from "node:assert/strict";
import { adaptCampaign, adaptChannel, adaptIssue, adaptResult, toCreateCampaignPayload } from "../src/api/adapters.js";

const campaign = adaptCampaign(
  {
    id: "campaign-1",
    name: "Test campaign",
    status: "running",
    selectedChannelCodes: ["email", "sms"],
    priority: "normal",
    recipientSelector: { type: "all" },
    messageSnapshot: { subject: "Hello", body: "World" },
  },
  {
    stats: {
      totalTasks: 100,
      queued: 10,
      sending: 5,
      succeeded: 80,
      retryScheduled: 3,
      deadLettered: 2,
    },
  },
);

assert.equal(campaign.id, "campaign-1");
assert.equal(campaign.status, "sending");
assert.equal(campaign.stats.total, 100);
assert.equal(campaign.stats.delivered, 80);
assert.equal(campaign.stats.inProgress, 15);
assert.equal(campaign.stats.waiting, 3);
assert.equal(campaign.stats.failed, 2);

const result = adaptResult({
  taskId: "task-1",
  campaignId: "campaign-1",
  recipientAddressSnapshot: "+49123",
  channelCode: "sms",
  status: "retry_scheduled",
  attemptCount: 2,
  messageSnapshot: { body: "Message" },
});

assert.equal(result.status, "will_retry");
assert.equal(result.recipient, "+49123");
assert.equal(result.channel, "sms");
assert.equal(result.message, "Message");

const issue = adaptIssue({
  id: "issue-1",
  campaignId: "campaign-1",
  channelCode: "email",
  errorCode: "PROVIDER_TIMEOUT",
  status: "open",
});

assert.equal(issue.reason, "Временная ошибка провайдера");
assert.equal(issue.severity, "warn");

const channel = adaptChannel({ code: "telegram", globalState: "degraded" });
assert.equal(channel.state, "limited");
assert.equal(channel.displayName, "Telegram");

const payload = toCreateCampaignPayload({
  name: "May promo",
  subject: "Promo",
  body: "Hello",
  recipientSelector: { type: "all" },
  channels: ["email"],
  priority: "normal",
});

assert.deepEqual(payload, {
  name: "May promo",
  regionIds: ["default"],
  message: { subject: "Promo", body: "Hello" },
  recipientSelector: { type: "all" },
  channels: ["email"],
  priority: "normal",
});

console.log("adapter tests passed");
