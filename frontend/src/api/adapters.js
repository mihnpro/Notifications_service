export function adaptCampaign(apiCampaign = {}, apiStats = null) {
  const stats = apiStats?.stats || apiCampaign?.stats || {};

  return {
    id: apiCampaign.id || apiCampaign.campaignId,
    name: apiCampaign.name || "Без названия",
    status: adaptCampaignStatus(apiCampaign.status),
    createdAt: formatDate(apiCampaign.createdAt),
    audience: getAudienceLabel(apiCampaign),
    channels: apiCampaign.selectedChannelCodes || apiCampaign.channels || [],
    priority: apiCampaign.priority || "normal",
    message: apiCampaign.messageSnapshot || apiCampaign.message || {
      subject: "",
      body: "",
    },
    stats: {
      total: stats.totalTasks || stats.total || 0,
      delivered: stats.succeeded || stats.delivered || 0,
      inProgress: (stats.queued || 0) + (stats.sending || 0) + (stats.inProgress || 0),
      waiting: stats.retryScheduled || stats.waiting || 0,
      failed: (stats.failed || 0) + (stats.deadLettered || 0) + (stats.cancelled || 0),
    },
  };
}

export function adaptResult(row = {}) {
  return {
    id: row.taskId || row.id,
    campaignId: row.campaignId,
    recipient: row.recipient || row.recipientAddressSnapshot || row.recipient_address_snapshot || "—",
    channel: row.channel || row.channelCode || row.channel_code,
    status: adaptDeliveryStatus(row.status),
    attempts: row.attemptCount || row.attempt_count || 0,
    sentAt: row.completedAt ? formatTime(row.completedAt) : "—",
    message: row.message?.body || row.messageSnapshot?.body || row.message_snapshot?.body || "",
    issue: row.finalErrorMessageShort || row.finalErrorCode || row.final_error_code || "—",
  };
}

export function adaptIssue(row = {}) {
  return {
    id: row.id || row.taskId,
    campaign: row.campaignName || row.campaignId || "Рассылка",
    channel: formatChannel(row.channelCode || row.channel || row.channel_code),
    reason: humanizeError(row.errorCode || row.reasonCode || row.error_code || row.reason_code),
    count: row.count || 1,
    action: "Повторить отправку",
    severity: row.status === "open" ? "warn" : "neutral",
  };
}

export function adaptChannel(channel = {}) {
  return {
    id: channel.id,
    code: channel.code,
    displayName: channel.displayName || channel.display_name || formatChannel(channel.code),
    state: adaptChannelState(channel.globalState || channel.global_state || channel.state),
    description: getChannelDescription(channel.code),
    selectedByDefault: ["email", "sms"].includes(channel.code),
  };
}

export function toCreateCampaignPayload(formState) {
  return {
    name: formState.name,
    regionIds: formState.regionIds || ["default"],
    message: {
      subject: formState.subject,
      body: formState.body,
    },
    recipientSelector: formState.recipientSelector,
    channels: formState.channels,
    priority: formState.priority || "normal",
  };
}

function adaptCampaignStatus(status) {
  return {
    running: "sending",
    completed: "completed",
    partially_failed: "needs_attention",
    failed: "needs_attention",
    cancelling: "cancelling",
    cancelled: "cancelled",
  }[status] || status || "sending";
}

function adaptDeliveryStatus(status) {
  return {
    succeeded: "delivered",
    sending: "sending",
    queued: "sending",
    retry_scheduled: "will_retry",
    failed: "failed",
    dead_lettered: "failed",
    cancelled: "failed",
  }[status] || status || "sending";
}

function adaptChannelState(state) {
  return {
    enabled: "enabled",
    disabled: "disabled",
    degraded: "limited",
  }[state] || state || "disabled";
}

function humanizeError(code) {
  return {
    PROVIDER_TIMEOUT: "Временная ошибка провайдера",
    TRANSIENT_503: "Временная ошибка доставки",
    ATTEMPTS_EXHAUSTED: "Превышено число попыток",
    PERMANENT_BOUNCE: "Адрес недоступен",
  }[code] || "Ошибка доставки";
}

function formatChannel(channel) {
  return { email: "Email", sms: "SMS", telegram: "Telegram", whatsapp: "WhatsApp", push: "Push" }[channel] || channel || "Канал";
}

function getChannelDescription(code) {
  return {
    email: "Письма на email-адреса",
    sms: "Короткие сообщения на телефон",
    telegram: "Сообщения в Telegram",
    whatsapp: "Сообщения в WhatsApp",
    push: "Push-уведомления в приложении",
  }[code] || "Канал отправки";
}

function getAudienceLabel(campaign) {
  const selector = campaign.recipientSelector || campaign.recipient_selector;

  if (!selector) return campaign.audience || "Получатели";
  if (selector.type === "all") return "Все активные пользователи";
  if (selector.type === "user_ids") return `${selector.userIds?.length || selector.user_ids?.length || 0} выбранных пользователей`;
  if (selector.type === "external_ids") return "CRM-сегмент";
  if (selector.type === "segment") return "Сегмент";

  return "Получатели";
}

function formatDate(value) {
  if (!value) return "—";
  return new Date(value).toLocaleString("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatTime(value) {
  if (!value) return "—";
  return new Date(value).toLocaleTimeString("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
  });
}
