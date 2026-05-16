import { getStoredToken } from "../auth/session";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "/api";

function buildQuery(query = {}) {
  const params = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") return;
    params.set(key, String(value));
  });
  const serialized = params.toString();
  return serialized ? `?${serialized}` : "";
}

function idempotencyHeaders() {
  return {
    "Idempotency-Key": globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`,
  };
}

async function request(path, options = {}) {
  const { auth = true, idempotent = false, headers = {}, ...rest } = options;
  const token = getStoredToken();
  const finalHeaders = {
    "Content-Type": "application/json",
    ...(auth && token ? { Authorization: `Bearer ${token}` } : {}),
    ...(idempotent ? idempotencyHeaders() : {}),
    ...headers,
  };

  const response = await fetch(`${API_BASE}${path}`, {
    headers: finalHeaders,
    ...rest,
  });

  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { message: text };
    }
  }

  if (!response.ok) {
    const error = new Error(data?.error?.message || data?.message || `Request failed: ${response.status}`);
    error.status = response.status;
    error.payload = data;
    throw error;
  }

  return data;
}

export const api = {
  login(payload) {
    return request("/auth/login", {
      method: "POST",
      auth: false,
      body: JSON.stringify(payload),
    });
  },

  health() {
    return request("/healthz", { auth: false });
  },

  ready() {
    return request("/readyz", { auth: false });
  },

  listCampaigns(query = {}) {
    return request(`/campaigns${buildQuery(query)}`);
  },

  getCampaign(campaignId) {
    return request(`/campaigns/${campaignId}`);
  },

  getCampaignStats(campaignId) {
    return request(`/campaigns/${campaignId}/stats`);
  },

  getCampaignTasks(campaignId, query = {}) {
    return request(`/campaigns/${campaignId}/tasks${buildQuery(query)}`);
  },

  getCampaignResults(campaignId, query = {}) {
    return request(`/campaigns/${campaignId}/results${buildQuery(query)}`);
  },

  getCampaignErrors(campaignId, query = {}) {
    return request(`/campaigns/${campaignId}/errors${buildQuery(query)}`);
  },

  createCampaign(payload) {
    return request("/campaigns", {
      method: "POST",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  cancelCampaign(campaignId, payload = {}) {
    return request(`/campaigns/${campaignId}/cancel`, {
      method: "POST",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  listChannels() {
    return request("/channels");
  },

  createChannel(payload) {
    return request("/channels", {
      method: "POST",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  updateChannel(channelId, payload) {
    return request(`/channels/${channelId}`, {
      method: "PATCH",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  enableChannel(channelId) {
    return request(`/channels/${channelId}/enable`, {
      method: "POST",
      idempotent: true,
    });
  },

  disableChannel(channelId) {
    return request(`/channels/${channelId}/disable`, {
      method: "POST",
      idempotent: true,
    });
  },

  getRegionalConfigs(channelId) {
    return request(`/channels/${channelId}/regional-configs`);
  },

  upsertRegionalConfig(channelId, regionId, payload) {
    return request(`/channels/${channelId}/regional-configs/${regionId}`, {
      method: "PUT",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  listDlq(query = {}) {
    return request(`/dlq${buildQuery(query)}`);
  },

  replayDlq(payload) {
    return request("/dlq/replay", {
      method: "POST",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  usersBulkImport(payload) {
    return request("/users/bulk", {
      method: "POST",
      idempotent: true,
      body: JSON.stringify(payload),
    });
  },

  estimateRecipients(payload) {
    return request("/users/estimate", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },
};
