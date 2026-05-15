from uuid import UUID, uuid5

DELIVERY_TASK_NAMESPACE = UUID("5b3856b8-0426-4fd9-9d95-e67e85d8d89e")


def build_delivery_task_id(campaign_id: UUID, user_channel_id: UUID, channel_id: UUID) -> UUID:
    """Generate deterministic delivery task ID for restart-safe fan-out reruns."""
    raw = f"{campaign_id}:{user_channel_id}:{channel_id}"
    return uuid5(DELIVERY_TASK_NAMESPACE, raw)
