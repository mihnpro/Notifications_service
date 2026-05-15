from enum import StrEnum


class OutboxTaskStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"


class OutboxTaskType(StrEnum):
    TOKEN_UPDATE = "token_update"  # noqa: S105
    RESPONSE_COLLECTION = "response_collection"
    RESUME_SEARCH_SCAN = "resume_search_scan"
    CHAT_POLLING = "chat_polling"
    COMPLETED_TASK_CLEANUP = "completed_task_cleanup"
