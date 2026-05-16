from litestar import get
from litestar.response import Response

from notifications_api.infra.metrics import metrics_response


@get("/metrics", include_in_schema=False, sync_to_thread=True)
def prometheus_metrics() -> Response[bytes]:
    data, content_type = metrics_response()
    return Response(content=data, media_type=content_type)
