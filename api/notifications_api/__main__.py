import uvicorn

from notifications_api.infra.config import GlobalConfig


def main() -> None:
    cfg = GlobalConfig.load()
    uvicorn.run("notifications_api.app.litestar:app", host=cfg.app.host, port=cfg.app.port)


if __name__ == "__main__":
    main()
