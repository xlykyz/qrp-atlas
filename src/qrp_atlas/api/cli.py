"""Configured QRP Atlas API launcher."""

from __future__ import annotations

import uvicorn

from qrp_atlas.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "qrp_atlas.api.server:app",
        host=settings.api.host,
        port=settings.api.port,
        log_level=settings.logging.level.lower(),
    )


if __name__ == "__main__":
    main()
