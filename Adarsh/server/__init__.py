# © NobiDeveloper

from aiohttp import web
from .stream_routes import routes


async def web_server():
    # Increase max size and disable read timeout for large file streaming
    web_app = web.Application(
        client_max_size=1024**3,  # 1GB max size
    )
    web_app.add_routes(routes)
    return web_app
