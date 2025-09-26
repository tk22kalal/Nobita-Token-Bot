import re
import time
import math
import logging
import secrets
import mimetypes
import asyncio
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from pyrogram.errors import FloodWait
from pyrogram.enums import ParseMode
from urllib.parse import quote_plus
from Adarsh.bot import multi_clients, work_loads, StreamBot
from Adarsh.server.exceptions import FIleNotFound, InvalidHash
from Adarsh import StartTime, __version__
from ..utils.time_format import get_readable_time
from ..utils.custom_dl import ByteStreamer
from Adarsh.utils.render_template import render_page
from Adarsh.utils.database import Database
from Adarsh.utils.file_properties import get_name, get_hash
from Adarsh.utils.human_readable import humanbytes
from Adarsh.vars import Var


routes = web.RouteTableDef()

# Initialize database conditionally - use a global instance
db = Database(Var.DATABASE_URL, Var.name)

async def render_prepare_page(temp_data):
    """Render the intermediate page template"""
    try:
        # Read the prepare.html template
        with open("Adarsh/template/prepare.html") as f:
            template_content = f.read()
        
        # Replace placeholders with actual data
        file_size = humanbytes(temp_data.get('file_size', 0))
        file_name = temp_data.get('file_name', 'Unknown File')
        caption = temp_data.get('caption', file_name)
        mime_type = temp_data.get('mime_type', 'application/octet-stream')
        
        # Determine if it's video/audio for icon
        tag = mime_type.split("/")[0].strip() if mime_type else 'file'
        
        template_content = template_content.replace("{{file_name}}", file_name)
        template_content = template_content.replace("{{caption}}", caption)
        template_content = template_content.replace("{{file_size}}", file_size)
        template_content = template_content.replace("{{token}}", temp_data['token'])
        template_content = template_content.replace("{{tag}}", tag)
        
        return template_content
        
    except Exception as e:
        logging.error(f"Error rendering prepare page: {e}")
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>Error</title></head>
        <body>
            <h1>Error loading page</h1>
            <p>{str(e)}</p>
        </body>
        </html>
        """

@routes.get("/favicon.ico")
async def favicon_handler(_):
    return web.Response(status=204)


@routes.get("/", allow_head=True)
async def root_route_handler(_):
    telegram_bot = "Not connected"
    if hasattr(StreamBot, 'username') and StreamBot.username:
        telegram_bot = "@" + StreamBot.username
        
    return web.json_response(
        {
            "server_status": "running",
            "uptime": get_readable_time(int(time.time() - StartTime)),
            "telegram_bot": telegram_bot,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )


@routes.get(r"/prepare/{token}", allow_head=True)
async def prepare_stream_handler(request: web.Request):
    """Intermediate page that shows file info and generate stream button"""
    try:
        token = request.match_info["token"]
        
        # Get file data from database
        temp_data = await db.get_temp_file(token)
        if not temp_data:
            return web.Response(text="❌ Link expired or not found", status=404)
        
        # Render intermediate page template
        return web.Response(text=await render_prepare_page(temp_data), content_type='text/html')
        
    except Exception as e:
        logging.error(f"Error in prepare_stream_handler: {e}")
        return web.Response(text="❌ Error loading page", status=500)


@routes.get(r"/api/generate/{token}")
async def generate_stream_handler(request: web.Request):
    """API endpoint to generate actual stream link by copying to BIN_CHANNEL"""
    try:
        token = request.match_info["token"]
        
        # Get file data from database
        temp_data = await db.get_temp_file(token)
        if not temp_data:
            return web.json_response({"error": "Link expired or not found"}, status=404)
        
        # Get the original message from Telegram
        client = StreamBot  # Use the main bot client
        original_msg = await client.get_messages(temp_data['from_chat_id'], temp_data['message_id'])
        
        if not original_msg:
            return web.json_response({"error": "Original message not found"}, status=404)
        
        # Copy message to BIN_CHANNEL with retry logic for FloodWait
        max_retries = 3
        for attempt in range(max_retries):
            try:
                log_msg = await original_msg.copy(
                    chat_id=Var.BIN_CHANNEL,
                    caption=temp_data['caption'][:1024],
                    parse_mode=ParseMode.HTML
                )
                break
            except FloodWait as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(e.x)
                else:
                    raise
        else:
            return web.json_response({"error": "Max retries exceeded for FloodWait"}, status=429)
        
        # Generate streaming URL
        file_name = get_name(log_msg) or temp_data['file_name'] or "NEXTPULSE"
        file_hash = get_hash(log_msg)
        fqdn_url = Var.get_url_for_file(str(log_msg.id))
        stream_link = f"{fqdn_url}watch/{log_msg.id}/{quote_plus(file_name)}?hash={file_hash}"
        
        # Keep temporary data for permanent links
        # await db.delete_temp_file(token)  # Commented out to make links permanent
        
        return web.json_response({
            "success": True,
            "stream_url": stream_link,
            "file_name": file_name
        })
        
    except Exception as e:
        logging.error(f"Error in generate_stream_handler: {e}")
        return web.json_response({"error": "Failed to generate stream link"}, status=500)


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")
        return web.Response(text=await render_page(id, secure_hash), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))

@routes.get(r"/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")
        return await media_streamer(request, id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))

class_cache = {}

async def media_streamer(request: web.Request, id: int, secure_hash: str):
    range_header = request.headers.get("Range", 0)
    
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]
    
    if Var.MULTI_CLIENT:
        logging.info(f"Client {index} is now serving {request.remote}")

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logging.debug(f"Using cached ByteStreamer object for client {index}")
    else:
        logging.debug(f"Creating new ByteStreamer object for client {index}")
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    logging.debug("before calling get_file_properties")
    file_id = await tg_connect.get_file_properties(id)
    logging.debug("after calling get_file_properties")
    
    if file_id.unique_id[:6] != secure_hash:
        logging.debug(f"Invalid hash for message with ID {id}")
        raise InvalidHash
    
    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    mime_type = file_id.mime_type
    file_name = file_id.file_name
    
    # Set disposition based on content type for better streaming
    if mime_type and (mime_type.startswith("video/") or mime_type.startswith("audio/")):
        disposition = "inline"  # Allow inline playback for media files
    else:
        disposition = "attachment"

    if mime_type:
        if not file_name:
            try:
                file_name = f"{secrets.token_hex(2)}.{mime_type.split('/')[1]}"
            except (IndexError, AttributeError):
                file_name = f"{secrets.token_hex(2)}.unknown"
    else:
        if file_name:
            mime_type = mimetypes.guess_type(file_id.file_name)
        else:
            mime_type = "application/octet-stream"
            file_name = f"{secrets.token_hex(2)}.unknown"

    # Enhanced headers for better proxy compatibility and streaming
    headers = {
        "Content-Type": f"{mime_type}",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache, no-store, must-revalidate",  # Prevent proxy caching issues
        "Pragma": "no-cache",
        "Expires": "0",
        "Access-Control-Allow-Origin": "*",  # CORS for browser compatibility
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Range, Content-Length",
        "X-Content-Type-Options": "nosniff",
        "Connection": "keep-alive",
    }

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers=headers,
    )
