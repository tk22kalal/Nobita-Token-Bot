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
from Adarsh.server.rate_limiter import rate_limiter


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


@routes.get("/robots.txt")
async def robots_handler(_):
    """Serve robots.txt to allow Google bots and other crawlers"""
    try:
        with open("robots.txt", "r") as f:
            content = f.read()
        return web.Response(text=content, content_type="text/plain")
    except FileNotFoundError:
        # Fallback if robots.txt doesn't exist
        return web.Response(
            text="User-agent: *\nAllow: /\n",
            content_type="text/plain"
        )


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
        # Get player choice from query parameter (plyr or videojs)
        player = request.rel_url.query.get("player", "plyr")
        
        # Get file data from database
        temp_data = await db.get_temp_file(token)
        if not temp_data:
            return web.json_response(
                {"success": False, "error": "Link expired or not found"}, 
                status=404,
                content_type='application/json'
            )
        
        # Get the original message from Telegram
        client = StreamBot  # Use the main bot client
        original_msg = await client.get_messages(temp_data['from_chat_id'], temp_data['message_id'])
        
        if not original_msg:
            return web.json_response(
                {"success": False, "error": "Original message not found"}, 
                status=404,
                content_type='application/json'
            )
        
        # Copy message to BIN_CHANNEL with retry logic for FloodWait
        max_retries = 3
        log_msg = None
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
                    await asyncio.sleep(e.value)
                else:
                    return web.json_response(
                        {"success": False, "error": "Server is busy. Please try again in a few seconds."}, 
                        status=429,
                        content_type='application/json'
                    )
            except Exception as copy_error:
                logging.error(f"Error copying message (attempt {attempt + 1}): {copy_error}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    return web.json_response(
                        {"success": False, "error": "Failed to process file. Please try again."}, 
                        status=500,
                        content_type='application/json'
                    )
        
        if not log_msg:
            return web.json_response(
                {"success": False, "error": "Failed to process file after retries"}, 
                status=500,
                content_type='application/json'
            )
        
        # Generate streaming URL with /watch/ prefix for stream links
        file_name = get_name(log_msg) or temp_data['file_name'] or "NEXTPULSE"
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8', errors='ignore')
        file_name = str(file_name)
        file_hash = get_hash(log_msg)
        
        # Use the same domain from the incoming request for consistent subdomain routing
        # Check X-Forwarded-Proto for proper HTTPS detection behind reverse proxies (Heroku, etc.)
        request_host = request.host
        forwarded_proto = request.headers.get('X-Forwarded-Proto', '').lower()
        if forwarded_proto in ('https', 'http'):
            scheme = forwarded_proto
        elif Var.HAS_SSL:
            scheme = 'https'
        else:
            scheme = request.scheme if request.scheme else 'http'
        base_url = f"{scheme}://{request_host}/"
        
        # Include player parameter in stream URL
        stream_link = f"{base_url}watch/{log_msg.id}/{quote_plus(file_name)}?hash={file_hash}&player={player}"
        
        # Keep temporary data for permanent links
        # await db.delete_temp_file(token)  # Commented out to make links permanent
        
        # Prepare response with stream URL
        response_data = {
            "success": True,
            "stream_url": stream_link,
            "file_name": file_name
        }
        
        # Include thumbnail URL if available
        if temp_data.get('thumbnail_url'):
            response_data['thumbnail_url'] = temp_data['thumbnail_url']
        
        return web.json_response(response_data, content_type='application/json')
        
    except Exception as e:
        logging.error(f"Error in generate_stream_handler: {e}", exc_info=True)
        return web.json_response(
            {"success": False, "error": "Server error. Please try again later."}, 
            status=500,
            content_type='application/json'
        )


@routes.get(r"/api/download/{token}")
async def generate_download_handler(request: web.Request):
    """API endpoint to generate download link by copying to BIN_CHANNEL"""
    try:
        # Get client IP for rate limiting
        client_ip = request.headers.get('X-Forwarded-For', request.remote).split(',')[0].strip()
        
        # Check rate limit
        can_proceed, message = rate_limiter.can_proceed(client_ip)
        if not can_proceed:
            return web.json_response(
                {"success": False, "error": message}, 
                status=429,
                content_type='application/json'
            )
        
        # Add request to rate limiter
        rate_limiter.add_request(client_ip)
        
        token = request.match_info["token"]
        
        # Get file data from database
        temp_data = await db.get_temp_file(token)
        if not temp_data:
            return web.json_response(
                {"success": False, "error": "Link expired or not found"}, 
                status=404,
                content_type='application/json'
            )
        
        # Get the original message from Telegram
        client = StreamBot  # Use the main bot client
        original_msg = await client.get_messages(temp_data['from_chat_id'], temp_data['message_id'])
        
        if not original_msg:
            return web.json_response(
                {"success": False, "error": "Original message not found"}, 
                status=404,
                content_type='application/json'
            )
        
        # Copy message to BIN_CHANNEL with retry logic for FloodWait
        max_retries = 3
        log_msg = None
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
                    await asyncio.sleep(e.value)
                else:
                    return web.json_response(
                        {"success": False, "error": "Server is busy. Please try again in a few seconds."}, 
                        status=429,
                        content_type='application/json'
                    )
            except Exception as copy_error:
                logging.error(f"Error copying message (attempt {attempt + 1}): {copy_error}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    return web.json_response(
                        {"success": False, "error": "Failed to process file. Please try again."}, 
                        status=500,
                        content_type='application/json'
                    )
        
        if not log_msg:
            return web.json_response(
                {"success": False, "error": "Failed to process file after retries"}, 
                status=500,
                content_type='application/json'
            )
        
        # Generate download URL with download=1 parameter (direct file link, not /watch/)
        file_name = get_name(log_msg) or temp_data['file_name'] or "NEXTPULSE"
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8', errors='ignore')
        file_name = str(file_name)
        file_hash = get_hash(log_msg)
        
        # Use the same domain from the incoming request for consistent subdomain routing
        # Check X-Forwarded-Proto for proper HTTPS detection behind reverse proxies (Heroku, etc.)
        request_host = request.host
        forwarded_proto = request.headers.get('X-Forwarded-Proto', '').lower()
        if forwarded_proto in ('https', 'http'):
            scheme = forwarded_proto
        elif Var.HAS_SSL:
            scheme = 'https'
        else:
            scheme = request.scheme if request.scheme else 'http'
        base_url = f"{scheme}://{request_host}/"
        
        download_link = f"{base_url}{log_msg.id}/{quote_plus(file_name)}?hash={file_hash}&download=1"
        
        # Keep temporary data for permanent links (don't delete)
        # await db.delete_temp_file(token)  # Commented out to make links permanent
        
        # Prepare response with download URL
        response_data = {
            "success": True,
            "download_url": download_link,
            "file_name": file_name
        }
        
        # Include thumbnail URL if available
        if temp_data.get('thumbnail_url'):
            response_data['thumbnail_url'] = temp_data['thumbnail_url']
        
        # Remove from rate limiter when done
        rate_limiter.remove_request(client_ip)
        
        return web.json_response(response_data, content_type='application/json')
        
    except Exception as e:
        logging.error(f"Error in generate_download_handler: {e}", exc_info=True)
        # Remove from rate limiter on error
        try:
            client_ip = request.headers.get('X-Forwarded-For', request.remote).split(',')[0].strip()
            rate_limiter.remove_request(client_ip)
        except:
            pass
        return web.json_response(
            {"success": False, "error": "Server error. Please try again later."}, 
            status=500,
            content_type='application/json'
        )


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            path_match = re.search(r"(\d+)(?:\/\S+)?", path)
            if not path_match:
                raise web.HTTPBadRequest(text="Invalid path format")
            id = int(path_match.group(1))
            secure_hash = request.rel_url.query.get("hash")
        # Get player choice from query parameter (None allows fallback to env var)
        player = request.rel_url.query.get("player")
        return web.Response(text=await render_page(id, secure_hash, player=player), content_type='text/html')
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError) as e:
        logging.warning(f"Transient error while handling /watch request for path {request.path}: {e}")
        return web.Response(status=503, text="Service temporarily unavailable")
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))

@routes.get(r"/{path:(?!api/)(?!watch/)(?!prepare/)[A-Za-z0-9_-]*\d.*}", allow_head=True)
async def media_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            path_match = re.search(r"(\d+)(?:\/\S+)?", path)
            if not path_match:
                raise web.HTTPBadRequest(text="Invalid path format")
            id = int(path_match.group(1))
            secure_hash = request.rel_url.query.get("hash")
        return await media_streamer(request, id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError) as e:
        logging.warning(f"Transient error while handling request for path {request.path}: {e}")
        return web.Response(status=503, text="Service temporarily unavailable")
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))

class_cache = {}

async def media_streamer(request: web.Request, id: int, secure_hash: str):
    range_header = request.headers.get("Range", 0)
    is_download = request.query.get("download") == "1"  # Check if download is requested
    
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
    part_count = math.ceil((until_bytes + 1) / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    mime_type = file_id.mime_type
    file_name = file_id.file_name
    
    # Set disposition based on request type - force download if download=1 parameter
    if is_download:
        disposition = "attachment"  # Force download
    elif mime_type and (mime_type.startswith("video/") or mime_type.startswith("audio/")):
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

    # Enhanced headers for better proxy compatibility, streaming, and iOS/Android iframe support
    headers = {
        "Content-Type": f"{mime_type}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",  # Allow some caching but prevent stale content
        "Access-Control-Allow-Origin": "*",  # CORS for browser compatibility
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Range, Content-Length",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "X-Content-Type-Options": "nosniff",
        "X-Forwarded-For": request.remote,  # Preserve original IP for Cloudflare
    }
    
    # Only include Content-Range header for partial content (206) responses
    # Including it in 200 OK responses violates HTTP spec and can corrupt downloads
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers=headers,
    )
