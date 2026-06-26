import re
import time
import math
import logging
import secrets
import mimetypes
import asyncio
import aiohttp as aiohttp_client
from datetime import datetime, timezone
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

# Dedicated logger for stream route diagnostics
stream_log = logging.getLogger("stream.routes")

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
        
        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
        if not temp_data:
            return web.Response(text="❌ Link expired or not found", status=404)
        
        return web.Response(text=await render_prepare_page(temp_data), content_type='text/html')
        
    except Exception as e:
        logging.error(f"Error in prepare_stream_handler: {e}")
        return web.Response(text="❌ Error loading page", status=500)


@routes.get(r"/api/generate/{token}")
async def generate_stream_handler(request: web.Request):
    """API endpoint to generate actual stream link by copying to BIN_CHANNEL"""
    try:
        token = request.match_info["token"]
        player = request.rel_url.query.get("player", "plyr")
        
        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
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
        
        serve_domain = Var.SERVE_DOMAIN if Var.SERVE_DOMAIN in ('web', 'webx') else None
        
        temp_data = await db.get_temp_file(token, serve_domain=serve_domain)
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
        stream_log.warning(
            f"[WATCH] ❌ Invalid hash for path={request.path} "
            f"ip={request.headers.get('X-Forwarded-For', request.remote)} "
            f"[CAUSE: hash in URL does not match file — tampered or wrong link]"
        )
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        stream_log.warning(
            f"[WATCH] ❌ File not found for path={request.path} "
            f"[CAUSE: message ID not in BIN_CHANNEL or file deleted from Telegram]"
        )
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError) as e:
        stream_log.warning(
            f"[WATCH] Transient error path={request.path} "
            f"type={type(e).__name__} error={e} "
            f"[CAUSE: client disconnected mid-request or bad HTTP framing]"
        )
        return web.Response(status=503, text="Service temporarily unavailable")
    except Exception as e:
        stream_log.critical(
            f"[WATCH] Unhandled error path={request.path} "
            f"type={type(e).__name__} error={e}",
            exc_info=True
        )
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
        stream_log.warning(
            f"[MEDIA] ❌ Invalid hash path={request.path} "
            f"ip={request.headers.get('X-Forwarded-For', request.remote)} "
            f"[CAUSE: hash mismatch — link tampered or wrong hash param]"
        )
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        stream_log.warning(
            f"[MEDIA] ❌ File not found path={request.path} "
            f"[CAUSE: message ID missing from BIN_CHANNEL or Telegram deleted it]"
        )
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError) as e:
        stream_log.warning(
            f"[MEDIA] Transient error path={request.path} "
            f"type={type(e).__name__} error={e} "
            f"[CAUSE: client disconnected or network reset during stream]"
        )
        return web.Response(status=503, text="Service temporarily unavailable")
    except Exception as e:
        stream_log.critical(
            f"[MEDIA] Unhandled error path={request.path} "
            f"type={type(e).__name__} error={e}",
            exc_info=True
        )
        raise web.HTTPInternalServerError(text=str(e))

class_cache = {}

async def media_streamer(request: web.Request, id: int, secure_hash: str):
    req_start = time.monotonic()
    client_ip = request.headers.get("X-Forwarded-For", request.remote or "unknown").split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "unknown")[:120]
    range_header = request.headers.get("Range", 0)
    is_download = request.query.get("download") == "1"

    stream_log.info(
        f"[MSG={id}] ▶ REQUEST — ip={client_ip} "
        f"range={range_header!r} download={is_download} "
        f"ua={user_agent!r}"
    )

    # ── Client selection ─────────────────────────────────────────────────────
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    current_loads = {k: v for k, v in work_loads.items()}
    stream_log.info(
        f"[MSG={id}] Client selected: index={index} "
        f"all_loads={current_loads} total_clients={len(multi_clients)}"
    )

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        stream_log.debug(f"[MSG={id}] Reusing cached ByteStreamer for client {index}")
    else:
        stream_log.info(f"[MSG={id}] Creating new ByteStreamer for client {index}")
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    # ── File properties ──────────────────────────────────────────────────────
    t0 = time.monotonic()
    file_id = await tg_connect.get_file_properties(id)
    prop_ms = (time.monotonic() - t0) * 1000
    stream_log.info(
        f"[MSG={id}] File properties resolved in {prop_ms:.0f}ms — "
        f"dc_id={file_id.dc_id} file_size={file_id.file_size} "
        f"mime={getattr(file_id, 'mime_type', 'unknown')} "
        f"file_name={getattr(file_id, 'file_name', 'unknown')}"
    )

    if file_id.unique_id[:6] != secure_hash:
        stream_log.warning(
            f"[MSG={id}] ❌ Hash mismatch — "
            f"expected={secure_hash!r} got={file_id.unique_id[:6]!r} "
            f"[CAUSE: link tampered or wrong hash]"
        )
        raise InvalidHash

    file_size = file_id.file_size

    # ── Range parsing ────────────────────────────────────────────────────────
    if range_header:
        try:
            range_str = range_header.replace("bytes=", "")
            from_bytes_str, until_bytes_str = range_str.split("-")
            from_bytes = int(from_bytes_str)
            until_bytes = int(until_bytes_str) if until_bytes_str else file_size - 1
        except Exception as e:
            stream_log.error(
                f"[MSG={id}] ❌ Malformed Range header {range_header!r}: {e} "
                f"[CAUSE: client sent invalid range — browser bug or unusual player]"
            )
            return web.Response(status=400, body=f"Bad Range header: {range_header}")
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if file_size > 0:
        pct_info = f"pct_start={from_bytes/file_size*100:.1f}% pct_end={until_bytes/file_size*100:.1f}%"
    else:
        pct_info = "pct=N/A(file_size=0)"
    stream_log.info(
        f"[MSG={id}] Range parsed — from={from_bytes} until={until_bytes} "
        f"file_size={file_size} {pct_info}"
    )

    # ── Range validation ─────────────────────────────────────────────────────
    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        stream_log.warning(
            f"[MSG={id}] ❌ Range not satisfiable — "
            f"from={from_bytes} until={until_bytes} file_size={file_size} "
            f"[CAUSE: player requested bytes outside file bounds]"
        )
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

    stream_log.info(
        f"[MSG={id}] Chunk math — offset={offset} first_cut={first_part_cut} "
        f"last_cut={last_part_cut} req_length={req_length//1024}KB "
        f"part_count={part_count} chunk_size={chunk_size//1024}KB"
    )

    if part_count <= 0:
        stream_log.error(
            f"[MSG={id}] ❌ Invalid part_count={part_count} — "
            f"offset={offset} until_bytes={until_bytes} from_bytes={from_bytes} "
            f"[CAUSE: math error or zero-length range — video may not start]"
        )

    body = tg_connect.yield_file(
        file_id, id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    # ── MIME / filename ──────────────────────────────────────────────────────
    mime_type = file_id.mime_type
    file_name = file_id.file_name

    if is_download:
        disposition = "attachment"
    elif mime_type and (mime_type.startswith("video/") or mime_type.startswith("audio/")):
        disposition = "inline"
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

    # ── Headers ──────────────────────────────────────────────────────────────
    headers = {
        "Content-Type": f"{mime_type}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Range, Content-Length",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "X-Content-Type-Options": "nosniff",
        "X-Forwarded-For": request.remote,
    }

    status_code = 206 if range_header else 200
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    setup_ms = (time.monotonic() - req_start) * 1000
    stream_log.info(
        f"[MSG={id}] ✅ Sending response — "
        f"status={status_code} content_length={req_length//1024}KB "
        f"mime={mime_type} disposition={disposition} "
        f"setup_time={setup_ms:.0f}ms "
        f"[DC={file_id.dc_id} parts={part_count}]"
    )

    return web.Response(
        status=status_code,
        body=body,
        headers=headers,
    )


# ──────────────────────────────────────────────────────────────────────────────
# /root-tree  —  GitHub repo file index (admin use)
# ──────────────────────────────────────────────────────────────────────────────

_ROOT_REPO   = "sunday2212/webreadme4"
_ROOT_FOLDER = "1234xxx"


def _build_tree(flat_items: list) -> dict:
    """
    Convert GitHub's flat tree list into a nested dict.
    Only keeps .html blobs and their parent directories inside _ROOT_FOLDER.
    Structure: { name: {'_t': 'dir', '_c': {...}} | {'_t': 'file'} }
    """
    root: dict = {}
    prefix = _ROOT_FOLDER + "/"

    for item in flat_items:
        path: str = item.get("path", "")
        kind: str = item.get("type", "")   # 'blob' | 'tree'

        if not path.startswith(prefix):
            continue

        rel = path[len(prefix):]           # strip "1234xxx/"
        if not rel:
            continue

        if kind == "blob" and not rel.endswith(".html"):
            continue                        # only HTML files

        parts = rel.split("/")
        node = root
        for i, part in enumerate(parts):
            is_last = (i == len(parts) - 1)
            if is_last:
                if kind == "blob":
                    node[part] = {"_t": "file"}
                else:
                    node.setdefault(part, {"_t": "dir", "_c": {}})
            else:
                node.setdefault(part, {"_t": "dir", "_c": {}})
                node = node[part]["_c"]

    return root


def _render_tree_html(node: dict, depth: int = 0) -> str:
    """Recursively render the nested tree as HTML details/summary."""
    if not node:
        return '<p class="empty">— empty —</p>'

    dirs  = sorted(k for k, v in node.items() if v["_t"] == "dir")
    files = sorted(k for k, v in node.items() if v["_t"] == "file")
    html  = ""

    for name in dirs:
        children = node[name].get("_c", {})
        # Count direct HTML files recursively for the badge
        def _count(n):
            t = sum(1 for v in n.values() if v["_t"] == "file")
            for v in n.values():
                if v["_t"] == "dir":
                    t += _count(v.get("_c", {}))
            return t
        cnt = _count(children)
        badge = f'<span class="badge">{cnt}</span>' if cnt else ""
        inner = _render_tree_html(children, depth + 1)
        open_attr = ""
        html += (
            f'<details{open_attr}>'
            f'<summary><span class="arr">▶</span>📁 {name} {badge}</summary>'
            f'<div class="indent">{inner}</div>'
            f'</details>'
        )

    for name in files:
        display = name[:-5] if name.endswith(".html") else name
        html += f'<div class="file"><span class="fi">📄</span>{display}</div>'

    return html


@routes.get("/root-tree")
async def root_tree_handler(request: web.Request) -> web.Response:
    """Serve an interactive collapsible file index of the GitHub repo folder."""
    token = Var.GIT_TOKEN
    if not token:
        return web.Response(
            text="<h2>GIT_TOKEN not configured.</h2>",
            content_type="text/html", status=500
        )

    api_url = f"https://api.github.com/repos/{_ROOT_REPO}/git/trees/HEAD?recursive=1"
    headers_gh = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "StreamBot-FileIndex/1.0",
    }

    try:
        async with aiohttp_client.ClientSession() as session:
            async with session.get(api_url, headers=headers_gh, timeout=aiohttp_client.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    return web.Response(
                        text=f"<h2>GitHub API error {resp.status}</h2><pre>{body[:500]}</pre>",
                        content_type="text/html", status=502
                    )
                data = await resp.json()
    except Exception as exc:
        logging.error(f"root-tree GitHub fetch error: {exc}")
        return web.Response(
            text=f"<h2>Fetch error</h2><pre>{exc}</pre>",
            content_type="text/html", status=502
        )

    truncated = data.get("truncated", False)
    flat_items = data.get("tree", [])
    tree = _build_tree(flat_items)
    tree_html = _render_tree_html(tree)

    ts = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
    trunc_warn = (
        '<p class="warn">⚠️ Repository tree was truncated by GitHub — some files may be missing.</p>'
        if truncated else ""
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>File Index — {_ROOT_FOLDER}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:20px 16px;min-height:100vh}}
h1{{color:#58a6ff;font-size:1.35rem;margin-bottom:4px}}
.meta{{color:#8b949e;font-size:.78rem;margin-bottom:18px}}
.warn{{background:#3d1f00;color:#e3b341;border:1px solid #e3b341;border-radius:6px;padding:8px 12px;margin-bottom:12px;font-size:.82rem}}
.tree{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 12px}}
details{{margin:2px 0}}
summary{{
  cursor:pointer;padding:7px 10px;border-radius:6px;
  display:flex;align-items:center;gap:6px;
  font-weight:600;color:#58a6ff;
  list-style:none;-webkit-tap-highlight-color:transparent
}}
summary::-webkit-details-marker{{display:none}}
summary:hover{{background:#21262d}}
details[open]>summary{{color:#79c0ff}}
.arr{{font-size:.6rem;color:#8b949e;transition:transform .15s;display:inline-block;min-width:10px}}
details[open]>summary .arr{{transform:rotate(90deg)}}
.indent{{padding-left:18px;border-left:1px solid #30363d;margin-left:15px;margin-top:2px}}
.file{{padding:6px 10px 6px 36px;color:#c9d1d9;font-size:.88rem;border-radius:4px;display:flex;align-items:center;gap:7px}}
.file:hover{{background:#21262d}}
.fi{{font-size:.9rem}}
.badge{{background:#21262d;color:#8b949e;font-size:.68rem;padding:1px 6px;border-radius:10px;font-weight:400;margin-left:4px}}
.empty{{color:#8b949e;font-style:italic;padding:6px 10px;font-size:.82rem}}
</style>
</head>
<body>
<h1>📁 {_ROOT_FOLDER}</h1>
{trunc_warn}
<div class="tree">
{tree_html}
</div>
</body>
</html>"""

    return web.Response(text=page, content_type="text/html", charset="utf-8")
