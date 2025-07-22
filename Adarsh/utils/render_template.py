import jinja2
import urllib.parse
import logging
import aiohttp

from Adarsh.vars import Var
from Adarsh.bot import StreamBot
from Adarsh.utils.human_readable import humanbytes
from Adarsh.utils.file_properties import get_file_ids
from Adarsh.server.exceptions import InvalidHash

async def render_page(id, secure_hash, src=None):
    file_data = await get_file_ids(StreamBot, int(Var.BIN_CHANNEL), int(id))
    if file_data.unique_id[:6] != secure_hash:
        logging.debug(f"link hash: {secure_hash} - {file_data.unique_id[:6]}")
        raise InvalidHash

    src = urllib.parse.urljoin(
        Var.URL,
        f"{id}/{urllib.parse.quote_plus(file_data.file_name)}?hash={secure_hash}",
    )

    tag = file_data.mime_type.split("/")[0].strip()
    file_size = humanbytes(file_data.file_size)

    if tag in ["video", "audio"]:
        template_file = "Adarsh/template/req.html"
    else:
        template_file = "Adarsh/template/dl.html"
        async with aiohttp.ClientSession() as s:
            async with s.get(src) as u:
                file_size = humanbytes(int(u.headers.get("Content-Length")))

    # Read template using Jinja2
    with open(template_file) as f:
        template = jinja2.Template(f.read())

    return template.render(
        file_name=file_data.file_name.replace("_", " "),
        file_url=src,
        file_size=file_size,
        tag=tag,
        file_unique_id=file_data.unique_id
    )
