import asyncio
import logging
import aiohttp
import traceback
from Adarsh.vars import Var


async def ping_server():
    """Keep-alive ping that uses THIS instance's URL only (domain independent)."""
    sleep_time = Var.PING_INTERVAL
    while True:
        await asyncio.sleep(sleep_time)
        try:
            # Use get_base_url() for domain independence
            ping_url = Var.get_base_url()
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.get(ping_url) as resp:
                    logging.info("Pinged server with response: {}".format(resp.status))
        except TimeoutError:
            logging.warning("Couldn't connect to the site URL..!")
        except Exception:
            traceback.print_exc()
