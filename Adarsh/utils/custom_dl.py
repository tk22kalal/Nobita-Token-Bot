import math
import time
import asyncio
import logging
from Adarsh.vars import Var
from typing import Dict, Union
from Adarsh.bot import work_loads
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid, FloodWait, RPCError
from pyrogram.errors.exceptions.service_unavailable_503 import Timeout as TelegramTimeout
from pyrogram.errors.exceptions.internal_server_error_500 import InternalServerError
from Adarsh.server.exceptions import FIleNotFound
from pyrogram.file_id import FileId, FileType, ThumbnailSource

# Separate logger so we can see it clearly
log = logging.getLogger("stream.downloader")


class ByteStreamer:
    def __init__(self, client: Client):
        """A custom class that holds the cache of a specific client and class functions.
        attributes:
            client: the client that the cache is for.
            cached_file_ids: a dict of cached file IDs.
            cached_file_properties: a dict of cached file properties.
        
        functions:
            generate_file_properties: returns the properties for a media of a specific message contained in Tuple.
            generate_media_session: returns the media session for the DC that contains the media file.
            yield_file: yield a file from telegram servers for streaming.
            
        This is a modified version of the <https://github.com/eyaadh/megadlbot_oss/blob/master/mega/telegram/utils/custom_download.py>
        Thanks to Eyaadh <https://github.com/eyaadh>
        """
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, id: int) -> FileId:
        """
        Returns the properties of a media of a specific message in a FIleId class.
        if the properties are cached, then it'll return the cached results.
        or it'll generate the properties from the Message ID and cache them.
        """
        if id not in self.cached_file_ids:
            await self.generate_file_properties(id)
            log.debug(f"[MSG={id}] File properties cached")
        return self.cached_file_ids[id]
    
    async def generate_file_properties(self, id: int) -> FileId:
        """
        Generates the properties of a media file on a specific message.
        returns ths properties in a FIleId class.
        """
        log.info(f"[MSG={id}] Fetching file properties from BIN_CHANNEL …")
        t0 = time.monotonic()
        file_id = await get_file_ids(self.client, Var.BIN_CHANNEL, id)
        elapsed = time.monotonic() - t0
        if not file_id:
            log.error(f"[MSG={id}] File not found in BIN_CHANNEL (lookup took {elapsed:.2f}s)")
            raise FIleNotFound
        log.info(
            f"[MSG={id}] Properties OK in {elapsed:.2f}s — "
            f"dc_id={file_id.dc_id} file_size={file_id.file_size} "
            f"mime={getattr(file_id, 'mime_type', 'unknown')} "
            f"unique_id={file_id.unique_id[:12]}…"
        )
        self.cached_file_ids[id] = file_id
        return self.cached_file_ids[id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.
        """
        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            log.info(
                f"[DC={file_id.dc_id}] No cached session — creating new media session "
                f"(client_dc={await client.storage.dc_id()})"
            )
            t0 = time.monotonic()
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client, file_id.dc_id, await client.storage.test_mode()
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for attempt in range(6):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )
                    try:
                        await media_session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        log.info(f"[DC={file_id.dc_id}] Authorization imported successfully on attempt {attempt+1}")
                        break
                    except AuthBytesInvalid:
                        log.warning(
                            f"[DC={file_id.dc_id}] AuthBytesInvalid on attempt {attempt+1}/6 — retrying …"
                        )
                        continue
                else:
                    log.error(f"[DC={file_id.dc_id}] All 6 auth attempts failed — stopping session")
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

            elapsed = time.monotonic() - t0
            log.info(f"[DC={file_id.dc_id}] Media session created in {elapsed:.2f}s")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            log.debug(f"[DC={file_id.dc_id}] Reusing cached media session")
        return media_session


    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation,
                                                     raw.types.InputDocumentFileLocation,
                                                     raw.types.InputPeerPhotoFileLocation,]:
        """
        Returns the file location for the media file.
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[str, None]:
        """
        Custom generator that yields the bytes of the media file.
        Modded from <https://github.com/eyaadh/megadlbot_oss/blob/master/mega/telegram/utils/custom_download.py#L20>
        Thanks to Eyaadh <https://github.com/eyaadh>
        """
        client = self.client
        work_loads[index] += 1

        media_id = getattr(file_id, 'media_id', 'unknown')
        file_size = getattr(file_id, 'file_size', 'unknown')
        dc_id = file_id.dc_id

        log.info(
            f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
            f"yield_file START — offset={offset} part_count={part_count} "
            f"chunk_size={chunk_size//1024}KB first_cut={first_part_cut} last_cut={last_part_cut} "
            f"file_size={file_size}"
        )
        stream_start = time.monotonic()

        media_session = await self.generate_media_session(client, file_id)
        location = await self.get_location(file_id)

        current_part = 1
        bytes_yielded = 0

        def _fetch_log_prefix():
            return (
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"[PART={current_part}/{part_count}] [OFFSET={offset}]"
            )

        try:
            # ── First chunk ──────────────────────────────────────────────────
            retry_count = 0
            max_retries = 5
            while True:
                t_fetch = time.monotonic()
                try:
                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location, offset=offset, limit=chunk_size
                        ),
                    )
                    fetch_ms = (time.monotonic() - t_fetch) * 1000
                    log.info(
                        f"{_fetch_log_prefix()} First chunk fetched in {fetch_ms:.0f}ms "
                        f"(retry={retry_count})"
                    )
                    if fetch_ms > 5000:
                        log.warning(
                            f"{_fetch_log_prefix()} ⚠ SLOW first chunk — {fetch_ms:.0f}ms "
                            f"(DC={dc_id} is slow or overloaded)"
                        )
                    break
                except FloodWait as e:
                    log.warning(
                        f"{_fetch_log_prefix()} FloodWait — Telegram rate limit hit, "
                        f"waiting {e.value}s before retry"
                    )
                    await asyncio.sleep(e.value)
                    continue
                except TelegramTimeout as e:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, 10)
                    log.error(
                        f"{_fetch_log_prefix()} ❌ TelegramTimeout on first chunk — "
                        f"error={e} retry={retry_count}/{max_retries} next_wait={wait_time}s "
                        f"[CAUSE: DC={dc_id} did not respond within Pyrogram's timeout — "
                        f"possible DC overload, bad route, or very large file reference]"
                    )
                    if retry_count >= max_retries:
                        log.error(
                            f"{_fetch_log_prefix()} ❌ FATAL: TelegramTimeout exceeded {max_retries} retries "
                            f"on first chunk — stream will fail. "
                            f"[Offset={offset} DC={dc_id} media_id={media_id}]"
                        )
                        raise
                    await asyncio.sleep(wait_time)
                    continue
                except InternalServerError as e:
                    retry_count += 1
                    wait_time = min(3 ** retry_count, 15)
                    log.error(
                        f"{_fetch_log_prefix()} ❌ Telegram InternalServerError on first chunk — "
                        f"error={e} retry={retry_count}/{max_retries} next_wait={wait_time}s "
                        f"[CAUSE: Telegram DC={dc_id} server-side error, usually transient]"
                    )
                    if retry_count >= max_retries:
                        log.error(
                            f"{_fetch_log_prefix()} ❌ FATAL: InternalServerError exceeded {max_retries} retries "
                            f"on first chunk — stream will fail."
                        )
                        raise
                    await asyncio.sleep(wait_time)
                    continue
                except Exception as e:
                    log.error(
                        f"{_fetch_log_prefix()} ❌ Unexpected error on first chunk fetch — "
                        f"type={type(e).__name__} error={e}"
                    )
                    raise

            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        log.warning(
                            f"{_fetch_log_prefix()} Empty chunk received — "
                            f"[CAUSE: Telegram returned 0 bytes at offset={offset}; "
                            f"possible end-of-file or bad file reference]"
                        )
                        break

                    chunk_len = len(chunk)

                    if part_count == 1:
                        sliced = chunk[first_part_cut:last_part_cut]
                        bytes_yielded += len(sliced)
                        yield sliced
                    elif current_part == 1:
                        sliced = chunk[first_part_cut:]
                        bytes_yielded += len(sliced)
                        yield sliced
                    elif current_part == part_count:
                        sliced = chunk[:last_part_cut]
                        bytes_yielded += len(sliced)
                        yield sliced
                    else:
                        bytes_yielded += chunk_len
                        yield chunk

                    log.debug(
                        f"{_fetch_log_prefix()} Yielded {chunk_len//1024}KB "
                        f"total_so_far={bytes_yielded//1024}KB"
                    )

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        break

                    # ── Subsequent chunks ────────────────────────────────────
                    retry_count = 0
                    max_retries = 5
                    while True:
                        t_fetch = time.monotonic()
                        try:
                            r = await media_session.send(
                                raw.functions.upload.GetFile(
                                    location=location, offset=offset, limit=chunk_size
                                ),
                            )
                            fetch_ms = (time.monotonic() - t_fetch) * 1000
                            if fetch_ms > 5000:
                                log.warning(
                                    f"{_fetch_log_prefix()} ⚠ SLOW chunk — {fetch_ms:.0f}ms "
                                    f"(DC={dc_id} offset={offset} — "
                                    f"network congestion or Telegram DC bottleneck)"
                                )
                            else:
                                log.debug(
                                    f"{_fetch_log_prefix()} Chunk fetched in {fetch_ms:.0f}ms"
                                )
                            break
                        except FloodWait as e:
                            log.warning(
                                f"{_fetch_log_prefix()} FloodWait — waiting {e.value}s "
                                f"[offset={offset}]"
                            )
                            await asyncio.sleep(e.value)
                            continue
                        except TelegramTimeout as e:
                            retry_count += 1
                            wait_time = min(2 ** retry_count, 10)
                            log.error(
                                f"{_fetch_log_prefix()} ❌ TelegramTimeout on chunk — "
                                f"error={e} retry={retry_count}/{max_retries} "
                                f"next_wait={wait_time}s offset={offset} "
                                f"[CAUSE: DC={dc_id} timeout — buffering/stall will occur]"
                            )
                            if retry_count >= max_retries:
                                log.error(
                                    f"{_fetch_log_prefix()} ❌ FATAL: TelegramTimeout after "
                                    f"{max_retries} retries at offset={offset} DC={dc_id} "
                                    f"— this is WHY the video stops/buffers indefinitely"
                                )
                                raise
                            await asyncio.sleep(wait_time)
                            continue
                        except InternalServerError as e:
                            retry_count += 1
                            wait_time = min(3 ** retry_count, 15)
                            log.error(
                                f"{_fetch_log_prefix()} ❌ Telegram InternalServerError on chunk — "
                                f"error={e} retry={retry_count}/{max_retries} "
                                f"next_wait={wait_time}s offset={offset}"
                            )
                            if retry_count >= max_retries:
                                log.error(
                                    f"{_fetch_log_prefix()} ❌ FATAL: InternalServerError after "
                                    f"{max_retries} retries at offset={offset} DC={dc_id}"
                                )
                                raise
                            await asyncio.sleep(wait_time)
                            continue
                        except Exception as e:
                            log.error(
                                f"{_fetch_log_prefix()} ❌ Unexpected error on chunk fetch — "
                                f"type={type(e).__name__} error={e} offset={offset}"
                            )
                            raise

            else:
                log.error(
                    f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                    f"Unexpected Telegram response type: {type(r).__name__} "
                    f"[CAUSE: GetFile did not return upload.File — possibly wrong location or expired file reference]"
                )

        except TelegramTimeout as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — TelegramTimeout after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB of {file_size} "
                f"part={current_part}/{part_count} offset={offset} "
                f"[WHY: Telegram DC={dc_id} stopped responding — "
                f"DC may be overloaded, the file reference may have expired, "
                f"or there is a network issue between server and Telegram]"
            )
            raise
        except InternalServerError as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — Telegram InternalServerError after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count} "
                f"[WHY: Telegram server-side error on DC={dc_id}]"
            )
            raise
        except (TimeoutError, AttributeError) as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — {type(e).__name__}: {e} after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count} "
                f"[WHY: asyncio timeout or session attribute error — "
                f"client may have disconnected or session is stale]"
            )
            pass
        finally:
            total_elapsed = time.monotonic() - stream_start
            log.info(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"yield_file END — parts_completed={current_part-1}/{part_count} "
                f"bytes_yielded={bytes_yielded//1024}KB "
                f"total_time={total_elapsed:.2f}s "
                f"avg_speed={bytes_yielded/(total_elapsed*1024) if total_elapsed > 0 else 0:.0f}KB/s"
            )
            work_loads[index] -= 1

    
    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            log.debug("File ID cache cleared")
