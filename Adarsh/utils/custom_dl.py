import math
import time
import asyncio
import logging
from Adarsh.vars import Var
from typing import Dict, Optional, Union
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
        """A custom class that holds the cache of a specific client and class functions."""
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        # Per-DC locks so only one coroutine can create/destroy a session at a time.
        # This prevents concurrent coroutines from tearing down each other's sessions.
        self._session_locks: Dict[int, asyncio.Lock] = {}
        asyncio.create_task(self.clean_cache())

    def _dc_lock(self, dc_id: int) -> asyncio.Lock:
        """Return (creating if needed) the per-DC asyncio Lock."""
        if dc_id not in self._session_locks:
            self._session_locks[dc_id] = asyncio.Lock()
        return self._session_locks[dc_id]

    async def get_file_properties(self, id: int) -> FileId:
        if id not in self.cached_file_ids:
            await self.generate_file_properties(id)
            log.debug(f"[MSG={id}] File properties cached")
        return self.cached_file_ids[id]

    async def generate_file_properties(self, id: int) -> FileId:
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
        Returns the media session for the DC that holds the file.
        Protected by a per-DC lock so concurrent coroutines never race to create
        or destroy the same session.
        """
        dc_id = file_id.dc_id
        async with self._dc_lock(dc_id):
            return await self._create_or_get_session(client, file_id)

    async def _create_or_get_session(self, client: Client, file_id: FileId) -> Session:
        """Inner (lock already held): return cached session or build a new one."""
        dc_id = file_id.dc_id
        media_session = client.media_sessions.get(dc_id)

        if media_session is not None:
            log.debug(f"[DC={dc_id}] Reusing cached media session")
            return media_session

        log.info(
            f"[DC={dc_id}] No cached session — creating new media session "
            f"(client_dc={await client.storage.dc_id()})"
        )
        t0 = time.monotonic()

        if dc_id != await client.storage.dc_id():
            media_session = Session(
                client,
                dc_id,
                await Auth(client, dc_id, await client.storage.test_mode()).create(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()

            for attempt in range(6):
                exported_auth = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                try:
                    await media_session.send(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id, bytes=exported_auth.bytes
                        )
                    )
                    log.info(f"[DC={dc_id}] Authorization imported on attempt {attempt+1}")
                    break
                except AuthBytesInvalid:
                    log.warning(f"[DC={dc_id}] AuthBytesInvalid on attempt {attempt+1}/6 — retrying …")
                    continue
            else:
                log.error(f"[DC={dc_id}] All 6 auth attempts failed — stopping session")
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            media_session = Session(
                client,
                dc_id,
                await client.storage.auth_key(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()

        elapsed = time.monotonic() - t0
        log.info(f"[DC={dc_id}] Media session created in {elapsed:.2f}s")
        client.media_sessions[dc_id] = media_session
        return media_session

    async def _recover_stale_session(
        self,
        client: Client,
        file_id: FileId,
        stale_session: Session,
        context: str,
    ) -> Session:
        """
        Safely drop a known-stale session and rebuild a fresh one.

        Safety guarantees:
        - Holds the per-DC lock for the entire operation so no other coroutine
          can read, create, or destroy a session for this DC concurrently.
        - Uses identity check (`is`) before deleting from the cache so we never
          remove a session that was already rebuilt by another coroutine.
        - Stops only the stale_session object (the one *we* hold), not whatever
          might currently be in the cache.
        """
        dc_id = file_id.dc_id
        async with self._dc_lock(dc_id):
            cached = client.media_sessions.get(dc_id)
            if cached is stale_session:
                # The cache still holds our stale object — safe to remove it
                del client.media_sessions[dc_id]
                log.info(
                    f"[DC={dc_id}] [{context}] Removed stale session from cache. "
                    f"Stopping it now …"
                )
                try:
                    await stale_session.stop()
                except Exception as stop_err:
                    log.warning(
                        f"[DC={dc_id}] [{context}] Error stopping stale session (ignoring): {stop_err}"
                    )
            else:
                # Another coroutine already rebuilt the session — reuse it
                if cached is not None:
                    log.info(
                        f"[DC={dc_id}] [{context}] Stale session already replaced by another "
                        f"coroutine — reusing the new session"
                    )
                    return cached
                log.info(
                    f"[DC={dc_id}] [{context}] Stale session already evicted — building fresh one"
                )

            # Build and cache the fresh session
            return await self._create_or_get_session(client, file_id)

    @staticmethod
    async def get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
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

        def _prefix():
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
                    log.info(f"{_prefix()} First chunk fetched in {fetch_ms:.0f}ms (retry={retry_count})")
                    if fetch_ms > 5000:
                        log.warning(f"{_prefix()} ⚠ SLOW first chunk — {fetch_ms:.0f}ms (DC={dc_id})")
                    break

                except FloodWait as e:
                    log.warning(f"{_prefix()} FloodWait {e.value}s — Telegram rate limit")
                    await asyncio.sleep(e.value)
                    continue

                except (TimeoutError, asyncio.TimeoutError) as e:
                    # asyncio-level timeout = DC session is stale/dead.
                    # Recover safely under the per-DC lock.
                    retry_count += 1
                    wait_time = min(2 ** retry_count, 10)
                    log.error(
                        f"{_prefix()} ❌ asyncio TimeoutError on first chunk — "
                        f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s "
                        f"[DC={dc_id} session is stale — rebuilding under lock]"
                    )
                    if retry_count >= max_retries:
                        log.error(
                            f"{_prefix()} ❌ FATAL: asyncio TimeoutError after {max_retries} retries "
                            f"on first chunk — DC={dc_id} session cannot be recovered"
                        )
                        raise
                    await asyncio.sleep(wait_time)
                    media_session = await self._recover_stale_session(
                        client, file_id, media_session, "first-chunk"
                    )
                    continue

                except TelegramTimeout as e:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, 10)
                    log.error(
                        f"{_prefix()} ❌ TelegramTimeout on first chunk — "
                        f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s "
                        f"[DC={dc_id} did not respond — overload or bad route]"
                    )
                    if retry_count >= max_retries:
                        log.error(
                            f"{_prefix()} ❌ FATAL: TelegramTimeout after {max_retries} retries "
                            f"on first chunk [DC={dc_id} media={media_id}]"
                        )
                        raise
                    await asyncio.sleep(wait_time)
                    continue

                except InternalServerError as e:
                    retry_count += 1
                    wait_time = min(3 ** retry_count, 15)
                    log.error(
                        f"{_prefix()} ❌ Telegram InternalServerError on first chunk — "
                        f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s"
                    )
                    if retry_count >= max_retries:
                        log.error(f"{_prefix()} ❌ FATAL: InternalServerError after {max_retries} retries")
                        raise
                    await asyncio.sleep(wait_time)
                    continue

                except Exception as e:
                    log.error(
                        f"{_prefix()} ❌ Unexpected error on first chunk — "
                        f"type={type(e).__name__} error={e}"
                    )
                    raise

            # ── Yield loop ───────────────────────────────────────────────────
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        log.warning(
                            f"{_prefix()} Empty chunk received "
                            f"[DC={dc_id} offset={offset} — possible EOF or bad file reference]"
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
                        f"{_prefix()} Yielded {chunk_len//1024}KB "
                        f"total={bytes_yielded//1024}KB"
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
                                    f"{_prefix()} ⚠ SLOW chunk — {fetch_ms:.0f}ms "
                                    f"DC={dc_id} offset={offset}"
                                )
                            else:
                                log.debug(f"{_prefix()} Chunk {fetch_ms:.0f}ms")
                            break

                        except FloodWait as e:
                            log.warning(f"{_prefix()} FloodWait {e.value}s [offset={offset}]")
                            await asyncio.sleep(e.value)
                            continue

                        except (TimeoutError, asyncio.TimeoutError) as e:
                            # Session went stale mid-stream — recover safely
                            retry_count += 1
                            wait_time = min(2 ** retry_count, 10)
                            log.error(
                                f"{_prefix()} ❌ asyncio TimeoutError mid-stream — "
                                f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s "
                                f"offset={offset} [DC={dc_id} session stale — rebuilding under lock]"
                            )
                            if retry_count >= max_retries:
                                log.error(
                                    f"{_prefix()} ❌ FATAL: asyncio TimeoutError after {max_retries} "
                                    f"retries at offset={offset} DC={dc_id} — video will stall"
                                )
                                raise
                            await asyncio.sleep(wait_time)
                            media_session = await self._recover_stale_session(
                                client, file_id, media_session, f"mid-stream-part{current_part}"
                            )
                            continue

                        except TelegramTimeout as e:
                            retry_count += 1
                            wait_time = min(2 ** retry_count, 10)
                            log.error(
                                f"{_prefix()} ❌ TelegramTimeout mid-stream — "
                                f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s "
                                f"offset={offset} [DC={dc_id} timeout]"
                            )
                            if retry_count >= max_retries:
                                log.error(
                                    f"{_prefix()} ❌ FATAL: TelegramTimeout after {max_retries} retries "
                                    f"at offset={offset} DC={dc_id} — video buffers indefinitely"
                                )
                                raise
                            await asyncio.sleep(wait_time)
                            continue

                        except InternalServerError as e:
                            retry_count += 1
                            wait_time = min(3 ** retry_count, 15)
                            log.error(
                                f"{_prefix()} ❌ Telegram InternalServerError mid-stream — "
                                f"error={e} retry={retry_count}/{max_retries} wait={wait_time}s "
                                f"offset={offset}"
                            )
                            if retry_count >= max_retries:
                                log.error(
                                    f"{_prefix()} ❌ FATAL: InternalServerError after {max_retries} "
                                    f"retries offset={offset} DC={dc_id}"
                                )
                                raise
                            await asyncio.sleep(wait_time)
                            continue

                        except Exception as e:
                            log.error(
                                f"{_prefix()} ❌ Unexpected error mid-stream — "
                                f"type={type(e).__name__} error={e} offset={offset}"
                            )
                            raise

            else:
                log.error(
                    f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                    f"Unexpected Telegram response type: {type(r).__name__} "
                    f"[GetFile did not return upload.File — expired file reference?]"
                )

        except TelegramTimeout as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — TelegramTimeout after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count} offset={offset} "
                f"[DC={dc_id} stopped responding — overload, expired reference, or network issue]"
            )
            raise
        except InternalServerError as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — InternalServerError after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count}"
            )
            raise
        except (TimeoutError, AttributeError) as e:
            total_elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED — {type(e).__name__}: {e} after {total_elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count} "
                f"[asyncio timeout or stale session — retries exhausted]"
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
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            log.debug("File ID cache cleared")
