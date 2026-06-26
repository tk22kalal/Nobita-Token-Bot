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

log = logging.getLogger("stream.downloader")


class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        self._session_locks: Dict[int, asyncio.Lock] = {}
        asyncio.create_task(self.clean_cache())

    def _dc_lock(self, dc_id: int) -> asyncio.Lock:
        if dc_id not in self._session_locks:
            self._session_locks[dc_id] = asyncio.Lock()
        return self._session_locks[dc_id]

    # ── File properties ───────────────────────────────────────────────────────

    async def get_file_properties(self, id: int) -> FileId:
        if id not in self.cached_file_ids:
            await self.generate_file_properties(id)
        return self.cached_file_ids[id]

    async def generate_file_properties(self, id: int) -> FileId:
        log.info(f"[MSG={id}] Fetching file properties from BIN_CHANNEL …")
        t0 = time.monotonic()
        file_id = await get_file_ids(self.client, Var.BIN_CHANNEL, id)
        elapsed = time.monotonic() - t0
        if not file_id:
            log.error(f"[MSG={id}] File not found in BIN_CHANNEL ({elapsed:.2f}s)")
            raise FIleNotFound
        log.info(
            f"[MSG={id}] Properties OK in {elapsed:.2f}s — "
            f"dc_id={file_id.dc_id} file_size={file_id.file_size} "
            f"mime={getattr(file_id, 'mime_type', '?')} "
            f"unique_id={file_id.unique_id[:12]}…"
        )
        self.cached_file_ids[id] = file_id
        return file_id

    async def refresh_file_properties(self, msg_id: int) -> FileId:
        """
        Force-evict cached file_id and re-fetch from BIN_CHANNEL to renew
        the file_reference token (which expires and causes GetFile to hang).
        """
        log.info(
            f"[MSG={msg_id}] 🔄 Refreshing file_reference — "
            f"evicting cached entry and re-fetching from BIN_CHANNEL"
        )
        self.cached_file_ids.pop(msg_id, None)
        return await self.generate_file_properties(msg_id)

    # ── Session management ────────────────────────────────────────────────────

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        dc_id = file_id.dc_id
        async with self._dc_lock(dc_id):
            return await self._create_or_get_session(client, file_id)

    async def _create_or_get_session(self, client: Client, file_id: FileId) -> Session:
        """
        Return a cached media session for the given DC, or build a fresh one.

        Session strategy (matches biisal's proven working approach):
          - Same DC as client's home: reuse client.storage.auth_key() directly.
            No ExportAuthorization / ImportAuthorization is needed because the
            client is already fully authenticated to its own DC.
          - Cross DC: create a brand-new auth key via Auth().create(), then
            ExportAuthorization from the main session and ImportAuthorization
            into the new session so Telegram grants it download rights.

        Why NOT always-ExportAuth (previous Nobita approach):
            Doing ExportAuthorization(dc_id=home_dc) on the same DC as the main
            session can silently fail or be rate-limited for certain account/DC
            states, causing GetFile to hang indefinitely.  Biisal avoids this
            by simply reusing auth_key() for same-DC sessions.
        """
        dc_id = file_id.dc_id
        media_session = client.media_sessions.get(dc_id)
        if media_session is not None:
            log.debug(f"[DC={dc_id}] Reusing cached media session")
            return media_session

        client_dc = await client.storage.dc_id()
        log.info(
            f"[DC={dc_id}] No cached session — creating new media session "
            f"(client_dc={client_dc}, same_dc={dc_id == client_dc})"
        )
        t0 = time.monotonic()

        if dc_id != client_dc:
            # Cross-DC: fresh key + ExportAuthorization/ImportAuthorization
            media_session = Session(
                client, dc_id,
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
                    log.info(
                        f"[DC={dc_id}] Auth imported on attempt {attempt + 1} "
                        f"(cross-DC from home_dc={client_dc})"
                    )
                    break
                except AuthBytesInvalid:
                    log.warning(
                        f"[DC={dc_id}] AuthBytesInvalid attempt {attempt + 1}/6 — retrying"
                    )
                    continue
            else:
                log.error(
                    f"[DC={dc_id}] All 6 ImportAuthorization attempts failed — stopping session"
                )
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            # Same DC: reuse the client's own auth key directly — no Export/Import needed
            media_session = Session(
                client, dc_id,
                await client.storage.auth_key(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()
            log.info(f"[DC={dc_id}] Same-DC session created using existing auth_key")

        elapsed = time.monotonic() - t0
        log.info(f"[DC={dc_id}] Media session ready in {elapsed:.2f}s")
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
        Uses the per-DC lock and identity check to avoid killing a session
        that was already rebuilt by a concurrent coroutine.
        """
        dc_id = file_id.dc_id
        async with self._dc_lock(dc_id):
            cached = client.media_sessions.get(dc_id)
            if cached is stale_session:
                del client.media_sessions[dc_id]
                log.info(f"[DC={dc_id}] [{context}] Removed stale session — stopping it …")
                try:
                    await stale_session.stop()
                except Exception as e:
                    log.warning(f"[DC={dc_id}] [{context}] Error stopping stale session (ignored): {e}")
            elif cached is not None:
                log.info(f"[DC={dc_id}] [{context}] Session already replaced by another coroutine — reusing")
                return cached
            else:
                log.info(f"[DC={dc_id}] [{context}] Session already evicted — building fresh one")
            return await self._create_or_get_session(client, file_id)

    # ── File location ─────────────────────────────────────────────────────────

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
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            return raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

    # ── Main streaming generator ──────────────────────────────────────────────

    async def yield_file(
        self,
        file_id: FileId,
        msg_id: int,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[str, None]:
        """
        Async generator that yields raw bytes of a Telegram media file.

        On TimeoutError:
          1. The stale DC session is dropped and rebuilt.
          2. The cached file_reference is evicted and re-fetched from BIN_CHANNEL
             (expired file_reference causes GetFile to hang on Telegram's side).
        """
        client = self.client
        work_loads[index] += 1

        media_id = getattr(file_id, 'media_id', '?')
        file_size = getattr(file_id, 'file_size', '?')
        dc_id = file_id.dc_id

        log.info(
            f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
            f"yield_file START — msg_id={msg_id} offset={offset} part_count={part_count} "
            f"chunk={chunk_size//1024}KB first_cut={first_part_cut} last_cut={last_part_cut} "
            f"file_size={file_size}"
        )
        stream_start = time.monotonic()

        media_session = await self.generate_media_session(client, file_id)
        location = await self.get_location(file_id)

        current_part = 1
        bytes_yielded = 0

        def _p():
            return (
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"[PART={current_part}/{part_count}] [OFFSET={offset}]"
            )

        async def _handle_timeout(e, context_label, retry_count, max_retries):
            """
            Shared timeout recovery: rebuild session AND refresh file_reference.
            Returns (new_media_session, new_location).
            """
            nonlocal file_id
            wait_time = min(2 ** retry_count, 10)
            log.error(
                f"{_p()} ❌ asyncio TimeoutError [{context_label}] — "
                f"retry={retry_count}/{max_retries} wait={wait_time}s"
            )
            if retry_count >= max_retries:
                log.error(
                    f"{_p()} ❌ FATAL: TimeoutError after {max_retries} retries — stream cannot be recovered"
                )
                raise e
            await asyncio.sleep(wait_time)

            # Step 1: rebuild the DC session (stale TCP connection)
            new_session = await self._recover_stale_session(
                client, file_id, media_session, context_label
            )

            # Step 2: refresh file_reference (expired token causes GetFile to hang)
            try:
                file_id = await self.refresh_file_properties(msg_id)
                new_location = await self.get_location(file_id)
                log.info(
                    f"{_p()} ✅ file_reference refreshed for msg={msg_id}"
                )
            except Exception as ref_err:
                log.error(
                    f"{_p()} ❌ Failed to refresh file_reference: {ref_err} — using old location"
                )
                new_location = location

            return new_session, new_location

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
                    log.info(f"{_p()} First chunk in {fetch_ms:.0f}ms (retry={retry_count})")
                    if fetch_ms > 5000:
                        log.warning(f"{_p()} ⚠ SLOW first chunk {fetch_ms:.0f}ms DC={dc_id}")
                    break

                except FloodWait as e:
                    log.warning(f"{_p()} FloodWait {e.value}s")
                    await asyncio.sleep(e.value)
                    continue

                except (TimeoutError, asyncio.TimeoutError) as e:
                    retry_count += 1
                    media_session, location = await _handle_timeout(
                        e, "first-chunk", retry_count, max_retries
                    )
                    continue

                except TelegramTimeout as e:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, 10)
                    log.error(
                        f"{_p()} ❌ TelegramTimeout first chunk — "
                        f"retry={retry_count}/{max_retries} wait={wait_time}s"
                    )
                    if retry_count >= max_retries:
                        raise
                    await asyncio.sleep(wait_time)
                    continue

                except InternalServerError as e:
                    retry_count += 1
                    wait_time = min(3 ** retry_count, 15)
                    log.error(
                        f"{_p()} ❌ InternalServerError first chunk — "
                        f"retry={retry_count}/{max_retries} wait={wait_time}s"
                    )
                    if retry_count >= max_retries:
                        raise
                    await asyncio.sleep(wait_time)
                    continue

                except Exception as e:
                    log.error(f"{_p()} ❌ Unexpected error first chunk — {type(e).__name__}: {e}")
                    raise

            # ── Yield loop ───────────────────────────────────────────────────
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        log.warning(
                            f"{_p()} Empty chunk — DC={dc_id} offset={offset} "
                            f"[possible EOF or bad file_reference]"
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

                    log.debug(f"{_p()} Yielded {chunk_len//1024}KB total={bytes_yielded//1024}KB")

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
                                log.warning(f"{_p()} ⚠ SLOW chunk {fetch_ms:.0f}ms DC={dc_id}")
                            else:
                                log.debug(f"{_p()} Chunk {fetch_ms:.0f}ms")
                            break

                        except FloodWait as e:
                            log.warning(f"{_p()} FloodWait {e.value}s offset={offset}")
                            await asyncio.sleep(e.value)
                            continue

                        except (TimeoutError, asyncio.TimeoutError) as e:
                            retry_count += 1
                            media_session, location = await _handle_timeout(
                                e, f"mid-stream-part{current_part}", retry_count, max_retries
                            )
                            continue

                        except TelegramTimeout as e:
                            retry_count += 1
                            wait_time = min(2 ** retry_count, 10)
                            log.error(
                                f"{_p()} ❌ TelegramTimeout mid-stream — "
                                f"retry={retry_count}/{max_retries} wait={wait_time}s"
                            )
                            if retry_count >= max_retries:
                                raise
                            await asyncio.sleep(wait_time)
                            continue

                        except InternalServerError as e:
                            retry_count += 1
                            wait_time = min(3 ** retry_count, 15)
                            log.error(
                                f"{_p()} ❌ InternalServerError mid-stream — "
                                f"retry={retry_count}/{max_retries} wait={wait_time}s"
                            )
                            if retry_count >= max_retries:
                                raise
                            await asyncio.sleep(wait_time)
                            continue

                        except Exception as e:
                            log.error(
                                f"{_p()} ❌ Unexpected mid-stream error — "
                                f"{type(e).__name__}: {e} offset={offset}"
                            )
                            raise

            else:
                log.error(
                    f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                    f"Unexpected Telegram response type: {type(r).__name__} "
                    f"[GetFile did not return upload.File — expired file_reference?]"
                )

        except TelegramTimeout as e:
            elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED TelegramTimeout after {elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB part={current_part}/{part_count}"
            )
            raise
        except InternalServerError as e:
            elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED InternalServerError after {elapsed:.1f}s"
            )
            raise
        except (TimeoutError, AttributeError) as e:
            elapsed = time.monotonic() - stream_start
            log.error(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"❌ STREAM ABORTED {type(e).__name__} after {elapsed:.1f}s "
                f"yielded={bytes_yielded//1024}KB [retries exhausted]"
            )
            pass
        finally:
            elapsed = time.monotonic() - stream_start
            log.info(
                f"[CLIENT={index}] [DC={dc_id}] [MEDIA={media_id}] "
                f"yield_file END — parts={current_part-1}/{part_count} "
                f"yielded={bytes_yielded//1024}KB "
                f"time={elapsed:.2f}s "
                f"speed={bytes_yielded/(elapsed*1024) if elapsed > 0 else 0:.0f}KB/s"
            )
            work_loads[index] -= 1

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            log.debug("File ID cache cleared")
