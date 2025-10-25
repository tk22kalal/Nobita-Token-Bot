import math
import asyncio
import logging
from Adarsh.vars import Var
from typing import Dict, Union
from Adarsh.bot import work_loads
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid, FloodWait
from Adarsh.server.exceptions import FIleNotFound
from pyrogram.file_id import FileId, FileType, ThumbnailSource

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
            logging.debug(f"Cached file properties for message with ID {id}")
        return self.cached_file_ids[id]
    
    async def generate_file_properties(self, id: int) -> FileId:
        """
        Generates the properties of a media file on a specific message.
        returns ths properties in a FIleId class.
        """
        file_id = await get_file_ids(self.client, Var.BIN_CHANNEL, id)
        logging.debug(f"Generated file ID and Unique ID for message with ID {id}")
        if not file_id:
            logging.debug(f"Message with ID {id} not found")
            raise FIleNotFound
        self.cached_file_ids[id] = file_id
        logging.debug(f"Cached media message with ID {id}")
        return self.cached_file_ids[id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.
        """

        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
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

                for _ in range(6):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )

                    try:
                        await media_session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logging.debug(
                            f"Invalid authorization bytes for DC {file_id.dc_id}"
                        )
                        continue
                else:
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
            logging.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            logging.debug(f"Using cached media session for DC {file_id.dc_id}")
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
        
        # Calculate expected total bytes for verification
        expected_bytes = (part_count - 1) * chunk_size + last_part_cut - first_part_cut
        bytes_yielded = 0
        
        logging.info(f"Starting file stream: parts={part_count}, chunk_size={chunk_size}, "
                    f"offset={offset}, first_cut={first_part_cut}, last_cut={last_part_cut}, "
                    f"expected_bytes={expected_bytes}")
        
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)

        max_retries = 5

        async def _send_with_retries(loc, off, lim):
            """
            Helper to send GetFile requests with retries on connection errors.
            Recreates media session and retries a few times before giving up.
            FloodWait is re-raised to be handled by outer scope.
            """
            nonlocal media_session
            attempts = 0
            while True:
                try:
                    return await media_session.send(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=lim)
                    )
                except FloodWait:
                    # Let outer handler manage FloodWait (so sleeping and retrying whole generator)
                    raise
                except (OSError, ConnectionResetError, TimeoutError, asyncio.TimeoutError) as e:
                    attempts += 1
                    logging.warning(f"Transport error while getting file (attempt {attempts}): {e!r}")
                    # stop and remove the broken media session so it will be recreated
                    try:
                        await media_session.stop()
                    except Exception:
                        logging.debug("Error while stopping media session after connection error.", exc_info=True)
                    client.media_sessions.pop(file_id.dc_id, None)
                    if attempts > max_retries:
                        logging.error("Max retries reached while trying to recover media session.")
                        raise
                    await asyncio.sleep(2 ** attempts)
                    # recreate session
                    media_session = await self.generate_media_session(client, file_id)
                    continue

        try:
            # initial request (with retries)
            r = await _send_with_retries(location, offset, chunk_size)
            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes
                    if not chunk:
                        logging.error(f"Empty chunk received at part {current_part}/{part_count}, offset {offset}")
                        raise Exception(f"Empty chunk at part {current_part}, expected more data")
                    
                    # Yield the appropriate portion of the chunk
                    if part_count == 1:
                        chunk_data = chunk[first_part_cut:last_part_cut]
                    elif current_part == 1:
                        chunk_data = chunk[first_part_cut:]
                    elif current_part == part_count:
                        chunk_data = chunk[:last_part_cut]
                    else:
                        chunk_data = chunk
                    
                    bytes_yielded += len(chunk_data)
                    yield chunk_data

                    current_part += 1
                    offset += chunk_size

                    if current_part > part_count:
                        logging.info(f"Stream completed: {part_count} parts, {bytes_yielded}/{expected_bytes} bytes")
                        break

                    # Small delay to avoid flooding Telegram
                    await asyncio.sleep(0.1)
                    
                    # request next part (with retries)
                    r = await _send_with_retries(location, offset, chunk_size)
                    
                    # Validate the response
                    if not isinstance(r, raw.types.upload.File):
                        logging.error(f"Invalid response type at part {current_part}/{part_count}: {type(r)}")
                        raise Exception(f"Invalid response from Telegram at part {current_part}")
                    
                    if not r.bytes:
                        logging.error(f"No bytes in response at part {current_part}/{part_count}")
                        raise Exception(f"Empty response from Telegram at part {current_part}")
        except FloodWait as e:
            logging.warning(f"FloodWait: {e.value} seconds. Sleeping then retrying generator...")
            await asyncio.sleep(e.value)
            # After sleeping, resume streaming by delegating to a fresh call of this generator.
            async for inner_chunk in self.yield_file(
                file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
            ):
                yield inner_chunk
        except (TimeoutError, AttributeError) as exc:
            logging.error("Timeout or attribute error while streaming file - aborting transfer", exc_info=True)
            raise
        except Exception as exc:
            # Log and re-raise to ensure aiohttp properly aborts the transfer
            logging.exception("Unexpected error while yielding file: %s", exc)
            raise
        finally:
            if bytes_yielded < expected_bytes:
                logging.warning(f"Incomplete stream: yielded {bytes_yielded}/{expected_bytes} bytes "
                              f"({current_part - 1}/{part_count} parts)")
            logging.debug(f"Finished yielding file with {max(0, current_part - 1)} parts, {bytes_yielded} bytes")
            work_loads[index] -= 1

    
    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logging.debug("Cleaned the cache")
