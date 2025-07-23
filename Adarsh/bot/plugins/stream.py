import re
import os
import asyncio
import json
from Adarsh.bot import StreamBot
from Adarsh.utils.database import Database
from Adarsh.utils.human_readable import humanbytes
from Adarsh.vars import Var
from urllib.parse import quote_plus
from pyrogram import filters, Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from Adarsh.utils.file_properties import get_name, get_hash
from helper_func import encode, get_message_id, decode, get_messages

db = Database(Var.DATABASE_URL, Var.name)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "False") == "True"
DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'

async def process_message(msg, json_output, skipped_messages):
    """Process individual message and add to output with enhanced error handling"""
    try:
        # Validate media content
        if not (msg.document or msg.video or msg.audio):
            raise ValueError("No media content found in message")
        
        # Prepare caption with fallbacks
        if msg.caption:
            caption = msg.caption.html
            # Clean caption: remove URLs, mentions, hashtags and extra spaces
            caption = re.sub(r'(https?://\S+|@\w+|#\w+)', '', caption)
            caption = re.sub(r'\s+', ' ', caption).strip()
        else:
            # Fallback to filename if no caption
            caption = get_name(msg) or "NEXTPULSE"
        
        # Copy message to bin channel with FloodWait handling
        max_retries = 3
        for attempt in range(max_retries):
            try:
                log_msg = await msg.copy(
                    chat_id=Var.BIN_CHANNEL,
                    caption=caption[:1024],  # Ensure caption length limit
                    parse_mode=ParseMode.HTML
                )
                break
            except FloodWait as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(e.x)
                else:
                    raise
        else:
            raise Exception("Max retries exceeded for FloodWait")
        
        # Generate streaming URL
        file_name = get_name(log_msg) or "NEXTPULSE"
        file_hash = get_hash(log_msg)
        fqdn_url = Var.get_url_for_file(str(log_msg.id))
        stream_link = f"{fqdn_url}watch/{log_msg.id}/{quote_plus(file_name)}?hash={file_hash}"
        
        # Add to successful output (same format as before)
        json_output.append({
            "title": caption,
            "streamingUrl": stream_link
        })
        
    except Exception as e:
        # Capture details for skipped messages
        file_name = get_name(msg) or "Unknown"
        skipped_messages.append({
            "id": msg.id,
            "file_name": file_name,
            "reason": str(e)
        })

@StreamBot.on_message(filters.private & filters.user(list(Var.OWNER_ID)) & filters.command('batch'))
async def batch(client: Client, message: Message):
    Var.reset_batch()
    json_output = []
    skipped_messages = []

    # Get first and last messages
    try:
        first_message = await client.ask(
            text="Forward the First Message from DB Channel (with Quotes)..\n\nor Send the DB Channel Post Link",
            chat_id=message.from_user.id,
            filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
            timeout=60
        )
        f_msg_id = await get_message_id(client, first_message)
        if not f_msg_id:
            await first_message.reply("❌ Error\n\nInvalid Forward or Link. Try again.")
            return

        second_message = await client.ask(
            text="Forward the Last Message from DB Channel (with Quotes)..\n\nor Send the DB Channel Post Link",
            chat_id=message.from_user.id,
            filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
            timeout=60
        )
        s_msg_id = await get_message_id(client, second_message)
        if not s_msg_id:
            await second_message.reply("❌ Error\n\nInvalid Forward or Link. Try again.")
            return
    except Exception as e:
        await message.reply(f"❌ Setup Error: {str(e)}")
        return

    # Determine message range
    start_id = min(f_msg_id, s_msg_id)
    end_id = max(f_msg_id, s_msg_id)
    total_messages = end_id - start_id + 1
    status_msg = await message.reply_text(f"🚀 Starting batch processing...\nTotal: {total_messages} messages")
    
    # Process messages in batches
    batch_size = 50
    processed_count = 0
    
    for batch_start in range(start_id, end_id + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_id)
        msg_ids = list(range(batch_start, batch_end + 1))
        
        try:
            messages = await get_messages(client, msg_ids)
        except Exception as e:
            messages = []
            # If batch fetch fails, get messages individually
            for msg_id in msg_ids:
                try:
                    msg = (await get_messages(client, [msg_id]))[0]
                    messages.append(msg)
                except:
                    messages.append(None)
        
        for msg in messages:
            processed_count += 1
            if not msg:
                skipped_messages.append({
                    "id": msg_ids[messages.index(msg)] if msg in messages else "Unknown",
                    "file_name": "Unknown",
                    "reason": "Message not found"
                })
                continue
                
            await process_message(msg, json_output, skipped_messages)
            if processed_count % 10 == 0:
                await status_msg.edit_text(
                    f"🔄 Processing...\n"
                    f"Progress: {processed_count}/{total_messages}\n"
                    f"Success: {len(json_output)} | Skipped: {len(skipped_messages)}"
                )

    # Prepare final output
    output_data = {
        "successful": json_output,  # Same format as before
        "skipped": skipped_messages  # Separate section for errors
    }
    
    # Save to file
    filename = f"/tmp/batch_output_{message.from_user.id}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    # Send results
    await client.send_document(
        chat_id=message.chat.id,
        document=filename,
        caption=f"✅ Batch processing complete!\n"
                f"Total: {total_messages} | "
                f"Success: {len(json_output)} | "
                f"Skipped: {len(skipped_messages)}"
    )
    await status_msg.delete()
    os.remove(filename)


@StreamBot.on_message((filters.private) & (filters.document | filters.audio | filters.photo), group=3)
async def private_receive_handler(c: Client, m: Message):
    if bool(CUSTOM_CAPTION) and bool(m.document):
        caption = CUSTOM_CAPTION.format(
            previouscaption="" if not m.caption else m.caption.html,
            filename=m.video.file_name
        )
    else:
        caption = m.caption.html if m.caption else get_name(m.video)
    caption = re.sub(r'@[\w_]+|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
    caption = re.sub(r'\s+', ' ', caption.strip())
    caption = re.sub(r'\s*#\w+', '', caption)

    try:
        log_msg = await m.copy(chat_id=Var.BIN_CHANNEL)
        await asyncio.sleep(0.5)
        stream_link = f"{Var.URL}watch/{str(log_msg.id)}/{quote_plus(get_name(log_msg))}?hash={get_hash(log_msg)}"
        download_link = f"{Var.URL}{str(log_msg.id)}/{quote_plus(get_name(log_msg))}?hash={get_hash(log_msg)}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("STREAM ⏯️", url=stream_link)]])
        await log_msg.edit_reply_markup(reply_markup)        
        F_text = f"<tr><td>&lt;a href='{download_link}' target='_blank'&gt; {caption} &lt;/a&gt;</td></tr>"
        text = f"<tr><td>{F_text}</td></tr>"
        X = await m.reply_text(text=f"{text}", disable_web_page_preview=True, quote=True)
        await asyncio.sleep(3)
    except FloodWait as e:
        print(f"Sleeping for {str(e.x)}s")
        await asyncio.sleep(e.x)

@StreamBot.on_message((filters.private) & (filters.video | filters.audio | filters.photo), group=3)
async def private_receive_handler(c: Client, m: Message):
    if bool(CUSTOM_CAPTION) and bool(m.video):
        caption = CUSTOM_CAPTION.format(
            previouscaption="" if not m.caption else m.caption.html,
            filename=m.video.file_name
        )
    else:
        caption = m.caption.html if m.caption else get_name(m.video)
    caption = re.sub(r'@[\w_]+|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
    caption = re.sub(r'\s+', ' ', caption.strip())

    try:
        log_msg = await m.copy(chat_id=Var.BIN_CHANNEL)
        await asyncio.sleep(0.5)
        stream_link = f"{Var.URL}watch/{str(log_msg.id)}/{quote_plus(get_name(log_msg))}?hash={get_hash(log_msg)}"
        download_link = f"{Var.URL}{str(log_msg.id)}/{quote_plus(get_name(log_msg))}?hash={get_hash(log_msg)}"

        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("STREAM ⏯️", url=stream_link)]])
        await log_msg.edit_reply_markup(reply_markup)        
        F_text = f"<tr><td>&lt;a href='{stream_link}' target='_blank'&gt; {caption} &lt;/a&gt;</td></tr>"
        text = f"<tr><td>{F_text}</td></tr>"
        X = await m.reply_text(text=f"{text}", disable_web_page_preview=True, quote=True)
    except FloodWait as e:
        print(f"Sleeping for {str(e.x)}s")
        await asyncio.sleep(e.x)
