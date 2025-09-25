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
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from Adarsh.utils.file_properties import get_name, get_hash, get_media_from_message
from helper_func import encode, get_message_id, decode, get_messages

db = Database(Var.DATABASE_URL, Var.name)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "False") == "True"
DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'

async def create_intermediate_link(message: Message):
    """Create intermediate link for the message and store data temporarily"""
    # Extract file information
    media = get_media_from_message(message)
    if not media:
        raise ValueError("No media found in message")
    
    # Prepare caption
    caption = ""
    if message.caption:
        caption = message.caption.html
        caption = re.sub(r'@[\w_]+|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
        caption = re.sub(r'\s+', ' ', caption.strip())
        caption = re.sub(r'\s*#\w+', '', caption)
    
    if not caption:
        caption = get_name(message) or "NEXTPULSE"
    
    # Prepare message data for temporary storage
    message_data = {
        'message_id': message.id,
        'file_name': getattr(media, 'file_name', None) or get_name(message),
        'file_size': getattr(media, 'file_size', 0),
        'mime_type': getattr(media, 'mime_type', 'application/octet-stream'),
        'caption': caption,
        'from_chat_id': message.chat.id,
        'file_unique_id': getattr(media, 'file_unique_id', '')
    }
    
    # Store in database and get token
    token = await db.store_temp_file(message_data)
    
    # Create intermediate link
    intermediate_link = f"{Var.URL}prepare/{token}"
    
    return intermediate_link, caption

async def create_intermediate_link_for_batch(message: Message):
    """Create intermediate link for batch processing"""
    intermediate_link, caption = await create_intermediate_link(message)
    return {
        "title": caption,
        "streamingUrl": intermediate_link  # This is now an intermediate link, not a direct stream
    }

async def process_message(msg, json_output, skipped_messages):
    """Process individual message and create intermediate link (updated for new system)"""
    try:
        # Validate media content
        if not (msg.document or msg.video or msg.audio):
            raise ValueError("No media content found in message")
        
        # Create intermediate link instead of immediate stream generation
        intermediate_data = await create_intermediate_link_for_batch(msg)
        json_output.append(intermediate_data)
        
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
    try:
        # Create intermediate link instead of immediate stream generation
        intermediate_link, caption = await create_intermediate_link(m)
        
        # Create button with intermediate link
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("GENERATE STREAM 🎬", url=intermediate_link)]])
        
        # Send response with intermediate link
        response_text = f"📁 <b>{caption}</b>\n\n🔗 Click the button below to generate your stream link:"
        await m.reply_text(text=response_text, reply_markup=reply_markup, disable_web_page_preview=True, quote=True)
        
    except Exception as e:
        await m.reply_text(f"❌ Error processing file: {str(e)}", quote=True)
        print(f"Error in private_receive_handler: {e}")

@StreamBot.on_message((filters.private) & (filters.video | filters.audio | filters.photo), group=3)
async def private_receive_handler_video(c: Client, m: Message):
    try:
        # Create intermediate link instead of immediate stream generation
        intermediate_link, caption = await create_intermediate_link(m)
        
        # Create button with intermediate link
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("GENERATE STREAM 🎬", url=intermediate_link)]])
        
        # Send response with intermediate link
        response_text = f"🎥 <b>{caption}</b>\n\n🔗 Click the button below to generate your stream link:"
        await m.reply_text(text=response_text, reply_markup=reply_markup, disable_web_page_preview=True, quote=True)
        
    except Exception as e:
        await m.reply_text(f"❌ Error processing file: {str(e)}", quote=True)
        print(f"Error in private_receive_handler_video: {e}")
