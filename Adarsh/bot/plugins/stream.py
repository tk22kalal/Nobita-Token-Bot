import re
import os
from os import getenv, environ
from dotenv import load_dotenv
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
GIT_TOKEN = int(getenv('GIT_TOKEN', ''))

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

async def upload_to_github(file_content: str, file_path: str, commit_message: str, token: str) -> bool:
    """Upload JSON file to GitHub repository.

    file_path should be: owner/repo/path/to/folder/filename.json
    """
    import base64
    import requests

    try:
        # Normalize and split the provided path
        parts = file_path.strip('/').split('/')
        if len(parts) < 3:
            # need at least owner/repo/file.json
            raise ValueError("file_path must be in the form owner/repo/path/to/file.json")
        owner = parts[0]
        repo = parts[1]
        path = '/'.join(parts[2:])

        # GitHub API endpoint for create/update contents
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
        
        # Encode content to base64
        content_encoded = base64.b64encode(file_content.encode()).decode()
        
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Check if file exists to get SHA
        response = requests.get(api_url, headers=headers)
        sha = None
        if response.status_code == 200:
            sha = response.json().get('sha')
        elif response.status_code not in (404,):
            # unexpected error, bubble it up for logs
            print(f"GitHub API GET returned {response.status_code}: {response.text}")
        
        # Prepare data
        data = {
            "message": commit_message,
            "content": content_encoded
        }
        if sha:
            data["sha"] = sha
        
        # Upload file
        response = requests.put(api_url, headers=headers, json=data)
        if response.status_code in [200, 201]:
            return True
        else:
            print(f"GitHub API PUT failed {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"Error uploading to GitHub: {e}")
        return False

@StreamBot.on_message(filters.private & filters.user(list(Var.OWNER_ID)) & filters.command('batch'))
async def batch(client: Client, message: Message):
    Var.reset_batch()
    
    try:
        # Step 1: Ask for GitHub destination folder
        dest_folder_msg = await client.ask(
            text="📁 Enter the GitHub destination folder path:\n\nExample: marrow/anatomy\nFormat: owner/repo/path/to/folder",
            chat_id=message.from_user.id,
            filters=filters.text,
            timeout=60
        )
        github_dest_folder = dest_folder_msg.text.strip()
        
        # Step 2: Ask for links with subjects
        links_msg = await client.ask(
            text="📝 Send the links with subjects in this format:\n\n"
                 "ANATOMY\nF - https://t.me/c/2024354927/237364\nL - https://t.me/c/2024354927/237366\n\n"
                 "BIOCHEMISTRY\nF - https://t.me/c/2024354927/237460\nL - https://t.me/c/2024354927/237462\n\n"
                 "Each subject should have F (first) and L (last) message links.",
            chat_id=message.from_user.id,
            filters=filters.text,
            timeout=120
        )
        
        # Parse the input
        links_text = links_msg.text.strip()
        subjects_data = []
        current_subject = None
        current_first = None
        current_last = None
        
        for line in links_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Check if it's a subject name (line without F - or L -)
            if not line.startswith('F -') and not line.startswith('L -'):
                # Save previous subject if exists
                if current_subject and current_first and current_last:
                    subjects_data.append({
                        'subject': current_subject,
                        'first': current_first,
                        'last': current_last
                    })
                current_subject = line
                current_first = None
                current_last = None
            elif line.startswith('F -'):
                current_first = line.replace('F -', '').strip()
            elif line.startswith('L -'):
                current_last = line.replace('L -', '').strip()
        
        # Add last subject
        if current_subject and current_first and current_last:
            subjects_data.append({
                'subject': current_subject,
                'first': current_first,
                'last': current_last
            })
        
        if not subjects_data:
            await message.reply("❌ No valid subjects found in the input. Please check the format.")
            return
        
        # Get GitHub token from environment
        git_token = os.environ.get('GIT_TOKEN', '')
        if not git_token:
            await message.reply("❌ GIT_TOKEN not found in environment variables. Please add it to repo secrets.")
            return
        
        status_msg = await message.reply_text(f"🚀 Starting batch processing for {len(subjects_data)} subjects...")
        
        # Process each subject
        for idx, subject_info in enumerate(subjects_data, 1):
            subject_name = subject_info['subject']
            json_output = []
            skipped_messages = []
            
            try:
                # Get first and last message IDs
                f_msg_id = await get_message_id(client, type('obj', (object,), {'text': subject_info['first']})())
                s_msg_id = await get_message_id(client, type('obj', (object,), {'text': subject_info['last']})())
                
                if not f_msg_id or not s_msg_id:
                    await status_msg.edit_text(f"❌ Invalid message IDs for {subject_name}")
                    continue
                
                # Determine message range
                start_id = min(f_msg_id, s_msg_id)
                end_id = max(f_msg_id, s_msg_id)
                total_messages = end_id - start_id + 1
                
                await status_msg.edit_text(
                    f"🔄 Processing {subject_name}...\n"
                    f"Subject {idx}/{len(subjects_data)}\n"
                    f"Messages: {total_messages}"
                )
                
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
                                "id": "Unknown",
                                "file_name": "Unknown",
                                "reason": "Message not found"
                            })
                            continue
                            
                        await process_message(msg, json_output, skipped_messages)
                
                # Prepare JSON output
                output_data = {
                    "successful": json_output,
                    "skipped": skipped_messages
                }
                
                # Save to file
                json_filename = f"{subject_name}.json"
                json_content = json.dumps(output_data, indent=4, ensure_ascii=False)
                
                # Upload to GitHub
                # github_dest_folder expected: owner/repo/path/to/folder
                # final github_file_path will be owner/repo/path/to/folder/filename.json
                github_file_path = f"{github_dest_folder}/{json_filename}".replace('//', '/')
                commit_msg = f"Add {json_filename} - {len(json_output)} files"
                
                upload_success = await asyncio.to_thread(
                    upload_to_github,
                    json_content,
                    github_file_path,
                    commit_msg,
                    git_token
                )
                
                if upload_success:
                    await status_msg.edit_text(
                        f"✅ {subject_name} completed!\n"
                        f"Uploaded: {json_filename}\n"
                        f"Success: {len(json_output)} | Skipped: {len(skipped_messages)}\n\n"
                        f"Progress: {idx}/{len(subjects_data)}"
                    )
                else:
                    await status_msg.edit_text(
                        f"❌ Failed to upload {subject_name} to GitHub\n"
                        f"Progress: {idx}/{len(subjects_data)}"
                    )
                
                # Small delay between subjects
                await asyncio.sleep(1)
                
            except Exception as e:
                await status_msg.edit_text(f"❌ Error processing {subject_name}: {str(e)}")
                continue
        
        await status_msg.edit_text(f"✅ All {len(subjects_data)} subjects processed and uploaded to GitHub!")
        
    except asyncio.TimeoutError:
        await message.reply("⏱️ Request timeout. Please try again.")
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")


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
