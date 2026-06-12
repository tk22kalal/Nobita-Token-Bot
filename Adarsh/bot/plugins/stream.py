import re
import os
import asyncio
import json
import logging
from pathlib import Path
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
from Adarsh.utils.thumbnail_extractor import extract_thumbnail_from_middle
from Adarsh.utils.github_uploader import upload_image_to_github

db = Database(Var.DATABASE_URL, Var.name)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", None)
PROTECT_CONTENT = os.environ.get('PROTECT_CONTENT', "False") == "True"
DISABLE_CHANNEL_BUTTON = os.environ.get("DISABLE_CHANNEL_BUTTON", None) == 'True'
GIT_TOKEN = os.environ.get('GIT_TOKEN', '')
THUMB_API = os.environ.get('THUMB_API', '')

def sanitize_caption(text: str) -> str:
    """Sanitize caption by removing HTML tags, links, @mentions, and hashtags"""
    if not text:
        return text
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove @mentions
    text = re.sub(r'@[\w_]+', '', text)
    # Remove all kinds of links
    text = re.sub(r'(?:https?://|t\.me/|telegram\.me/)[^\s]+', '', text)
    # Remove hashtags
    text = re.sub(r'\s*#\w+', '', text)
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text.strip())
    return text

async def create_intermediate_link(message: Message):
    """Create intermediate link for the message and store data temporarily.
    Uses the current domain's BASE_URL for complete domain independence."""
    # Extract file information
    media = get_media_from_message(message)
    if not media:
        raise ValueError("No media found in message")

    # Get caption with fallback chain and sanitization
    caption = ""
    
    # Try message caption first
    if message.caption:
        caption = sanitize_caption(message.caption.html)
    
    # Fallback to filename if caption is empty after sanitization
    if not caption or not caption.strip():
        filename = getattr(media, 'file_name', None) or get_name(message)
        if filename:
            caption = sanitize_caption(filename)
    
    # Fallback to random name if still empty
    if not caption or not caption.strip():
        import secrets
        caption = f"file_{secrets.token_hex(4)}"
    
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
    
    # Store with current domain for domain independence
    current_domain = Var.get_current_domain()
    token = await db.store_temp_file(message_data, domain=current_domain)
    
    # Use the current instance's BASE_URL (not hardcoded URL_WEB)
    base_url = Var.get_base_url()
    intermediate_link = f"{base_url}prepare/{token}"
    
    return intermediate_link, caption

async def create_intermediate_link_for_batch(message: Message, folder_name: str = None, client: Client = None, shared_thumbnail_url: str = None):
    """Create intermediate links for batch processing - both stream and download, with optional thumbnail.
    
    DOMAIN INDEPENDENCE: Each deployment generates links ONLY for its own domain.
    Set SERVE_DOMAIN='web' or SERVE_DOMAIN='webx' on each Heroku instance.
    If DUAL_DOMAIN_ENABLED=True and no SERVE_DOMAIN set, generates for both (legacy mode)."""
    try:
        media = get_media_from_message(message)
        if not media:
            raise ValueError("No media found in message")

        # Get caption with fallback chain and sanitization
        caption = ""
        
        # Try message caption first
        if message.caption:
            caption = sanitize_caption(message.caption.html)
        
        # Fallback to filename if caption is empty after sanitization
        if not caption or not caption.strip():
            filename = getattr(media, 'file_name', None) or get_name(message)
            if filename:
                caption = sanitize_caption(filename)
        
        # Fallback to random name if still empty
        if not caption or not caption.strip():
            import secrets
            caption = f"file_{secrets.token_hex(4)}"
        
        message_data = {
            'message_id': message.id,
            'file_name': getattr(media, 'file_name', None) or get_name(message),
            'file_size': getattr(media, 'file_size', 0),
            'mime_type': getattr(media, 'mime_type', 'application/octet-stream'),
            'caption': caption,
            'from_chat_id': message.chat.id,
            'file_unique_id': getattr(media, 'file_unique_id', '')
        }
        
        # Extract and upload thumbnail for video files BEFORE storing in database
        mime_type = getattr(media, 'mime_type', '')
        thumbnail_url = shared_thumbnail_url  # Use shared thumbnail if provided
        
        # Log thumbnail processing conditions
        logging.info(f"Thumbnail check - mime_type: {mime_type}, folder_name: {folder_name}, THUMB_API: {'Present' if THUMB_API else 'Missing'}, client: {'Present' if client else 'Missing'}, shared_thumbnail: {'Present' if shared_thumbnail_url else 'None'}")
        
        # Only extract thumbnail if we don't have a shared one AND this is a video
        if not shared_thumbnail_url and mime_type and mime_type.startswith('video/') and folder_name and THUMB_API and client:
            temp_video_path = None
            thumbnail_path = None
            try:
                logging.info(f"🎬 Starting thumbnail extraction for video: {caption}")
                logging.info(f"   Video mime type: {mime_type}")
                logging.info(f"   Folder name: {folder_name}")
                
                # Download video temporarily
                temp_dir = Path("/tmp/batch_videos")
                temp_dir.mkdir(exist_ok=True)
                import secrets as sec
                temp_video_path = str(temp_dir / f"video_{sec.token_hex(8)}.mp4")
                
                logging.info(f"   Downloading video to: {temp_video_path}")
                # Download the video file
                await client.download_media(message, file_name=temp_video_path)
                
                video_size = os.path.getsize(temp_video_path) if os.path.exists(temp_video_path) else 0
                logging.info(f"   Video downloaded successfully ({video_size} bytes)")
                
                # Extract thumbnail from middle of video
                logging.info(f"   Extracting thumbnail from video...")
                thumbnail_path = await extract_thumbnail_from_middle(temp_video_path)
                
                thumb_size = os.path.getsize(thumbnail_path) if os.path.exists(thumbnail_path) else 0
                logging.info(f"   Thumbnail extracted successfully: {thumbnail_path} ({thumb_size} bytes)")
                
                # Upload thumbnail to GitHub
                logging.info(f"   Uploading thumbnail to GitHub (folder: {folder_name})...")
                thumbnail_url = await upload_image_to_github(
                    image_path=thumbnail_path,
                    github_token=THUMB_API,
                    folder_name=folder_name,
                    title_name=caption
                )
                
                logging.info(f"✅ Thumbnail uploaded successfully: {thumbnail_url}")
                    
            except Exception as thumb_error:
                thumb_err_str = str(thumb_error)
                logging.error(f"❌ Thumbnail failed for '{caption}': {thumb_err_str}", exc_info=True)
                # Store the first thumbnail error so the batch loop can report it to the bot
                if not message_data.get('_thumb_error'):
                    message_data['_thumb_error'] = thumb_err_str
            finally:
                # Always cleanup temporary files, even on failure
                try:
                    if temp_video_path and os.path.exists(temp_video_path):
                        os.remove(temp_video_path)
                        logging.debug(f"Cleaned up temp video: {temp_video_path}")
                    if thumbnail_path and os.path.exists(thumbnail_path):
                        os.remove(thumbnail_path)
                        logging.debug(f"Cleaned up thumbnail: {thumbnail_path}")
                except Exception as cleanup_error:
                    logging.error(f"Error cleaning up temp files: {cleanup_error}")
        else:
            # Log why thumbnail extraction was skipped
            reasons = []
            if not mime_type or not mime_type.startswith('video/'):
                reasons.append(f"not a video (mime: {mime_type})")
            if not folder_name:
                reasons.append("no folder_name provided")
            if not THUMB_API:
                reasons.append("THUMB_API not configured")
            if not client:
                reasons.append("no client provided")
            if reasons:
                logging.info(f"⏭️  Skipping thumbnail for {caption}: {', '.join(reasons)}")
        
        if thumbnail_url:
            message_data['thumbnail_url'] = thumbnail_url
        
        # DOMAIN INDEPENDENCE: Get current domain and base URL
        current_domain = Var.get_current_domain()
        base_url = Var.get_base_url()
        
        # If SERVE_DOMAIN is set (web or webx), only create token for THIS domain
        # This ensures complete independence - each Heroku app handles its own domain
        if current_domain:
            token = await db.store_temp_file(message_data, domain=current_domain)
            stream_link = f"{base_url}prepare/{token}?type=stream"
            download_link = f"{base_url}prepare/{token}?type=download"
            
            result = {
                "title": caption,
                "streamingUrl": stream_link,
                "downloadUrl": download_link
            }
        else:
            # Legacy mode: if no SERVE_DOMAIN set, create tokens for both (backwards compatible)
            token_web = await db.store_temp_file(message_data, domain='web')
            token_webx = await db.store_temp_file(message_data, domain='webx')
            
            stream_link = f"{Var.URL_WEB}prepare/{token_web}?type=stream"
            stream_link_x = f"{Var.URL_WEBX}prepare/{token_webx}?type=stream"
            download_link = f"{Var.URL_WEB}prepare/{token_web}?type=download"
            download_link_x = f"{Var.URL_WEBX}prepare/{token_webx}?type=download"
            
            result = {
                "title": caption,
                "streamingUrl": stream_link,
                "streamingUrlx": stream_link_x,
                "downloadUrl": download_link,
                "downloadUrlx": download_link_x
            }
        
        if thumbnail_url:
            result["thumbnailUrl"] = thumbnail_url

        # Carry thumb error forward so the batch loop can surface it to the bot
        thumb_err = message_data.get('_thumb_error')
        if thumb_err:
            result["_thumb_error"] = thumb_err

        return result
    except Exception as e:
        raise ValueError(f"Failed to create intermediate links: {str(e)}")

async def create_pdf_download_links(message: Message):
    """Create download-only links for PDF files. No streaming URL, no thumbnail."""
    media = get_media_from_message(message)
    if not media:
        raise ValueError("No media found in message")

    # Title priority: caption → filename → random
    title = ""
    if message.caption:
        title = sanitize_caption(message.caption.html)
    if not title or not title.strip():
        filename = getattr(media, 'file_name', None) or get_name(message)
        if filename:
            title = sanitize_caption(filename)
    if not title or not title.strip():
        import secrets as _sec
        title = f"pdf_{_sec.token_hex(4)}"

    message_data = {
        'message_id': message.id,
        'file_name': getattr(media, 'file_name', None) or get_name(message),
        'file_size': getattr(media, 'file_size', 0),
        'mime_type': getattr(media, 'mime_type', 'application/pdf'),
        'caption': title,
        'from_chat_id': message.chat.id,
        'file_unique_id': getattr(media, 'file_unique_id', '')
    }

    current_domain = Var.get_current_domain()

    if current_domain:
        token = await db.store_temp_file(message_data, domain=current_domain)
        base_url = Var.get_base_url()
        download_link = f"{base_url}prepare/{token}?type=download"
        if current_domain == 'web':
            return {"title": title, "pdf_downloadUrl": download_link}
        else:
            return {"title": title, "pdf_downloadUrlx": download_link}
    else:
        # Legacy mode: generate for both domains
        token_web = await db.store_temp_file(message_data, domain='web')
        token_webx = await db.store_temp_file(message_data, domain='webx')
        return {
            "title": title,
            "pdf_downloadUrl": f"{Var.URL_WEB}prepare/{token_web}?type=download",
            "pdf_downloadUrlx": f"{Var.URL_WEBX}prepare/{token_webx}?type=download"
        }


async def process_message(msg, json_output, skipped_messages, folder_name=None, client=None, shared_thumbnail_url=None):
    """Process individual message and create intermediate link (updated for new system with thumbnail support)"""
    try:
        # Silently skip plain text messages (no media at all)
        if not (msg.document or msg.video or msg.audio):
            return

        # PDF documents → download-only links, no streaming or thumbnail
        is_pdf = (
            msg.document and
            getattr(msg.document, 'mime_type', '') == 'application/pdf'
        )

        if is_pdf:
            pdf_data = await create_pdf_download_links(msg)
            json_output.append(pdf_data)
            return

        # Videos / audio / other documents → full streaming + download links
        intermediate_data = await create_intermediate_link_for_batch(msg, folder_name, client, shared_thumbnail_url)
        json_output.append(intermediate_data)

    except Exception as e:
        # Capture details for skipped messages
        file_name = get_name(msg) or "Unknown"
        skipped_messages.append({
            "id": msg.id,
            "file_name": file_name,
            "reason": str(e)
        })

def generate_lecture_html(json_filename: str) -> str:
    """Generate the lecture HTML page for a given JSON filename."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Lectures - NEXTPULSE | NEET PG Preparation</title>
  <meta name="description" content="Access comprehensive video lectures. Expert faculty for NEET PG preparation.">
  <meta name="keywords" content="NEET PG, medical lectures, video lectures">
  <link rel="stylesheet" href="../../styles.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
  <style>
    .completion-toggle {{
      position: absolute;
      top: 1rem;
      right: 1rem;
      cursor: pointer;
      z-index: 10;
    }}

    .completion-circle {{
      width: 32px;
      height: 32px;
      border: 2px solid #e2e8f0;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      background: white;
      transition: all 0.3s ease;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }}

    .completion-circle:hover {{
      border-color: #4299e1;
      transform: scale(1.05);
    }}

    .completion-circle.completed {{
      background: #48bb78;
      border-color: #48bb78;
    }}

    .completion-circle .fas.fa-check {{
      color: white;
      font-size: 14px;
      opacity: 0;
      transition: opacity 0.3s ease;
    }}

    .completion-circle .fas.fa-check.visible {{
      opacity: 1;
    }}

    .lecture-card {{
      position: relative;
    }}

    .video-popup {{
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.8);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 1000;
    }}

    .video-popup-content {{
      background: white;
      border-radius: 8px;
      width: 90%;
      max-width: 800px;
      max-height: 90vh;
      overflow: auto;
      position: relative;
      padding: 20px;
    }}

    .refresh-popup {{
      position: absolute;
      top: 10px;
      left: 15px;
      background: white;
      border: none;
      border-radius: 50%;
      width: 32px;
      height: 32px;
      font-size: 16px;
      cursor: pointer;
      color: #333;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 2px 5px rgba(0,0,0,0.2);
      transition: all 0.3s ease;
    }}

    .refresh-popup:hover {{
      background: #f0f0f0;
      transform: scale(1.1);
    }}

    .close-popup {{
      position: absolute;
      top: 10px;
      right: 15px;
      background: white;
      border: none;
      border-radius: 50%;
      width: 32px;
      height: 32px;
      font-size: 20px;
      cursor: pointer;
      color: #333;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 2px 5px rgba(0,0,0,0.2);
      transition: all 0.3s ease;
    }}

    .close-popup:hover {{
      background: #f0f0f0;
      transform: scale(1.1);
    }}

    .video-title {{
      margin-top: 0;
      margin-bottom: 15px;
      color: #2c3e50;
    }}

    .iframe-container {{
      position: relative;
      width: 100%;
      height: 0;
      padding-bottom: 56.25%;
    }}

    .iframe-container iframe {{
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      border: none;
      border-radius: 4px;
    }}

    .lecture-card {{
      background: white;
      border-radius: 8px;
      padding: 20px;
      margin-bottom: 15px;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
      transition: transform 0.2s;
    }}

    .lecture-card:hover {{
      transform: translateY(-3px);
    }}

    .lecture-card h3 {{
      margin-top: 0;
      margin-bottom: 15px;
      color: #2c3e50;
    }}

    .button-container {{
      display: flex;
      gap: 10px;
    }}

    .stream-button, .download-button {{
      padding: 8px 16px;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-weight: 600;
      display: flex;
      align-items: center;
      gap: 5px;
      transition: background-color 0.2s;
    }}

    .stream-button {{
      background-color: #3498db;
      color: white;
    }}

    .stream-button:hover {{
      background-color: #2980b9;
    }}

    .download-button {{
      background-color: #2ecc71;
      color: white;
    }}

    .download-button:hover {{
      background-color: #27ae60;
    }}
  </style>
  <script src="../../access-control.js"></script>
  <script src="../../block.js"></script>
  <script src="../../error-handler/link-checker.js"></script>
  <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-5920367457745298"
     crossorigin="anonymous"></script>
</head>
<body>
<script>
  if (!accessControl.initProtectedPage()) {{
    throw new Error('Access denied - redirecting to index.html');
  }}
</script>
  <header>
    <div class="header-content">
      <button onclick="history.back()" style="position: fixed; top: 20px; left: 20px; background: transparent; color: white; border: none; padding: 10px; cursor: pointer; z-index: 1001; font-size: 20px;">
        <i class="fas fa-arrow-left"></i>
      </button>
      <div class="search-bar">
        <i class="fas fa-search"></i>
        <input type="text" placeholder="Search lectures..." id="searchInput">
      </div>
    </div>
  </header>
  <main>
    <div class="lecture-list" id="lectureList">
      <p>Loading lectures...</p>
    </div>
  </main>

  <script>
    let lecturesData = [];

    class LectureCompletion {{
      constructor() {{
        this.storageKey = 'lectureCompletions';
      }}

      isCompleted(platform, subject, lectureTitle) {{
        const completions = this.getCompletions();
        const key = `${{platform}}-${{subject}}-${{lectureTitle}}`;
        return completions[key] || false;
      }}

      toggleCompletion(platform, subject, lectureTitle) {{
        const completions = this.getCompletions();
        const key = `${{platform}}-${{subject}}-${{lectureTitle}}`;
        completions[key] = !completions[key];
        this.saveCompletions(completions);
        return completions[key];
      }}

      getCompletions() {{
        try {{
          const stored = localStorage.getItem(this.storageKey);
          return stored ? JSON.parse(stored) : {{}};
        }} catch (error) {{
          console.error('Error reading completions from localStorage:', error);
          return {{}};
        }}
      }}

      saveCompletions(completions) {{
        try {{
          localStorage.setItem(this.storageKey, JSON.stringify(completions));
        }} catch (error) {{
          console.error('Error saving completions to localStorage:', error);
        }}
      }}

      createCompletionToggle(platform, subject, lectureTitle) {{
        const isCompleted = this.isCompleted(platform, subject, lectureTitle);

        const toggle = document.createElement('div');
        toggle.className = 'completion-toggle';
        toggle.innerHTML = `
          <div class="completion-circle ${{isCompleted ? 'completed' : ''}}">
            <i class="fas fa-check ${{isCompleted ? 'visible' : ''}}"></i>
          </div>
        `;

        toggle.onclick = (e) => {{
          e.stopPropagation();
          const newStatus = this.toggleCompletion(platform, subject, lectureTitle);
          const circle = toggle.querySelector('.completion-circle');
          const checkIcon = toggle.querySelector('.fas.fa-check');

          if (newStatus) {{
            circle.classList.add('completed');
            checkIcon.classList.add('visible');
          }} else {{
            circle.classList.remove('completed');
            checkIcon.classList.remove('visible');
          }}
        }};

        return toggle;
      }}
    }}

    const completionTracker = new LectureCompletion();

    async function loadLectures() {{
      console.log('Starting to load lectures...');
      try {{
        console.log('Fetching from {json_filename}...');
        const response = await fetch('{json_filename}');
        console.log('Fetch response status:', response.status, response.statusText);

        if (!response.ok) {{
          throw new Error(`HTTP error! status: ${{response.status}}`);
        }}

        const data = await response.json();
        console.log('JSON data received:', data);

        lecturesData = data.lectures || [];
        console.log('Loaded lectures from JSON:', lecturesData.length, 'lectures');

        if (lecturesData.length > 0) {{
          renderLectures(lecturesData);
        }} else {{
          console.log('No lectures found in JSON, using fallback');
          useFallbackLectures();
        }}
      }} catch (error) {{
        console.error('Error loading lectures:', error);
        console.log('Using fallback lectures due to error');
        useFallbackLectures();
      }}
    }}

    function useFallbackLectures() {{
      lecturesData = [
        {{ title: "Introduction", streamingUrl: "https://www.youtube.com/embed/dQw4w9WgXcQ" }},
        {{ title: "Basic Terminology", streamingUrl: "https://www.youtube.com/embed/dQw4w9WgXcQ" }}
      ];
      console.log('Using fallback lectures:', lecturesData.length, 'lectures');
      renderLectures(lecturesData);
    }}

    function escapeHtml(text) {{
      const map = {{
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;',
        '\\\\': '&#92;'
      }};
      return text.replace(/[&<>"'\\\\]/g, m => map[m]);
    }}

    function renderLectures(lectures) {{
      console.log('Rendering lectures:', lectures.length);
      const lectureList = document.getElementById('lectureList');

      if (!lectureList) {{
        console.error('lectureList element not found!');
        return;
      }}

      lectureList.innerHTML = '';

      if (lectures.length === 0) {{
        lectureList.innerHTML = '<p>No lectures available.</p>';
        return;
      }}

      lectures.forEach((lecture, index) => {{
        console.log(`Rendering lecture ${{index + 1}}:`, lecture.title);
        const lectureCard = document.createElement('div');
        lectureCard.className = 'lecture-card';

        const titleElement = document.createElement('h3');
        titleElement.textContent = lecture.title;

        const buttonContainer = document.createElement('div');
        buttonContainer.className = 'button-container';

        const openButton = document.createElement('button');
        openButton.className = 'open-button';
        openButton.innerHTML = '<i class="fas fa-play-circle"></i> Open';

        openButton.addEventListener('click', function() {{
          openStreamPlayer(
            lecture.streamingUrl,
            lecture.downloadUrl || lecture.streamingUrl,
            lecture.title
          );
        }});

        buttonContainer.appendChild(openButton);
        lectureCard.appendChild(titleElement);
        lectureCard.appendChild(buttonContainer);

        const completionToggle = completionTracker.createCompletionToggle('marrow', 'subject', lecture.title);
        lectureCard.appendChild(completionToggle);

        lectureList.appendChild(lectureCard);
      }});

      console.log('Finished rendering all lectures');
    }}

    function openVideo(title, url) {{
      const popup = document.createElement('div');
      popup.className = 'video-popup';
      popup.innerHTML = `
        <div class="video-popup-content">
          <button class="close-popup" onclick="closeVideo(this)">&times;</button>
          <h2 class="video-title">${{title}}</h2>
          <div class="iframe-container">
            <iframe src="${{url}}" allowfullscreen></iframe>
          </div>
        </div>
      `;

      popup.onclick = function(e) {{
        if (e.target === popup) {{
          closeVideo(popup.querySelector('.close-popup'));
        }}
      }};

      document.body.appendChild(popup);
    }}

    function closeVideo(button) {{
      const popup = button.closest('.video-popup');
      document.body.removeChild(popup);
    }}

    function downloadVideo(streamUrl) {{
      const downloadUrl = streamUrl.replace('/watch/', '/dl/');
      window.open(downloadUrl, '_blank');
    }}

    function openStreamPlayer(streamUrl, downloadUrl, title) {{
      if (!streamUrl) {{
        alert('Stream URL not available');
        return;
      }}

      const sanitizedTitle = (title || 'Lecture Video').trim();

      const params = new URLSearchParams({{
        stream: streamUrl,
        title: sanitizedTitle
      }});

      if (downloadUrl) {{
        params.append('download', downloadUrl);
      }}

      const currentPath = window.location.pathname;
      const pathParts = currentPath.split('/').filter(p => p && !p.includes('.html'));

      const isGitHubPages = window.location.hostname.includes('github.io');

      let streamPlayerUrl;
      if (isGitHubPages && pathParts.length > 0) {{
        const repoName = pathParts[0];
        streamPlayerUrl = `/${{repoName}}/stream-player.html?${{params.toString()}}`;
      }} else {{
        streamPlayerUrl = `/stream-player.html?${{params.toString()}}`;
      }}

      window.location.href = streamPlayerUrl;
    }}

    document.getElementById('searchInput').addEventListener('input', function(e) {{
      const query = e.target.value.toLowerCase();
      const filteredLectures = lecturesData.filter(lecture =>
        lecture.title.toLowerCase().includes(query)
      );
      renderLectures(filteredLectures);
    }});

    document.addEventListener('DOMContentLoaded', function() {{
      console.log('DOM loaded, starting lecture load...');
      loadLectures();
    }});
  </script>
  <script src="../../stream-player-utils.js"></script>
  <script src="../../theme.js"></script>
  <nav class="bottom-nav">
    <a href="../../app.html" class="active"><i class="fas fa-lightbulb"></i><span>Home</span></a>
    <a href="../../00x12345.html"><i class="fas fa-play-circle"></i><span>Videos</span></a>
    <a href="../../searchx.html"><i class="fas fa-search"></i><span>Search</span></a>
    <a href="../../quizx/index.html"><i class="fas fa-question-circle"></i><span>Q Bank</span></a>
  </nav>
</body>
</html>"""


async def upload_to_github(file_content: str, file_path: str, commit_message: str, token: str, branch: str = None):
    """Upload JSON file to GitHub repository.

    file_path: expected format "owner/repo/path/to/file.json"
    Returns (True, None) on success, (False, error_detail) on failure.
    """
    import base64
    import aiohttp

    try:
        if not token:
            return False, "GIT_TOKEN is empty or not set"

        # Normalize and split path
        normalized = file_path.strip().lstrip('/').rstrip('/')
        parts = normalized.split('/', 2)
        if len(parts) < 3:
            return False, f"Invalid path format: '{file_path}' — expected owner/repo/path/to/file.json"

        owner, repo, path = parts[0], parts[1], parts[2]
        api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

        content_encoded = base64.b64encode(file_content.encode('utf-8')).decode('utf-8')

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        async with aiohttp.ClientSession() as session:
            params = {}
            if branch:
                params['ref'] = branch

            sha = None
            get_warning = None
            async with session.get(api_url, headers=headers, params=params) as resp_get:
                if resp_get.status == 200:
                    data = await resp_get.json()
                    sha = data.get('sha')
                elif resp_get.status == 404:
                    sha = None  # file doesn't exist yet, will create
                elif resp_get.status == 401:
                    return False, "GitHub token is invalid or expired (401 Unauthorized)"
                elif resp_get.status == 403:
                    get_text = await resp_get.text()
                    return False, f"GitHub access forbidden (403) — token may lack 'repo' scope. Detail: {get_text[:300]}"
                else:
                    get_text = await resp_get.text()
                    get_warning = f"GET {resp_get.status}: {get_text[:200]}"

            payload = {
                "message": commit_message or "Add file via bot",
                "content": content_encoded
            }
            if sha:
                payload["sha"] = sha
            if branch:
                payload["branch"] = branch

            async with session.put(api_url, headers=headers, json=payload) as resp_put:
                resp_text = await resp_put.text()
                if resp_put.status in (200, 201):
                    return True, None
                elif resp_put.status == 401:
                    return False, "GitHub token invalid/expired (401). Re-check GIT_TOKEN."
                elif resp_put.status == 403:
                    return False, f"GitHub 403 Forbidden — token likely missing 'repo' write scope.\nRepo: {owner}/{repo}\nDetail: {resp_text[:300]}"
                elif resp_put.status == 404:
                    return False, f"GitHub 404 — repo '{owner}/{repo}' not found or token has no access to it.\nURL: {api_url}"
                elif resp_put.status == 422:
                    return False, f"GitHub 422 Unprocessable — possibly wrong branch or SHA conflict.\nDetail: {resp_text[:300]}"
                else:
                    detail = f"HTTP {resp_put.status}\nURL: {api_url}\nResponse: {resp_text[:400]}"
                    if get_warning:
                        detail = f"GET warning: {get_warning}\n{detail}"
                    return False, detail

    except Exception as e:
        return False, f"Exception during upload: {type(e).__name__}: {e}"

@StreamBot.on_message(filters.private & filters.user(list(Var.OWNER_ID)) & filters.command('batch'))
async def batch(client: Client, message: Message):
    Var.reset_batch()
    
    try:
        # Step 1: Ask for GitHub destination folder
        dest_folder_msg = await client.ask(
            text="📁 Enter the GitHub destination folder path:\n\n"
                 "Format: owner/repo/path/to/folder\n"
                 "Example: username/repository/marrow/anatomy\n\n"
                 "This is where JSON files will be uploaded.",
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
            await message.reply(
                "❌ GIT_TOKEN not found in environment variables.\n\n"
                "Please add your GitHub Personal Access Token with repo permissions:\n"
                "1. Go to GitHub Settings → Developer settings → Personal access tokens\n"
                "2. Generate new token (classic) with 'repo' scope\n"
                "3. Add GIT_TOKEN to your environment variables"
            )
            return
        
        status_msg = await message.reply_text(f"🚀 Starting batch processing for {len(subjects_data)} subjects...")
        
        # Process each subject
        for idx, subject_info in enumerate(subjects_data, 1):
            subject_name = subject_info['subject']
            json_output = []
            skipped_messages = []
            
            try:
                # Create mock message objects for get_message_id
                class MockMessage:
                    def __init__(self, text):
                        self.text = text
                        self.forward_from_chat = None
                        self.forward_sender_name = None
                
                # Get first and last message IDs
                f_msg_id = await get_message_id(client, MockMessage(subject_info['first']))
                s_msg_id = await get_message_id(client, MockMessage(subject_info['last']))
                
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
                shared_thumbnail_url = None  # Reset per subject
                thumb_warning = None         # First thumbnail error seen in this subject
                
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
                        
                        # Use subject name as folder for thumbnail organization
                        thumbnail_folder = subject_name.lower().replace(" ", "_")
                        # Pass shared_thumbnail_url so only first video extracts, rest reuse
                        await process_message(msg, json_output, skipped_messages, thumbnail_folder, client, shared_thumbnail_url)
                        
                        # After processing first video, check result for thumbnail URL or error
                        if json_output:
                            last_entry = json_output[-1]
                            if not shared_thumbnail_url and 'thumbnailUrl' in last_entry:
                                shared_thumbnail_url = last_entry['thumbnailUrl']
                                logging.info(f"✅ Thumbnail reused for remaining videos in {subject_name}: {shared_thumbnail_url}")
                            # Capture first thumbnail error to report to bot (only once per subject)
                            if not thumb_warning and '_thumb_error' in last_entry:
                                thumb_warning = last_entry.pop('_thumb_error')
                            elif '_thumb_error' in last_entry:
                                last_entry.pop('_thumb_error')  # strip from JSON even if already have warning

                # Strip any leftover _thumb_error keys from all entries before saving
                clean_output = [{k: v for k, v in e.items() if k != '_thumb_error'} for e in json_output]

                # Prepare JSON output in the required format
                output_data = {
                    "subjectName": subject_name.lower().replace(" ", ""),
                    "lectures": clean_output,
                    "skipped": skipped_messages
                }
                
                # Save to file
                json_filename = f"{subject_name}.json"
                json_content = json.dumps(output_data, indent=4, ensure_ascii=False)
                
                # Upload JSON to GitHub
                github_file_path = f"{github_dest_folder}/{json_filename}".replace('//', '/')
                commit_msg = f"Add {json_filename} - {len(clean_output)} lectures, {len(skipped_messages)} skipped"
                
                upload_success, upload_error = await upload_to_github(
                    json_content,
                    github_file_path,
                    commit_msg,
                    git_token
                )

                # Upload HTML to GitHub alongside the JSON
                html_filename = f"{subject_name}.html"
                html_content = generate_lecture_html(json_filename)
                github_html_path = f"{github_dest_folder}/{html_filename}".replace('//', '/')
                html_commit_msg = f"Add {html_filename} for {json_filename}"

                html_upload_success, html_upload_error = await upload_to_github(
                    html_content,
                    github_html_path,
                    html_commit_msg,
                    git_token
                )

                # Build status text
                thumb_note = f"\n⚠️ Thumbnail error: {thumb_warning[:200]}" if thumb_warning else ""
                html_note = "" if html_upload_success else f"\n⚠️ HTML upload failed: {(html_upload_error or '')[:150]}"

                if upload_success:
                    await status_msg.edit_text(
                        f"✅ {subject_name} completed!\n"
                        f"Uploaded: {json_filename} + {html_filename}\n"
                        f"Lectures: {len(clean_output)} | Skipped: {len(skipped_messages)}"
                        f"{thumb_note}{html_note}\n\n"
                        f"Progress: {idx}/{len(subjects_data)}"
                    )
                else:
                    error_detail = upload_error or "Unknown error"
                    logging.error(f"GitHub upload failed for {subject_name}: {error_detail}")
                    await status_msg.edit_text(
                        f"❌ Failed to upload {subject_name} to GitHub\n\n"
                        f"🔍 Reason:\n{error_detail}\n\n"
                        f"📁 Path attempted: {github_file_path}"
                        f"{thumb_note}{html_note}\n\n"
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

@StreamBot.on_message((filters.private) & (filters.video | filters.audio | filters.photo), group=4)
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
