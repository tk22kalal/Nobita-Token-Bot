import os
import time
import shutil
import psutil
import asyncio
from Adarsh.bot import StreamBot
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram import filters
from utils_bot import *
from Adarsh import StartTime
from Adarsh.vars import Var
from Adarsh.utils.thumbnail_extractor import check_ffmpeg


START_TEXT = """ ʏᴏᴜʀ  ᴛᴇʟᴇɢʀᴀᴍ  ᴅᴄ  ɪꜱ : `{}`  """

@StreamBot.on_message(filters.regex("DC"))
async def start(bot, update):
    text = START_TEXT.format(update.from_user.dc_id)
    await update.reply_text(
        text=text,
        disable_web_page_preview=True,
        quote=True
    )

@StreamBot.on_message(filters.private & filters.regex("status📊"))
async def stats(bot, update):
    currentTime = readable_time((time.time() - StartTime))
    total, used, free = shutil.disk_usage('.')
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
    recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
    cpuUsage = psutil.cpu_percent(interval=0.5)
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    botstats = f'<b>⏳ ᴜᴘᴛɪᴍᴇ:</b> {currentTime}\n' \
              f'<b>♻️ ᴛᴏᴛᴀʟ:</b> {total}\n' \
              f'<b>🆓 ꜰʀᴇᴇ: </b> {free}\n' \
              f'<b>🉐 ᴏᴄᴄᴜᴘɪᴇᴅ:</b> {used} \n\n\n' \
              f'<b>📊  ᴅᴀᴛᴀ  ᴜꜱᴀɢᴇꜱ  📊</b>\n\n<b>☣️  ᴄᴘᴜ:</b> {cpuUsage}% \n' \
              f'<b>☢️  ʀᴀᴍ:</b> {memory}% \n' \
              f'<b>☣️  ᴅɪꜱᴋ:</b> {disk}% \n' \
              f'<b>📤  ᴜᴘʟᴏᴀᴅ:</b> {sent}\n' \
              f'<b>📥  ᴅᴏᴡɴ:</b> {recv}'
    await update.reply_text(botstats)


@StreamBot.on_message(filters.private & filters.user(list(Var.ADMIN_IDS)) & filters.command('checkenv'))
async def checkenv(bot, message):
    """Diagnose the server environment — ffmpeg, tokens, Python path."""
    lines = ["<b>🔍 Environment Check</b>\n"]

    # ffmpeg / ffprobe
    ffmpeg_ok, ffmpeg_detail = check_ffmpeg()
    ffmpeg_icon = "✅" if ffmpeg_ok else "❌"
    lines.append(f"{ffmpeg_icon} <b>ffmpeg:</b> <code>{ffmpeg_detail}</code>")

    # ffmpeg version
    if ffmpeg_ok:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            out, _ = await proc.communicate()
            ver_line = out.decode(errors="ignore").split("\n")[0]
            lines.append(f"   <i>{ver_line}</i>")
        except Exception as e:
            lines.append(f"   <i>version check failed: {e}</i>")
    else:
        lines.append("   <b>Fix:</b> <code>sudo apt install ffmpeg</code>  then restart bot")

    lines.append("")

    # Secrets / env vars
    def secret_status(key):
        val = os.environ.get(key, '')
        if val:
            masked = val[:4] + "…" + val[-2:] if len(val) > 6 else "***"
            return f"✅ <b>{key}:</b> set ({masked})"
        return f"❌ <b>{key}:</b> NOT SET"

    for key in ["THUMB_API", "GIT_TOKEN", "API_ID", "API_HASH", "BOT_TOKEN", "DATABASE_URL"]:
        lines.append(secret_status(key))

    lines.append("")

    # Python path
    import sys
    lines.append(f"🐍 <b>Python:</b> <code>{sys.version.split()[0]}</code>")
    lines.append(f"📂 <b>CWD:</b> <code>{os.getcwd()}</code>")

    # PORT
    port = os.environ.get("PORT", "8080")
    lines.append(f"🌐 <b>PORT:</b> <code>{port}</code>")

    await message.reply_text("\n".join(lines), parse_mode="html")


@StreamBot.on_message(filters.private & filters.command('root'))
async def root_command(bot, message):
    """Send the GitHub file index link to the requesting admin."""
    user_id = message.from_user.id
    # Check admin inline — safer than filters.user() when ADMIN_IDS may be empty at import time
    admin_ids = getattr(Var, 'ADMIN_IDS', set()) or getattr(Var, 'OWNER_ID', set())
    if not admin_ids or user_id not in admin_ids:
        return  # silently ignore non-admins

    base_url = Var.URL.rstrip('/')
    tree_url = f"{base_url}/root-tree"
    await message.reply_text(
        "👋 Hi Admin!\n\n"
        "📁 Tap the button below to browse the file index.\n"
        "Folders are expandable — click any folder to open it.\n"
        "HTML files are listed without the .html extension.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📂 Open File Index", url=tree_url)
        ]]),
        quote=True
    )
