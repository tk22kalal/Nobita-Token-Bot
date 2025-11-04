import os
import asyncio
import secrets
import logging
from pathlib import Path

async def extract_video_thumbnail(video_path: str, output_path: str = None, seek_time: str = "00:00:05") -> str:
    """
    Extract a thumbnail from a video file using ffmpeg.
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional, will auto-generate if not provided)
        seek_time: Time position to extract thumbnail from (default: 5 seconds, or middle if video is longer)
    
    Returns:
        Path to the extracted thumbnail
    """
    try:
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video file not found: {video_path}")
        
        if output_path is None:
            thumbnail_dir = Path("/tmp/thumbnails")
            thumbnail_dir.mkdir(exist_ok=True)
            random_name = secrets.token_hex(8)
            output_path = str(thumbnail_dir / f"thumb_{random_name}.jpg")
        
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            "ffmpeg",
            "-ss", seek_time,
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "2",
            "-y",
            output_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode('utf-8', errors='ignore')
            logging.error(f"ffmpeg error: {error_msg}")
            raise RuntimeError(f"Failed to extract thumbnail: {error_msg}")
        
        if not os.path.exists(output_path):
            raise RuntimeError("Thumbnail extraction failed: output file not created")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Error extracting thumbnail: {e}")
        raise


async def get_video_duration(video_path: str) -> float:
    """
    Get the duration of a video file in seconds.
    
    Args:
        video_path: Path to the video file
    
    Returns:
        Duration in seconds
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            duration = float(stdout.decode('utf-8').strip())
            return duration
        else:
            return 0.0
            
    except Exception as e:
        logging.error(f"Error getting video duration: {e}")
        return 0.0


async def extract_thumbnail_from_middle(video_path: str, output_path: str = None) -> str:
    """
    Extract a thumbnail from the middle of a video file.
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional)
    
    Returns:
        Path to the extracted thumbnail
    """
    try:
        duration = await get_video_duration(video_path)
        
        if duration > 0:
            middle_time = duration / 2
            seek_time = f"{int(middle_time // 3600):02d}:{int((middle_time % 3600) // 60):02d}:{int(middle_time % 60):02d}"
        else:
            seek_time = "00:00:05"
        
        return await extract_video_thumbnail(video_path, output_path, seek_time)
        
    except Exception as e:
        logging.error(f"Error extracting thumbnail from middle: {e}")
        return await extract_video_thumbnail(video_path, output_path, "00:00:05")
