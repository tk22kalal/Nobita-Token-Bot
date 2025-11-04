import os
import asyncio
import secrets
import logging
from pathlib import Path

async def extract_video_thumbnail(video_path: str, output_path: str = None, seek_time: str = "00:00:01") -> str:
    """
    Extract a thumbnail from a video file using ffmpeg (FAST mode for batch processing).
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional, will auto-generate if not provided)
        seek_time: Time position to extract thumbnail from (default: 1 second for speed)
    
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
        
        # Ultra-fast thumbnail extraction optimized for batch processing
        # -ss before -i: faster seeking
        # -vf scale: reduce to 320 width for speed (maintains aspect ratio)
        # -q:v 10: lower quality = faster processing (1-31 scale, 10 is good balance)
        # -frames:v 1: extract only 1 frame
        cmd = [
            "ffmpeg",
            "-ss", seek_time,          # Seek to position BEFORE input (faster)
            "-i", video_path,           # Input video
            "-vf", "scale=320:-1",      # Resize to width 320, maintain aspect ratio
            "-frames:v", "1",           # Extract only 1 frame
            "-q:v", "10",               # Lower quality for speed (1=best, 31=worst)
            "-y",                       # Overwrite output
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
    Extract a thumbnail from a video file (FAST mode - from beginning for speed).
    
    Optimized for batch processing of 1000+ files:
    - Extracts from 2 seconds in (skips black intro frames)
    - No duration calculation (saves time)
    - Low resolution (320px width)
    - Lower quality for faster processing
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional)
    
    Returns:
        Path to the extracted thumbnail
    """
    try:
        # For speed, extract from beginning (2 seconds in to skip potential black frames)
        # No duration check needed - saves significant time for large batches
        return await extract_video_thumbnail(video_path, output_path, "00:00:02")
        
    except Exception as e:
        logging.error(f"Error extracting thumbnail: {e}")
        # Fallback to 1 second if 2 seconds fails
        return await extract_video_thumbnail(video_path, output_path, "00:00:01")
