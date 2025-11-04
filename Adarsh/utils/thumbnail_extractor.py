import os
import asyncio
import secrets
import logging
from pathlib import Path

async def extract_video_thumbnail(video_path: str, output_path: str = None, seek_time: str = "00:00:10") -> str:
    """
    Extract a thumbnail from a video file using ffmpeg (HIGH QUALITY mode).
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional, will auto-generate if not provided)
        seek_time: Time position to extract thumbnail from (default: 10 seconds)
    
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
        
        # High quality thumbnail extraction at 10 seconds
        # -ss before -i: faster seeking
        # -vf scale: 1280 width for better quality (maintains aspect ratio)
        # -q:v 2: high quality (1=best, 31=worst, 2 is excellent quality)
        # -frames:v 1: extract only 1 frame
        cmd = [
            "ffmpeg",
            "-ss", seek_time,          # Seek to position BEFORE input (faster)
            "-i", video_path,           # Input video
            "-vf", "scale=1280:-1",     # Resize to width 1280 for better quality, maintain aspect ratio
            "-frames:v", "1",           # Extract only 1 frame
            "-q:v", "2",                # High quality (1=best, 31=worst)
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
    Extract a thumbnail from a video file at 10 seconds with high quality.
    
    Improved for batch processing:
    - Extracts from 10 seconds in (better frame than start)
    - High resolution (1280px width)
    - High quality (q:v 2)
    - No duration calculation (saves time)
    
    Args:
        video_path: Path to the video file
        output_path: Path where to save the thumbnail (optional)
    
    Returns:
        Path to the extracted thumbnail
    """
    try:
        # Extract from 10 seconds for better frame quality
        return await extract_video_thumbnail(video_path, output_path, "00:00:10")
        
    except Exception as e:
        logging.error(f"Error extracting thumbnail at 10s: {e}")
        # Fallback to 2 seconds if 10 seconds fails (video might be shorter)
        try:
            return await extract_video_thumbnail(video_path, output_path, "00:00:02")
        except:
            # Final fallback to 1 second
            return await extract_video_thumbnail(video_path, output_path, "00:00:01")
