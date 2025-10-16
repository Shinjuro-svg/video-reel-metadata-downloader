import re
import os
import json
import requests
from urllib.parse import urlparse
from yt_dlp import YoutubeDL
import instaloader
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

# -----------------------------
# Helper Functions
# -----------------------------

def sanitize_filename(s: str) -> str:
    """Replace invalid Windows filename characters with underscore."""
    return re.sub(r'[<>:"/\\|?*]', '_', s)

def detect_platform(url: str) -> str:
    """Detect platform from URL: YouTube, TikTok, Instagram, or unknown."""
    url = url.strip()
    if not url:
        return "unknown"

    domain = urlparse(url).netloc.lower()

    if re.search(r"(youtube\.com|youtu\.be)$", domain):
        return "youtube"
    if re.search(r"(tiktok\.com)$", domain):
        return "tiktok"
    if re.search(r"(instagram\.com)$", domain):
        return "instagram"
    return "unknown"

# -----------------------------
# TikTok Downloader
# -----------------------------

def download_tiktok(url: str, rapidapi_key: str, base_dir: str = "./downloads"):
    api_host = "tiktok-video-downloader-api.p.rapidapi.com"
    api_endpoint = f"https://{api_host}/media"

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": api_host
    }
    params = {"videoUrl": url}

    print(f"🔍 Fetching TikTok info for {url} ...")
    resp = requests.get(api_endpoint, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    video_url = data.get("downloadUrl")
    video_id = data.get("id") or "unknown_id"
    username = data.get("author", {}).get("username", "unknown_user")

    if not video_url:
        raise RuntimeError("Could not find 'downloadUrl' in API response:\n" + json.dumps(data, indent=2))

    folder_name = sanitize_filename(f"{username}_{video_id}")
    video_dir = os.path.join(base_dir, 'tiktok', folder_name)
    os.makedirs(video_dir, exist_ok=True)

    video_path = os.path.join(video_dir, f"{username}_{video_id}.mp4")
    metadata_path = os.path.join(video_dir, f"{username}_{video_id}_metadata.json")

    if os.path.exists(video_path):
        print(f"⚠️ Video already exists: {video_path}. Skipping download.\n")
        return data, video_path

    print(f"⬇️ Downloading TikTok video @{username} ({video_id})...")
    video_resp = requests.get(video_url, stream=True)
    video_resp.raise_for_status()
    with open(video_path, "wb") as f:
        for chunk in video_resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    with open(metadata_path, "w", encoding="utf-8") as mf:
        json.dump(data, mf, ensure_ascii=False, indent=2)

    print(f"✅ TikTok download complete: {video_path}")
    return data, video_path

# -----------------------------
# YouTube Downloader
# -----------------------------

def download_youtube(url: str, base_dir: str = "./downloads"):
    exclude_fields = ['formats', 'automatic_captions', 'heatmap', 'http_headers', 'vbr', 'abr', 'vcodec', 'acodec']

    with YoutubeDL({}) as ydl:
        info = ydl.extract_info(url, download=False)

    metadata = {k: v for k, v in info.items() if k not in exclude_fields}
    video_title = sanitize_filename(info['title'])
    folder_path = os.path.join(base_dir, 'youtube', video_title)
    os.makedirs(folder_path, exist_ok=True)

    video_path = os.path.join(folder_path, f"{video_title}.mp4")
    if os.path.exists(video_path):
        print(f"⚠️ YouTube video already exists: {video_path}. Skipping download.")
        return metadata, video_path

    metadata_path = os.path.join(folder_path, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    ydl_opts_download = {
        'outtmpl': os.path.join(folder_path, '%(title)s.%(ext)s'),
    }
    with YoutubeDL(ydl_opts_download) as ydl:
        ydl.download([url])

    print(f"✅ YouTube download complete: {video_path}")
    return metadata, video_path

# -----------------------------
# Instagram Downloader
# -----------------------------

def download_instagram(url: str, L=None, base_dir: str = "./downloads"):
    if L is None:
        L = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False
        )

    shortcode = url.strip("/").split("/")[-1]
    post = instaloader.Post.from_shortcode(L.context, shortcode)

    folder_name = sanitize_filename(f"{post.owner_username}_{shortcode}")
    folder_path = os.path.join(base_dir, 'instagram', folder_name)
    os.makedirs(folder_path, exist_ok=True)

    video_filename = sanitize_filename(f"{post.owner_username}_{shortcode}.mp4")
    video_path = os.path.join(folder_path, video_filename)
    if os.path.exists(video_path):
        print(f"⚠️ Instagram video already exists: {video_path}. Skipping download.")
        return

    with requests.get(post.video_url, stream=True) as r:
        r.raise_for_status()
        with open(video_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    metadata = {
        "username": post.owner_username,
        "caption": post.caption,
        "likes": post.likes,
        "comments": post.comments,
        "url": url,
    }

    metadata_path = os.path.join(folder_path, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    print(f"✅ Instagram download complete: {video_path}")
    return metadata, video_path

# -----------------------------
# Unified Download Function
# -----------------------------

def download_video(url: str):
    platform = detect_platform(url)
    if platform == "unknown":
        print("❌ Could not detect platform from URL.")
        return

    print(f"Detected platform: {platform}")

    if platform == "tiktok":
        rapidapi_key = os.getenv("RAPIDAPI_KEY")
        if not rapidapi_key:
            raise EnvironmentError("TikTok RapidAPI key not found in environment.")
        return download_tiktok(url, rapidapi_key)

    elif platform == "youtube":
        return download_youtube(url)

    elif platform == "instagram":
        USERNAME = os.getenv("INSTA_USERNAME")
        PASSWORD = os.getenv("INSTA_PASSWORD")
        L = None
        if USERNAME and PASSWORD:
            L = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                download_video_thumbnails=False,
                download_geotags=False,
                download_comments=False,
                save_metadata=False
            )
            L.login(USERNAME, PASSWORD)
        return download_instagram(url, L=L)

# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    url = input("Enter TikTok / Instagram / YouTube URL: ").strip()
    try:
        download_video(url)
    except Exception as e:
        print(f"❌ Error: {e}")
