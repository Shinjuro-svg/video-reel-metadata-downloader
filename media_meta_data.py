import json
import os
import re
from urllib.parse import urlparse
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

# ==================== INSTAGRAM META ====================
def fetch_instagram_meta(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        def og(prop):
            tag = soup.find("meta", {"property": prop})
            return tag["content"].strip() if tag and tag.get("content") else None

        desc = og("og:description")
        image = og("og:image")

        # Replace None or nan with empty strings
        return {
            "url": url or "",
            "description": (desc or "").replace("nan", ""),
            "thumbnail": (image or "").replace("nan", ""),
        }
    except Exception:
        return {"url": url, "description": "", "thumbnail": ""}


# ==================== YOUTUBE DURATION ====================
def parse_youtube_duration_short(duration):
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.fullmatch(duration)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"


# ==================== TIKTOK META ====================
def fetch_tiktok_reel_metadata(reel_url):
    oembed_endpoint = "https://www.tiktok.com/oembed"
    params = {"url": reel_url}
    time.sleep(3)
    try:
        response = requests.get(oembed_endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        html = data.get("html", "")
        soup = BeautifulSoup(html, "html.parser")

        result = {
            "Title / Headline": data.get("title"),
            "Creator / Channel": data.get("author_url"),
            "Thumbnail Url": data.get("thumbnail_url"),
        }
        return result
    except requests.exceptions.RequestException as e:
        print(f"Error fetching TikTok data: {e}")
        return None


# ==================== YOUTUBE API ====================
def extract_video_id(url):
    patterns = [r"(?:v=|/v/|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})"]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


API_KEY = os.getenv("YOUTUBE_DATA_API")
INPUT_CSV = "WHATSAPP_AI_Video_Library_2025-10-12.csv"
OUTPUT_CSV = "YouTube_Metadata_Output.csv"
TEMP_CSV = "temp.csv"

youtube = build("youtube", "v3", developerKey=API_KEY)


def get_channel_statistics(channel_id):
    request = youtube.channels().list(part="snippet,statistics", id=channel_id)
    response = request.execute()
    if not response.get("items"):
        return None
    stats = response["items"][0]["statistics"]
    return stats.get("subscriberCount")


def fetch_video_data(video_id):
    time.sleep(2)
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails,status,topicDetails,recordingDetails",
        id=video_id
    )
    response = request.execute()
    if not response.get("items"):
        print(f"not fetched.")
        return None
    print(f"fetched.")
    item = response["items"][0]
    snippet = item.get("snippet", {})
    content = item.get("contentDetails", {})
    subsribers = get_channel_statistics(snippet.get("channelId"))
    return {
        "Title / Headline": snippet.get("title"),
        "Description / Summary": snippet.get("description"),
        "Creator / Channel": f"https://www.youtube.com/channel/{snippet.get('channelId')}",
        "Publish Date": snippet.get("publishedAt").split("T")[0] if snippet.get("publishedAt") else None,
        "Thumbnail Url": snippet.get("thumbnails", {}).get("default", {}).get("url"),
        "Language": snippet.get("defaultAudioLanguage"),
        "Duration": parse_youtube_duration_short(content.get("duration")),
        "Followers / Subscribers": subsribers,
    }


# ==================== PLATFORM DETECTION ====================
def detect_platform(url: str) -> str:
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


def download_metadata(row_num: int, url: str):
    platform = detect_platform(url)
    if platform == "unknown":
        print("❌ Could not detect platform from URL.")
        return
    if platform == "youtube":
        return fetch_video_data(extract_video_id(url))
    if platform == "tiktok":
        return fetch_tiktok_reel_metadata(url)
    if platform == "instagram":
        return fetch_instagram_meta(url)
    print(f"Detected platform: {platform}")


# ==================== MAIN LOOP ====================
try:
    df = pd.read_csv(INPUT_CSV, encoding="utf-8")
    df = df.astype(str)
    
    df.replace(r'^\s*nan\s*$', '', regex=True, inplace=True)
    print(f"📊 Loaded {len(df)} records from CSV")
    print(f"Columns: {list(df.columns)}")

    # ✅ Create temp CSV at the start
    df.to_csv(TEMP_CSV, index=False, encoding="utf-8")
    print(f"🗂️ Created live temp CSV: {TEMP_CSV}")

    success_count = 0
    skipped_error_count = 0  # merged counter

    for idx, row in df.iterrows():
        try:
            url = str(row.get("URL / Media", "")).strip()
            if not url or url == "nan":
                print(f"⚠️ Record {idx + 1}: Missing URL, skipping.")
                skipped_error_count += 1
                continue

            results = download_metadata(idx + 1, url)
            if results:
                for key, value in results.items():
                    if key not in df.columns:
                        df[key] = None
                        print(f"➕ Added new column: '{key}'")
                    # replace any nan-like values with empty strings
                    if isinstance(value, float) and pd.isna(value):
                        value = ""
                    elif isinstance(value, str) and value.lower() == "nan":
                        value = ""
                    df.at[idx, key] = value
                    print(f"✅ Record {idx + 1}: Added '{key}'")

                success_count += 1

                # ✅ Write to CSV live
                df.to_csv(TEMP_CSV, index=False, encoding="utf-8")

            else:
                skipped_error_count += 1
                print(f"❌ Record {idx + 1}: Failed to fetch metadata.")

        except Exception as e:
            print(f"❌ Record {idx + 1}: Error: {e}")
            skipped_error_count += 1

    # Replace old file with temp
    os.replace(TEMP_CSV, INPUT_CSV)
    print("\n===============================")
    print(f"✅ Finished updating CSV.")
    print(f"📈 Successful rows: {success_count}")
    print(f"⚠️ Skipped/Errored rows: {skipped_error_count}")
    print("===============================")

except Exception as e:
    print(f"❌ Error processing CSV: {e}")