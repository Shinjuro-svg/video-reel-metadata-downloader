import csv
import re
import os
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv() 

API_KEY = os.getenv("YOUTUBE_DATA_API") 
INPUT_CSV = "WHATSAPP_AI_Video_Library_2025-10-12.csv"
OUTPUT_CSV = "YouTube_Metadata_Output.csv"

youtube = build("youtube", "v3", developerKey=API_KEY)


def extract_video_id(url):
    patterns = [
        r"(?:v=|/v/|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def get_all_categories():
    category_map = {}
    request = youtube.videoCategories().list(
        part="snippet",
        regionCode="US"
    )
    response = request.execute()

    for item in response.get("items", []):
        category_id = item["id"]
        category_name = item["snippet"]["title"]
        category_map[category_id] = category_name

    return category_map


def fetch_video_data(video_id, category_map):
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails,status,topicDetails,recordingDetails",
        id=video_id
    )
    response = request.execute()

    if not response.get("items"):
        return None

    item = response["items"][0]
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    content = item.get("contentDetails", {})
    status = item.get("status", {})
    topic = item.get("topicDetails", {})
    record = item.get("recordingDetails", {})

    category_id = snippet.get("categoryId")
    category_name = category_map.get(category_id, "Unknown")

    return {
        "video_id": video_id,
        "url": f"https://youtu.be/{video_id}",
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "published_at": snippet.get("publishedAt"),
        "tags": ", ".join(snippet.get("tags", [])) if snippet.get("tags") else "",
        "category_id": category_id,
        "thumbnails_default": snippet.get("thumbnails", {}).get("default", {}).get("url"),
        "thumbnails_medium": snippet.get("thumbnails", {}).get("medium", {}).get("url"),
        "thumbnails_high": snippet.get("thumbnails", {}).get("high", {}).get("url"),
        "category_name": category_name,
        "topic_names": ", ".join(t.replace("https://en.wikipedia.org/wiki/", "") for t in topic.get("topicCategories", [])) if topic.get("topicCategories") else "",
        "default_language": snippet.get("defaultLanguage"),
        "default_audio_language": snippet.get("defaultAudioLanguage"),
        "duration": content.get("duration"),
        "dimension": content.get("dimension"),
        "definition": content.get("definition"),
        "caption": content.get("caption"),
        "licensed_content": content.get("licensedContent"),
        "projection": content.get("projection"),
        "upload_status": status.get("uploadStatus"),
        "privacy_status": status.get("privacyStatus"),
        "license": status.get("license"),
        "embeddable": status.get("embeddable"),
        "public_stats_viewable": status.get("publicStatsViewable"),
        "made_for_kids": status.get("madeForKids"),
        "views": stats.get("viewCount"),
        "likes": stats.get("likeCount"),
        "favorites": stats.get("favoriteCount"),
        "comments": stats.get("commentCount"),
        "topic_categories": ", ".join(topic.get("topicCategories", [])) if topic.get("topicCategories") else "",
        "location_description": record.get("locationDescription"),
        "recording_date": record.get("recordingDate")
    }


def main():
    results = []

    print("📚 Fetching YouTube category map...")
    category_map = get_all_categories()
    print(f"✅ Loaded {len(category_map)} categories.\n")

    with open(INPUT_CSV, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["Platform"].lower() == "youtube":
                url = row["URL / Media"]
                video_id = extract_video_id(url)
                if video_id:
                    print(f"🎥 Fetching data for video: {video_id}")
                    data = fetch_video_data(video_id, category_map)
                    if data:
                        results.append(data)

    if results:
        with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ Successfully saved {len(results)} YouTube records with full metadata to '{OUTPUT_CSV}'")
    else:
        print("\n⚠️ No valid YouTube videos found in CSV.")


if __name__ == "__main__":
    main()
