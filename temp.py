import csv
import requests
from bs4 import BeautifulSoup
from time import sleep 
import os

INPUT_CSV = "WHATSAPP_AI_Video_Library_2025-10-12.csv"
OUTPUT_CSV = "Instagram_Metadata.csv"

def fetch_instagram_meta(url):
    """Scrape Instagram Open Graph tags (no API)"""
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

        title = og("og:title")
        desc = og("og:description")
        image = og("og:image")

        if title and "• Instagram" in title:
            title = title.split("• Instagram")[0].strip()

        return {
            "url": url,
            "description": desc or "",
            "thumbnail": image or "",
        }
    except Exception:
        # Return empty fields on error
        return {
            "url": url,
            "description": "",
            "thumbnail": "",
        }

# --- Step 1: Read the CSV and collect Instagram URLs ---
instagram_links = []
with open(INPUT_CSV, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["Platform"].strip().lower() == "instagram":
            instagram_links.append(row["URL / Media"].strip())

print(f"Found {len(instagram_links)} Instagram links.")

# --- Step 2: Prepare output CSV (create header if not exists) ---
file_exists = os.path.exists(OUTPUT_CSV)
with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
    fieldnames = ["url", "title", "description", "thumbnail"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

# --- Step 3: Scrape each and append immediately ---
for i, link in enumerate(instagram_links, start=1):
    print(f"[{i}/{len(instagram_links)}] Scraping: {link}")
    data = fetch_instagram_meta(link)

    # Append immediately to CSV
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "description", "thumbnail"])
        writer.writerow(data)

    # Safe print preview
    title_preview = (data["title"] or "No title")[:60]
    print(f"✅ Saved: {title_preview}")

    sleep(2)  # polite delay

print(f"\n✅ Done! Appended metadata to {OUTPUT_CSV}")