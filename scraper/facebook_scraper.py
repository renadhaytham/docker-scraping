import http.client
import json
import time
import pandas as pd
import os
from pathlib import Path

from scraper.config import (
    API_KEY,
    API_HOST,
    POST_LIMIT,
    REQUEST_DELAY,
    PAGE_NAMES,
    PAGE_IDS,
    OUTPUT_DIR_FACEBOOK
)

# ==============================
# FETCH POSTS FROM API
# ==============================
def fetch_posts(page_id, cursor=None):
    conn = http.client.HTTPSConnection(API_HOST)

    endpoint = f"/page/posts?page_id={page_id}"
    if cursor:
        endpoint += f"&cursor={cursor}"

    conn.request(
        "GET",
        endpoint,
        headers={
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }
    )

    res = conn.getresponse()
    data = res.read()

    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        return {}

# ==============================
# BUILD DATAFRAME FOR ONE PAGE
# ==============================
def get_page_dataframe(page_id, limit):
    all_posts = []
    cursor = None

    while len(all_posts) < limit:
        response = fetch_posts(page_id, cursor)
        results = response.get("results", [])

        if not results:
            break

        all_posts.extend(results)
        cursor = response.get("cursor")

        if not cursor:
            break

        time.sleep(REQUEST_DELAY)

    all_posts = all_posts[:limit]

    rows = []

    for idx, post in enumerate(all_posts, start=1):
        message = post.get("message") or post.get("message_rich")
        if not message:
            continue
        
        message = message.strip()
        message = message.replace("\r\n", "\\n").replace("\n", "\\n")

        rows.append({
           "post_number": idx,
           "caption": message,
           "date": pd.to_datetime(post.get("timestamp"), unit="s"),
            "post_type": (
               "video" if post.get("video")
                else "image" if post.get("image")
                else "text"
        ),
        "url": post.get("url")
        })

    df = pd.DataFrame(rows)

    return df


# ==============================
# MAIN PROCESS
# ==============================
def main():
    os.makedirs(OUTPUT_DIR_FACEBOOK, exist_ok=True)

    pages_tables = {}

    for name, page_id in zip(PAGE_NAMES, PAGE_IDS):
        print(f"Fetching posts for {name}...")
        df = get_page_dataframe(page_id, POST_LIMIT)

        pages_tables[name] = df

        # Save to CSV
        output_path = Path(OUTPUT_DIR_FACEBOOK) / f"{name.lower()}_posts.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Saved -> {output_path}")


    return pages_tables


