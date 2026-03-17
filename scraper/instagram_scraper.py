import instaloader
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from scraper.config import (
    PROFILE_NAMES,
    REQUEST_DELAY,
    POST_LIMIT,
    OUTPUT_DIR_INSTAGRAM
)


START_DATE = None  
END_DATE = None     

# ==============================
# SETUP
# ==============================
L = instaloader.Instaloader()



# ==============================
# MAIN SCRAPER FUNCTION
# ==============================
def scrape_profile(profile_name):
    print(f"\nFetching posts for: {profile_name}")
    try:
        profile = instaloader.Profile.from_username(L.context, profile_name)
    except instaloader.exceptions.ProfileNotExistsException:
        print(f"Warning: Profile {profile_name} does not exist. Skipping...")
        return None
    except instaloader.exceptions.ConnectionException:
        print(f"Warning: Connection error on {profile_name}. Skipping...")
        return None

    posts_data = []
    count = 0

    for post in profile.get_posts():
        try:
            post_date = post.date_utc

            # Filter by date if specified
            if START_DATE and post_date < START_DATE:
                break
            if END_DATE and post_date > END_DATE:
                continue

            caption = post.caption if post.caption else ""
            caption = caption.replace("\r\n", "\\n").replace("\n", "\\n")

            posts_data.append({
                "post_number": count + 1,
                "caption": caption,
                "date": post_date,
                "likes": post.likes,
                "comments": post.comments,
                "url": post.url,
                "post_type": "video" if post.is_video else "image"
            })

            count += 1
            time.sleep(REQUEST_DELAY)  # Avoid rate-limits

            if count >= POST_LIMIT:
                break

        except instaloader.exceptions.QueryReturnedNotFoundException:
            print("Post not accessible, skipping...")
            continue
        except Exception as e:
            print(f"Unknown error: {e}")
            continue

    if posts_data:
        OUTPUT_DIR_INSTAGRAM.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(posts_data)
        output_path = Path(OUTPUT_DIR_INSTAGRAM) / f"{profile_name}_posts.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Saved CSV -> {output_path}")
        return df
    else:
        print(f"No posts collected for {profile_name}")
        return None

# ==============================
# RUN FOR ALL PROFILES
# ==============================
def main():
    tables = {}
    for profile_name in PROFILE_NAMES:
        df = scrape_profile(profile_name)
        if df is not None:
            tables[profile_name] = df
    return tables

