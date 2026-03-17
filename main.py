from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from scraper.config import (
    PROFILE_NAMES,
    PAGE_NAMES,
    PAGE_IDS,
    OUTPUT_DIR_INSTAGRAM,
    OUTPUT_DIR_FACEBOOK,
    POST_LIMIT,
    REQUEST_DELAY
)
from scraper.instagram_scraper import scrape_profile
from scraper.facebook_scraper import get_page_dataframe
import os
import time
from datetime import datetime
from pathlib import Path

app = FastAPI()

scheduler = BackgroundScheduler()

# Scheduler job

def monthly_scraping():
    print(f"\n Monthly scraping started: {datetime.now()}")
    os.makedirs(OUTPUT_DIR_INSTAGRAM, exist_ok=True)
    os.makedirs(OUTPUT_DIR_FACEBOOK, exist_ok=True)

    # Instagram
    instagram_tables = {}
    for profile in PROFILE_NAMES:
        try:
            df = scrape_profile(profile)
            if df is not None:
                instagram_tables[profile] = df
                print(f" Instagram scraped: {profile}")
        except Exception as e:
            print(f" Error scraping Instagram {profile}: {e}")
        time.sleep(REQUEST_DELAY)

    # Facebook
    facebook_tables = {}
    for name, page_id in zip(PAGE_NAMES, PAGE_IDS):
        try:
            print(f"Fetching posts for {name}...")
            df = get_page_dataframe(page_id, POST_LIMIT)
            facebook_tables[name] = df
            print(f" Facebook scraped: {name}")

            output_path = Path(OUTPUT_DIR_FACEBOOK) / f"{name.lower()}_posts.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"Saved -> {output_path}")
        except Exception as e:
            print(f" Error scraping Facebook {name}: {e}")
        time.sleep(REQUEST_DELAY)

    print(f" Monthly scraping completed: {datetime.now()}")

@app.on_event("startup")
def start_scheduler():
    scheduler.add_job(monthly_scraping, "cron", day=1, hour=8, minute=0)
    scheduler.start()
    print("Scheduler started. Waiting for monthly scraping...")

@app.on_event("shutdown")
def shutdown_scheduler():
    scheduler.shutdown()
    print("Scheduler stopped.")

@app.get("/")
def root():
    return {"message": "Monthly scraping scheduler is running."}
