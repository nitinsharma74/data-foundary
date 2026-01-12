import requests
import csv
import time
import json
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ================= CONFIG =================

SUBREDDIT = "crunchyroll"
POSTS_URL = f"https://www.reddit.com/r/{SUBREDDIT}/new.json"

DAYS_BACK = 60

OUTPUT_FILE = f"reddit_sentiment_analysis/fixtures/reddit_crunchyroll_{DAYS_BACK}days.csv"

REQUEST_DELAY = 6  # One request every 6 seconds

# ========================================

cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).timestamp()


# ================= SAFE REQUEST =================

def reddit_get(url, params=None):
    while True:
        try:
            now = datetime.utcnow()
            seconds_passed = now.hour * 3600 + now.minute * 60 + now.second

            headers = {
                "User-Agent": f"data-slow-scraper_{seconds_passed}s/1.0 (research purpose)"
            }

            print(f"Requesting: {url}")
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 429:
                print("429 received -> sleeping 60 seconds")
                time.sleep(60)
                continue

            response.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return response

        except requests.RequestException as e:
            print(f"Request error: {e} → sleeping 5 minutes")
            time.sleep(300)


# ================= POSTS =================

def fetch_posts(after=None):
    params = {"limit": 100}
    if after:
        params["after"] = after

    resp = reddit_get(POSTS_URL, params=params)
    return resp.json()


def extract_recent_posts(data):
    posts = []
    for child in data["data"]["children"]:
        post = child["data"]
        if post.get("created_utc", 0) >= cutoff:
            posts.append(post)
    return posts


# ================= COMMENTS =================

def fetch_top_comments(permalink):
    url = f"https://www.reddit.com{permalink}.json"
    params = {
        "sort": "top",
        "limit": 10
    }

    resp = reddit_get(url, params=params)
    data = resp.json()

    if len(data) < 2:
        print("No comments payload")
        return []

    children = data[1]["data"]["children"]
    if not children:
        print("Reddit returned empty comments")
        return []

    return parse_comments(children)


def parse_comments(children):
    results = []

    for child in children:
        if child.get("kind") != "t1":
            continue

        d = child["data"]
        comment = {
            "id": d.get("id"),
            "author": d.get("author"),
            "body": d.get("body"),
            "score": d.get("score"),
            "created_utc": datetime.utcfromtimestamp(
                d.get("created_utc", 0)
            ).isoformat() if d.get("created_utc") else None,
            "replies": []
        }

        replies = d.get("replies")
        if isinstance(replies, dict):
            comment["replies"] = parse_comments(
                replies.get("data", {}).get("children", [])
            )

        results.append(comment)

    return results


# ================= SENTIMENT =================

def collect_texts(comments, texts):
    for c in comments:
        if c.get("body"):
            texts.append(c["body"])
        if c.get("replies"):
            collect_texts(c["replies"], texts)


def sentiment_score(texts, analyzer):
    if not texts:
        return 0.0

    total = 0.0
    for t in texts:
        total += analyzer.polarity_scores(t)["compound"]

    return total / len(texts)


# ================= MAIN =================

def main():
    sia = SentimentIntensityAnalyzer()
    all_posts = []
    after = None

    print("=== Fetching posts ===")

    while True:
        data = fetch_posts(after)
        posts = data["data"]["children"]

        if not posts:
            break

        all_posts.extend(extract_recent_posts(data))

        oldest = posts[-1]["data"].get("created_utc", 0)
        if oldest < cutoff:
            break

        after = data["data"].get("after")
        if not after:
            break

    print(f"Fetched {len(all_posts)} posts")

    print("=== Fetching comments (VERY SLOW) ===")

    for i, post in enumerate(all_posts, 1):
        print(f"\nPost {i}/{len(all_posts)}: {post.get('title')}")

        texts = [post.get("title", ""), post.get("selftext", "")]

        comments = fetch_top_comments(post["permalink"])
        collect_texts(comments, texts)

        post["comments"] = comments
        post["sentiment_score"] = round(sentiment_score(texts, sia), 4)

    print("=== Writing CSV ===")

    fields = [
        "id", "title", "selftext", "author", "ups", "downs", "score",
        "num_comments", "created_utc", "upvote_ratio", "link_flair_text",
        "is_self", "is_video", "url", "permalink", "sentiment_score", "comments"
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for p in all_posts:
            writer.writerow({
                "id": p.get("id"),
                "title": p.get("title"),
                "selftext": p.get("selftext", ""), 
                "author": p.get("author"),
                "ups": p.get("ups"),
                "downs": p.get("downs"),
                "score": p.get("score"),
                "num_comments": p.get("num_comments"),
                "created_utc": datetime.utcfromtimestamp(
                    p.get("created_utc", 0)
                ).isoformat(),
                "upvote_ratio": p.get("upvote_ratio"),
                "link_flair_text": p.get("link_flair_text"),
                "is_self": p.get("is_self"),
                "is_video": p.get("is_video"),
                "url": p.get("url"),
                "permalink": "https://reddit.com" + p.get("permalink", ""),
                "sentiment_score": p.get("sentiment_score"),
                "comments": json.dumps(p.get("comments", []))
            })

    print(f"\nDONE -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
