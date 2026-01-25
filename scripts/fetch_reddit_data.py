import re
import csv
import json
import time
from typing import Any

import numpy as np
import requests
from datetime import datetime, timedelta, timezone
from transformers import pipeline
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate

# ================= CONFIG =================

SUBREDDIT = "crunchyroll"
POSTS_URL = f"https://www.reddit.com/r/{SUBREDDIT}/new.json"

DAYS_BACK = 60  # Number of days to look back for posts

OUTPUT_FILE = f"reddit_crunchyroll_{DAYS_BACK}days.csv"

REQUEST_DELAY = 3  # Seconds between requests to avoid rate limiting

# ========================================


# ================= SAFE REQUEST =================

def reddit_get(url: str, params: dict[str, Any] | None = None) -> requests.Response:
    """Safely make a GET request to Reddit with rate limiting and error handling.
    
    Retries on 429 (rate limit) and other request exceptions.
    """
    while True:
        try:
            now = datetime.now(timezone.utc)
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

def fetch_posts(after: str | None = None) -> dict[str, Any]:
    """Fetch a batch of posts from the subreddit.
    
    Args:
        after: Pagination token for fetching next batch
    
    Returns:
        JSON response containing posts
    """
    params = {"limit": 100}
    if after:
        params["after"] = after

    resp = reddit_get(POSTS_URL, params=params)
    return resp.json()


def extract_recent_posts(data: dict[str, Any], cutoff: float) -> list[dict[str, Any]]:
    """Filter posts to only include those within the DAYS_BACK cutoff.
    
    Args:
        data: JSON response from Reddit API
        cutoff: Unix timestamp cutoff for filtering posts
    
    Returns:
        List of posts created after the cutoff timestamp
    """
    posts = []
    for child in data["data"]["children"]:
        post = child["data"]
        if post.get("created_utc", 0) >= cutoff:
            posts.append(post)
    return posts


# ================= COMMENTS =================

def fetch_top_comments(permalink: str) -> list[dict[str, Any]]:
    """Fetch top 200 comments for a post, including nested replies.
    
    Args:
        permalink: Post permalink path (e.g., /r/subreddit/comments/id/title/)
    
    Returns:
        List of parsed comment dictionaries with nested replies
    """
    url = f"https://www.reddit.com{permalink}.json"
    params = {
        "sort": "top",
        "limit": 200
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


def parse_comments(children: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recursively parse comment objects from Reddit API response.
    
    Args:
        children: List of comment objects from Reddit API
    
    Returns:
        List of comment dictionaries with id, author, body, score, timestamp, and nested replies
    """
    results = []

    for child in children:
        if child.get("kind") != "t1":
            continue

        d = child["data"]
        # Remove AutoModerator comments
        if d.get("author") != "AutoModerator":
            comment = {
                "id": d.get("id"),
                "author": d.get("author"),
                "body": d.get("body"),
                "score": d.get("score"),
                "created_utc": datetime.fromtimestamp(
                    d.get("created_utc", 0), timezone.utc
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


def get_sentiment_bucket(score: float) -> str:
    if score >= 0.20:
        return "positive"
    elif score <= -0.20:
        return "negative"
    else:
        return "neutral"


# ================= SENTIMENT SCORING =================

LABEL_TO_SCORE = {
    "very negative": -1.0,
    "negative": -0.5,
    "neutral": 0.0,
    "positive": 0.5,
    "very positive": 1.0
}


def score_text(sentiment_pipeline: Any, text: str) -> float:
    """Score text using the sentiment pipeline with label mapping.
    
    Args:
        sentiment_pipeline: The transformer sentiment analysis pipeline
        text: Text to analyze
    
    Returns:
        Sentiment score between -1 and 1
    """
    if not text or not text.strip():
        return 0.0
    result = sentiment_pipeline(text)[0]
    label = result["label"].lower()
    confidence = result["score"]
    base = LABEL_TO_SCORE.get(label, 0.0)
    return round(base * confidence, 4)


# ================= CATEGORIZATION =================

# Device patterns (simplified - prioritize precision over recall)
DEVICE_PATTERNS = {
    "Mobile": [
        r"iphone", r"ipad", r"\bios\b",
        r"android(?!\s*tv)",
        r"(on\s+my|the)\s+phone", r"(phone|mobile)\s+app",
    ],
    
    "TV": [
        # Streaming devices
        r"apple\s*tv", r"android\s*tv", r"fire\s*tv", r"firestick",
        r"roku", r"chromecast", r"google\s*tv",
        r"smart\s*tv",
        r"\btv\s+app\b",
    ],
    
    "Game Console": [
        r"ps[45]\b", r"playstation", r"xbox", r"nintendo\s*switch",
    ],
    
    "Web": [
        r"(chrome|firefox|safari)\s+browser",
        r"\b(web|browser)\s+(app|player|version)\b",
        r"\bwebsite\b", r"crunchyroll\.com",
    ],
    
    "Desktop": [
        r"windows\s+(app|10|11)", r"\bpc\s+app\b",
        r"mac\s*(book|os)", r"desktop\s+app",
    ],
}

# Issue type patterns (simplified categories)
ISSUE_TYPE_PATTERNS = {
    "Billing": [
        r"billing", r"charged", r"charge", r"payment", r"refund", r"invoice", r"price", r"cost", r"fee"
    ],
    "Account": [
        r"account", r"login", r"log\s*in", r"password", r"sign\s*in", r"signin", r"logout", r"sign\s*out",
        r"profile", r"subscription", r"cancel", r"trial", r"premium", r"membership"
    ],
    "Content - Subs & Dubs": [
        # Subtitles/Text
        r"subtitle", r"subtitles", r"subs", r"typo", r"translation", r"translated", r"text\s*error",
        r"wrong\s*text", r"misspell", r"caption",
        # Dubs/Audio
        r"\bdub\b", r"dubs", r"dubbing", r"dubbed", r"english\s*dub", r"japanese\s*dub",
        r"voice\s*actor", r"voice\s*over", r"audio\s*track",
        # Video artifacts
        r"artifact", r"glitch", r"pixelat", r"blocky", r"visual\s*bug", r"video\s*quality",
        r"blurry", r"fuzzy", r"distort"
    ],
    "Playback": [
        # Errors
        r"playback\s*error", r"error\s*code", r"won't\s*play", r"can't\s*play", r"cannot\s*play",
        r"not\s*playing", r"failed\s*to\s*play", r"video\s*error", r"stream\s*error",
        # Rebuffers
        r"buffer", r"buffering", r"rebuffer", r"loading", r"stuck", r"freeze", r"freezing",
        r"frozen", r"lag", r"lagging", r"slow",
        # Play delay
        r"delay", r"latency", r"slow\s*start", r"takes\s*forever", r"long\s*load", r"won't\s*load",
        # Subs rendering
        r"subs?\s*not\s*showing", r"subtitle\s*rendering", r"subs?\s*out\s*of\s*sync", r"subs?\s*sync",
        r"subtitle\s*delay", r"subs?\s*timing", r"subs?\s*missing", r"no\s*subtitles?"
    ],
    "UX/App": [
        # Crashes
        r"crash", r"crashing", r"crashed", r"force\s*close", r"app\s*closes", r"keeps\s*closing",
        r"shuts\s*down", r"not\s*responding",
        # UI issues
        r"ui\s*bug", r"interface", r"layout", r"display\s*issue", r"button", r"menu",
        r"navigation", r"screen\s*issue", r"visual\s*glitch", r"broken\s*ui"
    ],
}


def categorize_text(text: str, patterns_dict: dict[str, list[str]]) -> list[str]:
    """Match text against pattern dictionary and return all matching categories.
    
    Args:
        text: Text to search (lowercase)
        patterns_dict: Dict of category -> list of regex patterns
    
    Returns:
        List of matching category names
    """
    matches = []
    for category, patterns in patterns_dict.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                matches.append(category)
                break  # Only add category once
    return matches


def extract_categories(title: str | None, selftext: str | None) -> dict[str, str]:
    """Extract device and issue type from post title and body.
    
    Args:
        title: Post title
        selftext: Post body text
    
    Returns:
        Dict with device and issue_type fields
    """
    combined_text = f"{title or ''} {selftext or ''}".lower()
    
    # Extract devices (platforms and specific devices)
    devices = categorize_text(combined_text, DEVICE_PATTERNS)
    
    # Extract issue types
    issues = categorize_text(combined_text, ISSUE_TYPE_PATTERNS)
    
    return {
        "device": ", ".join(devices) if devices else "",
        "issue_type": ", ".join(issues) if issues else "",
    }


def flatten_comment_bodies(
    comments: list[dict[str, Any]],
    bodies: list[str],
    limit: int
) -> None:
    """Recursively extract comment body text up to a limit.
    
    Args:
        comments: List of comment dictionaries with nested replies
        bodies: Output list to append comment bodies to (modified in-place)
        limit: Maximum number of comment bodies to collect
    """
    for comment in comments:
        if len(bodies) >= limit:
            return
        body = comment.get("body")
        if body:
            bodies.append(body)
        if comment.get("replies"):
            flatten_comment_bodies(comment["replies"], bodies, limit)


def summarize_post(
    llm: ChatOllama,
    prompt: PromptTemplate,
    title: str,
    body: str,
    comments: str
) -> str:
    """Generate an LLM summary of a Reddit post.
    
    Args:
        llm: ChatOllama language model instance
        prompt: Prompt template for summarization
        title: Post title
        body: Post body text
        comments: Concatenated comment text
    
    Returns:
        Summary string or empty string if no content
    """
    if not (title or body or comments):
        return ""

    formatted = prompt.format(
        title=title or "",
        body=body or "",
        comments=comments or "",
    )
    response = llm.invoke(formatted)
    return getattr(response, "content", response) or ""


# ================= MAIN =================

def main() -> None:
    """Main entry point for Reddit data fetching and analysis.
    
    Fetches posts from the configured subreddit, analyzes sentiment using a
    transformer model, generates summaries with an LLM, and writes results to CSV.
    """
    # Calculate cutoff timestamp for filtering posts
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).timestamp()
    
    # Initialize sentiment analysis pipeline
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="spacesedan/reddit-sentiment-analysis-longformer",
        max_length=4096,
        truncation=True,
    )

    # Initialize LLM and prompt template for summarization
    llm = ChatOllama(
        model="llama3.2:1b",
        temperature=0.0,
        reasoning=None,
        num_predict=256
    )
    
    prompt = PromptTemplate.from_template(
        "Summarize this Reddit post and its comments in 2-3 sentences.\n\n"
        "RULES:\n"
        "- State the main topic or issue being discussed\n"
        "- Note the overall community reaction (positive, negative, or mixed)\n"
        "- Mention specific complaints or praises if repeated by multiple users\n"
        "- Include relevant context: device (iOS/Android/TV/PC), app version, or feature mentioned\n"
        "- Output ONLY the summary text, no labels or introductions\n"
        "- Do not invent information not present in the content\n\n"
        "Post title: {title}\n"
        "Post body: {body}\n"
        "Top comments:\n{comments}\n\n"
        "Summary:"
    )

    all_posts = []
    after = None

    print("=== Fetching posts ===")

    while True:
        data = fetch_posts(after)
        posts = data["data"]["children"]

        if not posts:
            break

        all_posts.extend(extract_recent_posts(data, cutoff))

        oldest = posts[-1]["data"].get("created_utc", 0)
        if oldest < cutoff:
            break

        after = data["data"].get("after")
        if not after:
            break

    print(f"Fetched {len(all_posts)} posts")

    print("=== Fetching comments ===")

    for i, post in enumerate(all_posts, 1):
        print(f"\nPost {i}/{len(all_posts)}: {post.get('title')}")

        comments = fetch_top_comments(post["permalink"])
        post["comments"] = comments
        comment_bodies = []
        flatten_comment_bodies(comments, comment_bodies, limit=20)
        comments_text = "\n".join(comment_bodies)
        try:
            post["post_summary"] = summarize_post(
                llm,
                prompt,
                post.get("title", ""),
                post.get("selftext", ""),
                comments_text,
            )
        except Exception as exc:
            print(f"Summary error: {exc}")
            post["post_summary"] = ""
        
        # Score title and selftext
        post_scores = []
        if post.get("title"):
            post_scores.append(score_text(sentiment_pipeline, post["title"]))
        if post.get("selftext"):
            post_scores.append(score_text(sentiment_pipeline, post["selftext"]))
        
        # Score comments (weighted at 0.3)
        comment_scores = [score_text(sentiment_pipeline, c) for c in comment_bodies]
        
        # Calculate final score with weighted comment average
        if post_scores or comment_scores:
            final_score = np.mean([
                *post_scores,
                np.mean(comment_scores) * 0.3 if comment_scores else 0.0
            ])
            final_score = round(float(final_score), 4)
            final_score = np.clip(final_score, -0.8, 0.8)
            post["sentiment_score_transformer"] = float(final_score)
            print(f"Final Score: {final_score}")
        else:
            post["sentiment_score_transformer"] = 0.0
        post["sentiment_bucket_transformer"] = get_sentiment_bucket(post["sentiment_score_transformer"])
        
        # Extract device and issue type categories
        categories = extract_categories(post.get("title", ""), post.get("selftext", ""))
        post["device"] = categories["device"]
        post["issue_type"] = categories["issue_type"]

    print("=== Writing CSV ===")

    fields = [
        "id", "title", "selftext", "author", "ups", "downs", "score",
        "num_comments", "created_utc", "upvote_ratio", "link_flair_text",
        "is_self", "is_video", "url", "permalink", "sentiment_score_transformer",
        "sentiment_bucket_transformer", "device", "issue_type",
        "post_summary", "comments"
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
                "created_utc": datetime.fromtimestamp(
                    p.get("created_utc", 0), timezone.utc
                ).isoformat(),
                "upvote_ratio": p.get("upvote_ratio"),
                "link_flair_text": p.get("link_flair_text"),
                "is_self": p.get("is_self"),
                "is_video": p.get("is_video"),
                "url": p.get("url"),
                "permalink": "https://reddit.com" + p.get("permalink", ""),
                "sentiment_score_transformer": p.get("sentiment_score_transformer"),
                "sentiment_bucket_transformer": p.get("sentiment_bucket_transformer"),
                "device": p.get("device", ""),
                "issue_type": p.get("issue_type", ""),
                "post_summary": p.get("post_summary", ""),
                "comments": json.dumps(p.get("comments", []))
            })

    print(f"\nDONE -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
