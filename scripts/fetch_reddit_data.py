import requests
import csv
import time
import json
from datetime import datetime, timedelta, timezone
from transformers import pipeline
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate

# ================= CONFIG =================

SUBREDDIT = "crunchyroll"
POSTS_URL = f"https://www.reddit.com/r/{SUBREDDIT}/new.json"

DAYS_BACK = 60  # Number of days to look back for posts

OUTPUT_FILE = f"reddit_crunchyroll_{DAYS_BACK}days.csv"

REQUEST_DELAY = 3  # One request every 6 seconds to avoid rate limiting

# ========================================

cutoff = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).timestamp()


# ================= SAFE REQUEST =================

def reddit_get(url, params=None):
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

def fetch_posts(after=None):
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


def extract_recent_posts(data):
    """Filter posts to only include those within the DAYS_BACK cutoff.
    
    Args:
        data: JSON response from Reddit API
    
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

def fetch_top_comments(permalink):
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


def parse_comments(children):
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


def get_sentiment_bucket(score):
    """Categorize sentiment score into positive, negative, or neutral.
    
    Args:
        score: Sentiment score between -1 and 1
    
    Returns:
        'positive' if score >= 0.05, 'negative' if score <= -0.05, else 'neutral'
    """
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"


# ================= CATEGORIZATION =================

# Device patterns (simplified categories)
DEVICE_PATTERNS = {
    # Mobile (iOS and Android)
    "Mobile": [
        r"android(?!\s*tv)", r"android\s*phone", r"android\s*mobile",
        r"pixel\s*\d+", r"pixel\s*pro", r"pixel\s*fold", r"google\s*pixel", r"\bpixel\b",
        r"galaxy\s*s\d+", r"galaxy\s*a\d+", r"galaxy\s*note", r"galaxy\s*z", r"galaxy\s*fold", r"galaxy\s*flip", r"samsung\s*galaxy",
        r"oneplus", r"xiaomi", r"redmi", r"poco", r"oppo", r"vivo", r"motorola", r"moto\s*g",
        r"\bios\b", r"ipados",
        r"iphone\s*\d+", r"iphone\s*pro", r"iphone\s*max", r"iphone\s*plus", r"iphone\s*se", r"iphone",
        r"ipad\s*pro", r"ipad\s*air", r"ipad\s*mini", r"ipad",
        r"mobile", r"phone",
    ],
    
    # TV (Apple TV, Android TV)
    "TV": [
        r"android\s*tv", r"androidtv",
        r"nvidia\s*shield", r"shield\s*tv",
        r"apple\s*tv\s*4k", r"apple\s*tv", r"tvos",
        r"\btv\s*app\b",
    ],
    
    # Smart TV (Fire TV, Chromecast/Google TV, LG, Samsung, Sony, etc.)
    "Smart TV": [
        r"fire\s*tv", r"firetv", r"fire\s*stick", r"firestick", r"amazon\s*fire",
        r"chromecast", r"chrome\s*cast", r"google\s*tv",
        r"lg\s*tv", r"webos",
        r"samsung\s*tv", r"tizen",
        r"sony\s*tv",
        r"smart\s*tv", r"smarttv", r"vizio", r"hisense", r"tcl\s*tv", r"set\s*top\s*box",
    ],
    
    # Roku
    "Roku": [r"roku\s*ultra", r"roku\s*express", r"roku\s*streaming\s*stick", r"roku"],
    
    # Game Console
    "Game Console": [
        r"ps5", r"ps4", r"playstation\s*5", r"playstation\s*4", r"playstation",
        r"xbox\s*series", r"xbox\s*one", r"xbox",
        r"nintendo\s*switch", r"switch",
    ],
    
    # Browser
    "Browser": [
        r"safari", r"\bchrome\b(?!\s*cast)", r"firefox", r"\bedge\b",
        r"browser", r"web\s*player", r"webapp", r"website",
    ],
}

# Issue type patterns
ISSUE_TYPE_PATTERNS = {
    "Billing": [r"billing", r"charged", r"charge", r"payment", r"refund", r"invoice", r"price", r"cost", r"fee"],
    "Account": [r"account", r"login", r"log\s*in", r"password", r"sign\s*in", r"signin", r"logout", r"sign\s*out", r"profile", r"subscription", r"cancel", r"trial", r"premium", r"membership"],
    "Content - Subs Text/Typo": [r"subtitle", r"subtitles", r"subs", r"typo", r"translation", r"translated", r"text\s*error", r"wrong\s*text", r"misspell", r"caption"],
    "Content - Dubs": [r"\bdub\b", r"dubs", r"dubbing", r"dubbed", r"english\s*dub", r"japanese\s*dub", r"voice\s*actor", r"voice\s*over", r"audio\s*track"],
    "Content - Video Artifacts": [r"artifact", r"glitch", r"pixelat", r"blocky", r"visual\s*bug", r"video\s*quality", r"blurry", r"fuzzy", r"distort"],
    "Playback - Errors": [r"playback\s*error", r"error\s*code", r"won't\s*play", r"can't\s*play", r"cannot\s*play", r"not\s*playing", r"failed\s*to\s*play", r"video\s*error", r"stream\s*error"],
    "Playback - Rebuffers": [r"buffer", r"buffering", r"rebuffer", r"loading", r"stuck", r"freeze", r"freezing", r"frozen", r"lag", r"lagging", r"slow"],
    "Playback - Play Delay": [r"delay", r"latency", r"slow\s*start", r"takes\s*forever", r"long\s*load", r"won't\s*load"],
    "Playback - Subs Rendering": [r"subs?\s*not\s*showing", r"subtitle\s*rendering", r"subs?\s*out\s*of\s*sync", r"subs?\s*sync", r"subtitle\s*delay", r"subs?\s*timing", r"subs?\s*missing", r"no\s*subtitles?"],
    "UX/App - Crashes": [r"crash", r"crashing", r"crashed", r"force\s*close", r"app\s*closes", r"keeps\s*closing", r"shuts\s*down", r"not\s*responding"],
    "UX/App - Malformed UI": [r"ui\s*bug", r"interface", r"layout", r"display\s*issue", r"button", r"menu", r"navigation", r"screen\s*issue", r"visual\s*glitch", r"broken\s*ui"],
}


def categorize_text(text, patterns_dict):
    """Match text against pattern dictionary and return all matching categories.
    
    Args:
        text: Text to search (lowercase)
        patterns_dict: Dict of category -> list of regex patterns
    
    Returns:
        List of matching category names
    """
    import re
    matches = []
    for category, patterns in patterns_dict.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                matches.append(category)
                break  # Only add category once
    return matches


def extract_categories(title, selftext):
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


def flatten_comment_bodies(comments, bodies, limit):
    for comment in comments:
        if len(bodies) >= limit:
            return
        body = comment.get("body")
        if body:
            bodies.append(body)
        if comment.get("replies"):
            flatten_comment_bodies(comment["replies"], bodies, limit)


def summarize_post(llm, prompt, title, body, comments):
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

def main():
    """
    The sentiment-analysis pipeline uses a pretrained transformer-based text classification model 
    (a neural network fine-tuned on labeled sentiment data) to process your text, 
    outputting a label like POSITIVE or NEGATIVE with a confidence score.
    """
    sentiment_pipeline = pipeline("sentiment-analysis")

    """Initialize LLM and prompt template for summarization."""
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
        
        # Combine title, selftext, and comments for sentiment analysis
        sentiment_texts = []
        if post.get("title"):
            sentiment_texts.append(f"[TITLE] {post['title']}")
        if post.get("selftext"):
            sentiment_texts.append(f"[BODY] {post['selftext']}")
        for comment_body in comment_bodies:
            sentiment_texts.append(f"[COMMENT] {comment_body}")
        
        # Transformer sentiment on post title, selftext, and comments
        combined_text = " ".join(sentiment_texts)
        if combined_text:
            result = sentiment_pipeline(combined_text[:512])[0]
            post["sentiment_score_transformer"] = round(
                result["score"] if result["label"] == "POSITIVE" else -result["score"], 4
            )
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
