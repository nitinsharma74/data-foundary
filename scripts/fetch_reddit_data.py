import csv
import json
import time
from enum import Enum
from typing import Any, Optional

import numpy as np
import requests
from pydantic import BaseModel, Field
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


# ================= CATEGORIZATION (LLM-based) =================

class IssueCategory(str, Enum):
    billing = "Billing"
    account = "Account"
    content = "Content"
    playback = "Playback"
    ux_app = "UX/App"
    licensing_catalog = "Licensing/Catalog"
    general_discussion = "General Discussion"
    unknown = "Unknown"


class Platform(str, Enum):
    android_mobile = "Android (Mobile)"
    android_tv = "Android TV"
    fire_tv = "Fire TV"
    chromecast = "Chromecast"
    game_console = "Game Console"
    roku = "Roku"
    smart_tv_stb = "Smart TV / STB (Tetsuo)"
    browser_safari = "Browser (Safari)"
    browser_non_safari = "Browser (Non-Safari)"
    windows_app = "Windows App"
    ios = "iOS"
    tvos = "tvOS"
    unknown = "Unknown"


class RedditPostClassification(BaseModel):
    issue_category: IssueCategory = Field(
        description="The type of issue reported in the Reddit post. Use 'Unknown' if unclear."
    )
    platform: Platform = Field(
        default=Platform.unknown,
        description="The platform mentioned in the post. Use 'Unknown' if no device is mentioned."
    )


CLASSIFICATION_PROMPT = PromptTemplate(
    input_variables=["post_text", "comments_text"],
    template=(
        "You are an expert at classifying Reddit posts about a streaming service.\n\n"
        "Your task: Identify the PLATFORM (device) and ISSUE CATEGORY from the post.\n\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "STEP 1: PLATFORM DETECTION (Think carefully!)\n"
        "═══════════════════════════════════════════════════════════════════\n\n"
        "You MUST identify the platform even if not explicitly stated. Use your knowledge:\n\n"
        "GAME CONSOLE → PlayStation, PS3, PS4, PS5, Xbox, Xbox One, Xbox Series X/S, Nintendo Switch, Nintendo\n"
        "iOS → iPhone (any model), iPad (any model), iOS, iPod Touch\n"
        "tvOS → Apple TV, AppleTV\n"
        "Android (Mobile) → Any Android phone including: Samsung Galaxy, Google Pixel, OnePlus, Xiaomi, Redmi, "
        "Motorola/Moto (G5, G Power, Edge, etc.), Oppo, Vivo, Realme, Huawei, Honor, Nothing Phone, "
        "Sony Xperia, LG phone, HTC, Nokia phone, Asus ROG Phone, ZTE, Poco, Android mobile\n"
        "Android TV → Android TV, Google TV device, Sony Bravia (with Android), Nvidia Shield, Mi Box, "
        "Hisense Android TV, Philips Android TV, TCL with Google TV\n"
        "Fire TV → Fire TV, Fire TV Stick, Fire Stick, Firestick, Fire TV Cube, Amazon Fire TV, Fire TV 4K\n"
        "Roku → Roku, Roku TV, Roku Stick, Roku Express, Roku Ultra, TCL Roku TV, Hisense Roku TV\n"
        "Chromecast → Chromecast, Chromecast with Google TV, Google TV (dongle)\n"
        "Smart TV / STB (Tetsuo) → LG TV (webOS), Samsung TV (Tizen), Vizio TV, TCL TV (non-Roku/non-Google), "
        "Hisense TV (non-Android), Sony TV (non-Android), Panasonic TV, set-top box, cable box, STB\n"
        "Browser (Safari) → Safari browser, Safari on Mac\n"
        "Browser (Non-Safari) → Chrome, Firefox, Edge, Brave, Opera, Vivaldi, browser, website, web, "
        "watching on computer, laptop, desktop, MacBook (if using browser), PC (if using browser)\n"
        "Windows App → Windows app, PC app, Windows 10, Windows 11, Microsoft Store app, desktop app on Windows\n\n"
        "REASONING RULES:\n"
        "- If user mentions a phone model (e.g., 'Moto G5', 'Pixel 7', 'Galaxy S23') → likely Android (Mobile) or iOS\n"
        "- If user mentions 'phone' or 'mobile' without specifics → infer from context or use Android (Mobile) as default\n"
        "- If user mentions 'TV' without specifics → try to infer from other clues, otherwise Smart TV / STB (Tetsuo)\n"
        "- If user mentions 'app' → determine which platform based on context (phone app, TV app, etc.)\n"
        "- If user mentions 'streaming on my...' → identify the device\n"
        "- If multiple platforms mentioned → pick the PRIMARY one the issue is about\n"
        "- If truly NO device/platform can be inferred → Unknown\n\n"
        "SCOPE CHECK (for Account vs UX/App):\n"
        "- Read the comments carefully. If multiple commenters report experiencing the SAME issue, it's likely UX/App.\n"
        "- If the problem seems unique to the poster's account with no others confirming, it's Account.\n"
        "- Posts asking 'anyone else having this issue?' where comments confirm → UX/App.\n\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "STEP 2: ISSUE CATEGORY\n"
        "═══════════════════════════════════════════════════════════════════\n\n"
        "- Billing: payments, charges, refunds, subscriptions, pricing, renewal, free trial ended\n"
        "- Account: Issues affecting the INDIVIDUAL USER'S ACCOUNT — login problems, password reset, "
        "email verification, profile settings, account linking, bans, subscription status specific to their account. "
        "Use this when the problem seems unique to the poster's account.\n"
        "- Content: subtitle TEXT errors, typos in subs, dubbing quality, wrong translation, "
        "missing dub, audio track issues, video artifacts/glitches in the content itself\n"
        "- Playback: video won't play, buffering, loading, streaming errors, error codes, "
        "playback quality, resolution issues, subtitle SYNC/timing issues, audio sync, "
        "video freezing, can't stream, connection issues\n"
        "- UX/App: Issues affecting MULTIPLE USERS or the app itself — app crashes, app not opening, "
        "UI bugs, navigation issues, missing features, app update problems, download feature issues, "
        "watchlist issues, search not working, widespread login outages. "
        "If comments show others experiencing the same problem, classify as UX/App.\n"
        "- Licensing/Catalog: show removed, region-locked, not available in library, missing episodes, "
        "'when will X be added', simulcast schedule questions, content availability, geo-restrictions\n"
        "- General Discussion: recommendations, opinions, polls, non-issue questions like 'do adults use Crunchyroll?', "
        "comparisons to other services, general praise or complaints not tied to a specific issue\n"
        "- Unknown: only if the issue truly does not fit any category above\n\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "NOW CLASSIFY THIS POST:\n"
        "═══════════════════════════════════════════════════════════════════\n"
        "{post_text}\n\n"
        "**comments:**\n{comments_text}"
    )
)


def extract_categories(
    classification_chain: Any,
    title: str | None,
    selftext: str | None,
    comments_text: str | None = None
) -> dict[str, str]:
    """Extract device and issue type from post title, body, and comments using LLM.
    
    Args:
        classification_chain: The LLM chain for classification
        title: Post title
        selftext: Post body text
        comments_text: Concatenated comment text from the post
    
    Returns:
        Dict with device and issue_type fields
    """
    combined_text = f"**title:** {title or ''}\n**body:** {selftext or ''}"
    
    print(f"Classifying post: {title[:50] if title else 'No title'}...")
    
    try:
        result = classification_chain.invoke({
            "post_text": combined_text,
            "comments_text": comments_text or "No comments available."
        })
        return {
            "device": result.platform.value if result.platform else "Unknown",
            "issue_type": result.issue_category.value if result.issue_category else "Unknown",
        }
    except Exception as e:
        print(f"Classification error: {e}")
        return {
            "device": "Unknown",
            "issue_type": "Unknown",
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
    
    # Initialize LLM chain for classification (with structured output)
    classification_llm = ChatOllama(
        model="deepseek-r1:8b",
        temperature=0.0,
        reasoning=False,
        num_predict=512
    ).with_structured_output(RedditPostClassification)
    
    classification_chain = CLASSIFICATION_PROMPT | classification_llm

    prompt = PromptTemplate.from_template(
    """
    You are summarizing a Reddit thread.

    CRITICAL OUTPUT CONSTRAINTS (must follow):
    - Output MUST contain ONLY the lines described in "Required format".
    - Do NOT include any headings (e.g., "EXAMPLE", "RULES", "INPUT"), preambles, explanations, or quotes.
    - Do NOT repeat the prompt, the rules, or the input.
    - The first characters of your response MUST be: "Post summary:"

    Required format (exactly):
    Post summary: <1–2 sentences>
    Comments:
    - <bullet>
    - <bullet>
    (...)

    Task:
    1) Post summary:
    - Summarize ONLY the Title + Selftext.
    - State the core topic/complaint/question and key specifics ONLY if explicitly present.
    - Do NOT use any information from comments.

    2) Comments:
    - Summarize what the comments discuss (themes), not individual commenters.
    - Use short bullets; keep distinct themes separate.
    - Aggregate repeats; use frequency words when helpful (Most/Many/Some/Few).
    - Capture: confirmations, disagreements, advice/workarounds, requests for info, explanations, outcomes, sentiment (only if widespread).
    - Ignore one-off jokes/sarcasm/pure reactions unless they reflect a common theme.
    - Do NOT invent details.

    Edge cases:
    - If Selftext is empty/None, summarize using Title only.
    - If there are no comments or comments are empty/None:
    Comments:
    - No comments provided.

    INPUT (use only what is inside the tags):
    <THREAD>
    Title: {title}
    Selftext: {body}
    Comments:
    {comments}
    </THREAD>
    """
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
        flatten_comment_bodies(comments, comment_bodies, limit=50)
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
        
        # Extract device and issue type categories using LLM
        categories = extract_categories(
            classification_chain,
            post.get("title", ""),
            post.get("selftext", ""),
            comments_text
        )
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
