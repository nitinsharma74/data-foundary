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

DAYS_BACK = 5  # Number of days to look back for posts

OUTPUT_FILE = f"reddit_crunchyroll_{DAYS_BACK}days.csv"

REQUEST_DELAY = 3  # Seconds between requests to avoid rate limiting

MAX_COMMENTS_PER_POST = 2000  # Maximum comments to fetch per post

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

def extract_post_id_from_permalink(permalink: str) -> str:
    """Extract the post ID (t3_xxx format) from a permalink.
    
    Args:
        permalink: Post permalink path (e.g., /r/subreddit/comments/abc123/title/)
    
    Returns:
        Post fullname in t3_xxx format
    """
    parts = permalink.strip("/").split("/")
    for i, part in enumerate(parts):
        if part == "comments" and i + 1 < len(parts):
            return f"t3_{parts[i + 1]}"
    return ""


def fetch_more_children(
    link_id: str,
    children_ids: list[str],
    sort: str = "top"
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch additional comments using /api/morechildren endpoint.
    
    Args:
        link_id: Post fullname (e.g., t3_abc123)
        children_ids: List of comment ID36s to fetch
        sort: Sort order for comments
    
    Returns:
        Tuple of (parsed comments, more objects for further fetching)
    """
    if not children_ids:
        return [], []
    
    url = "https://www.reddit.com/api/morechildren.json"
    params = {
        "api_type": "json",
        "link_id": link_id,
        "children": ",".join(children_ids[:100]),  # API limit of 100 per request
        "sort": sort,
        "limit_children": "false",
    }
    
    try:
        resp = reddit_get(url, params=params)
        data = resp.json()
        things = data.get("json", {}).get("data", {}).get("things", [])
        return parse_comments_with_more(things)
    except Exception as e:
        print(f"Error fetching more children: {e}")
        return [], []


def fetch_top_comments(
    permalink: str,
    target_count: int = MAX_COMMENTS_PER_POST
) -> list[dict[str, Any]]:
    """Fetch up to target_count comments for a post, including nested replies.
    
    Args:
        permalink: Post permalink path (e.g., /r/subreddit/comments/id/title/)
        target_count: Maximum number of comments to fetch (default: MAX_COMMENTS_PER_POST)
    
    Returns:
        List of parsed comment dictionaries with nested replies
    """
    url = f"https://www.reddit.com{permalink}.json"
    params = {
        "sort": "top",
        "limit": 500,  # Request more initially
        "depth": 20,   # Get deeper nesting
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

    # Parse initial comments and collect "more" objects
    comments, more_objects = parse_comments_with_more(children)
    
    # Extract post ID for morechildren API calls
    post_id = extract_post_id_from_permalink(permalink)
    
    if not post_id:
        print(f"Could not extract post ID from permalink: {permalink}")
        return comments
    
    # Count total comments (including nested)
    def count_comments(comment_list: list[dict[str, Any]]) -> int:
        total = 0
        for c in comment_list:
            total += 1
            if c.get("replies"):
                total += count_comments(c["replies"])
        return total
    
    total_fetched = count_comments(comments)
    print(f"  Initial fetch: {total_fetched} comments, {len(more_objects)} 'more' stubs")
    
    # Fetch additional comments from "more" objects until we hit target
    iterations = 0
    max_iterations = 50  # Safety limit to prevent infinite loops
    
    while total_fetched < target_count and more_objects and iterations < max_iterations:
        iterations += 1
        more_obj = more_objects.pop(0)
        more_children_ids = more_obj.get("data", {}).get("children", [])
        
        if not more_children_ids:
            continue
        
        # Fetch in batches of 100 (API limit)
        for i in range(0, len(more_children_ids), 100):
            if total_fetched >= target_count:
                break
                
            batch = more_children_ids[i:i + 100]
            additional, new_more = fetch_more_children(post_id, batch, sort="top")
            
            comments.extend(additional)
            more_objects.extend(new_more)
            
            batch_count = count_comments(additional)
            total_fetched += batch_count
            
            if batch_count > 0:
                print(f"  Fetched {batch_count} more comments (total: {total_fetched})")
    
    print(f"  Final comment count: {total_fetched}")
    return comments


def parse_comments_with_more(
    children: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Recursively parse comment objects and collect 'more' stubs for later fetching.
    
    Args:
        children: List of comment objects from Reddit API
    
    Returns:
        Tuple of (parsed comments, 'more' objects for additional fetching)
    """
    results = []
    more_objects = []

    for child in children:
        kind = child.get("kind")
        
        if kind == "t1":
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
                    nested_comments, nested_more = parse_comments_with_more(
                        replies.get("data", {}).get("children", [])
                    )
                    comment["replies"] = nested_comments
                    more_objects.extend(nested_more)

                results.append(comment)
                
        elif kind == "more":
            # Collect "more" stubs for later fetching
            more_objects.append(child)

    return results, more_objects


def parse_comments(children: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recursively parse comment objects from Reddit API response.
    
    Args:
        children: List of comment objects from Reddit API
    
    Returns:
        List of comment dictionaries with id, author, body, score, timestamp, and nested replies
    """
    comments, _ = parse_comments_with_more(children)
    return comments


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
    announcements = "Announcements"
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
    multiple_devices = "Multiple Devices"
    unknown = "Unknown"


class IssueCategoryResult(BaseModel):
    """Result model for issue category classification."""
    issue_category: IssueCategory = Field(
        description="The type of issue reported in the Reddit post. Use 'Unknown' if unclear."
    )


class PlatformResult(BaseModel):
    """Result model for platform detection."""
    platform: Platform = Field(
        default=Platform.unknown,
        description="The platform mentioned in the post. Use 'Unknown' if no device is mentioned."
    )


# Flair to category mapping (used as high-priority hint, LLM confirms)
FLAIR_HINT_MAP: dict[str, list[str]] = {
    "News": ["Announcements"],
    "Discord": ["Announcements", "General Discussion"],
    "Streaming Catalog": ["Licensing/Catalog"],
    "Service Discussion": ["General Discussion"],
    "Account/Membership": ["Account", "Billing"],
    "Streaming | Technical Issue": ["UX/App", "Playback"],
    "Dubs": ["Licensing/Catalog", "General Discussion", "Content"],
}


# Issue Category Prompt (used when flair is ambiguous or missing)
ISSUE_CATEGORY_PROMPT = PromptTemplate(
    input_variables=["post_text", "comments_text", "flair_hint"],
    template=(
        "Classify this Reddit post into ONE issue category.\n\n"
        "{flair_hint}"
        "MANDATORY FLAIR RULES:\n"
        "- If flair hint says 'News' → MUST choose Announcements\n"
        "- If flair hint suggests a category, follow it unless content is COMPLETELY unrelated\n\n"
        "CATEGORIES:\n"
        "Announcements → official news, pricing updates, dub release announcements, service updates, company announcements, ANY post with 'News' flair\n"
        "Billing → USER complaints about their personal payments, charges, refunds (NOT news about pricing changes)\n"
        "Account → login, password reset, email verification, profile settings, account linking (user-specific, NOT outages)\n"
        "Playback → video won't play, buffering, error codes, loading, subtitle NOT displaying, audio sync\n"
        "Content → subtitle TEXT errors, wrong translation, dub script quality, voice acting issues\n"
        "UX/App → app crashes, UI bugs, updates, navigation, downloads, watchlist, skip button, outages, server errors\n"
        "Licensing/Catalog → show removed, region-locked, missing episodes, geo-restrictions, 'when will X be added'\n"
        "General Discussion → recommendations, opinions, feature requests, general questions (NEVER for News flair)\n"
        "Unknown → cannot determine, ambiguous\n\n"
        "KEY RULES:\n"
        "- News flair = Announcements. ALWAYS.\n"
        "- News/pricing announcements → Announcements (NOT Billing, NOT General Discussion)\n"
        "- Account = user-specific issue. UX/App = affects anyone.\n"
        "- Content = subtitle/translation issues. Playback = video/audio not working.\n"
        "- Feature requests → General Discussion.\n\n"
        "POST:\n{post_text}\n\n"
        "COMMENTS:\n{comments_text}"
    )
)

# Platform Detection Prompt
PLATFORM_PROMPT = PromptTemplate(
    input_variables=["post_text", "comments_text"],
    template=(
        "Identify the PLATFORM/DEVICE where the issue is occurring.\n\n"
        "CRITICAL RULES FOR DEVICE EXTRACTION:\n"
        "1. PRIMARY: Look for device EXPLICITLY mentioned by OP in title/body\n"
        "2. COMMENTS: Only use if commenters report the SAME issue on OTHER devices\n"
        "   - If OP reports issue on Device A, and comments confirm same issue on Device B → Multiple Devices\n"
        "   - Do NOT extract devices from comments discussing unrelated topics\n"
        "3. NO DEVICE: If post is news/announcement/general discussion with no user issue → Unknown\n\n"
        "Do NOT infer devices from:\n"
        "- Comments just mentioning what device they use (without confirming the issue)\n"
        "- News articles, announcements, or pricing updates\n"
        "- General discussions without a specific technical issue\n\n"
        "PLATFORMS:\n"
        "Game Console → PlayStation, PS3/4/5, Xbox, Nintendo Switch, console\n"
        "iOS → iPhone, iPad, iOS\n"
        "tvOS → Apple TV\n"
        "Android (Mobile) → Samsung Galaxy, Google Pixel, OnePlus, Xiaomi, Motorola, Android phone\n"
        "Android TV → Android TV, Google TV, Nvidia Shield, Mi Box\n"
        "Fire TV → Fire TV, Fire TV Stick, Firestick, Fire TV Cube\n"
        "Roku → Roku, Roku TV, Roku Stick\n"
        "Chromecast → Chromecast, Chromecast with Google TV\n"
        "Smart TV / STB (Tetsuo) → LG TV, Samsung TV (Tizen), Vizio TV, Panasonic TV, set-top box\n"
        "Browser (Safari) → Safari browser\n"
        "Browser (Non-Safari) → Chrome, Firefox, Edge, browser, website, PC, laptop, desktop\n"
        "Windows App → Windows app, PC app, Microsoft Store app\n"
        "Multiple Devices → SAME issue confirmed on 2+ different device types\n"
        "Unknown → no device mentioned, or post is news/announcement\n\n"
        "RULES:\n"
        "- Phone brand → Android (Mobile) or iOS accordingly\n"
        "- Unspecified 'phone'/'mobile' → Android (Mobile)\n"
        "- Unspecified 'TV' → Smart TV / STB (Tetsuo)\n"
        "- 'PC'/'computer'/'laptop' → Browser (Non-Safari)\n"
        "- 'works on X but not Y' → classify as Y (the broken device)\n\n"
        "POST:\n{post_text}\n\n"
        "COMMENTS:\n{comments_text}"
    )
)


def extract_categories(
    issue_chain: Any,
    platform_chain: Any,
    title: str | None,
    selftext: str | None,
    comments_text: str | None = None,
    flair: str | None = None
) -> dict[str, str]:
    """Extract device and issue type using LLM with flair as high-priority hint.
    
    Args:
        issue_chain: LLM chain for issue category classification
        platform_chain: LLM chain for platform detection
        title: Post title
        selftext: Post body text
        comments_text: Concatenated comment text from the post
        flair: Reddit link flair text (high-priority hint)
    
    Returns:
        Dict with device and issue_type fields
    """
    combined_text = f"**title:** {title or ''}\n**body:** {selftext or ''}"
    comments = comments_text or "No comments available."
    
    print(f"Classifying post: {title[:50] if title else 'No title'}...")
    
    # === ISSUE CATEGORY: Always use LLM, flair as high-priority hint ===
    issue_type = "Unknown"
    try:
        # Build flair hint based on mapping
        if flair and flair in FLAIR_HINT_MAP:
            options = FLAIR_HINT_MAP[flair]
            if len(options) == 1:
                flair_hint = f"FLAIR HINT (HIGH PRIORITY): Post has '{flair}' flair. This strongly suggests: {options[0]}. Only choose differently if content CLEARLY contradicts this.\n\n"
            else:
                flair_hint = f"FLAIR HINT (HIGH PRIORITY): Post has '{flair}' flair. Choose from these likely categories: {', '.join(options)}. Only choose outside these if content CLEARLY contradicts.\n\n"
        elif flair:
            flair_hint = f"FLAIR INFO: Post has '{flair}' flair (unmapped, use content to classify).\n\n"
        else:
            flair_hint = ""
        
        result = issue_chain.invoke({
            "post_text": combined_text,
            "comments_text": comments,
            "flair_hint": flair_hint
        })
        issue_type = result.issue_category.value if result.issue_category else "Unknown"
        
        # Log with flair alignment info
        if flair and flair in FLAIR_HINT_MAP:
            expected = FLAIR_HINT_MAP[flair]
            if issue_type in expected:
                print(f"  [ISSUE] flair-aligned: '{flair}' → {issue_type}")
            else:
                print(f"  [ISSUE] flair-override: '{flair}' (expected {expected}) → {issue_type}")
        elif flair:
            print(f"  [ISSUE] unmapped-flair: '{flair}' → {issue_type}")
        else:
            print(f"  [ISSUE] no-flair → {issue_type}")
    
    except Exception as e:
        print(f"  [ISSUE] error: {e}")
        issue_type = "Unknown"
    
    # === PLATFORM: Always use LLM ===
    device = "Unknown"
    try:
        result = platform_chain.invoke({
            "post_text": combined_text,
            "comments_text": comments
        })
        device = result.platform.value if result.platform else "Unknown"
        print(f"  [PLATFORM] llm → {device}")
    except Exception as e:
        print(f"  [PLATFORM] error: {e}")
        device = "Unknown"
    
    return {
        "device": device,
        "issue_type": issue_type,
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
    
    # Initialize LLM for classification (shared model, separate chains)
    classification_llm = ChatOllama(
        model="deepseek-r1:8b",
        temperature=0.0,
        reasoning=True,
        num_predict=1024
    )
    
    # Two separate chains for issue category and platform
    issue_chain = ISSUE_CATEGORY_PROMPT | classification_llm.with_structured_output(IssueCategoryResult)
    platform_chain = PLATFORM_PROMPT | classification_llm.with_structured_output(PlatformResult)

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
        flatten_comment_bodies(comments, comment_bodies, limit=MAX_COMMENTS_PER_POST)
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
        
        # Extract device and issue type categories using two-tier classification
        categories = extract_categories(
            issue_chain,
            platform_chain,
            post.get("title", ""),
            post.get("selftext", ""),
            comments_text,
            post.get("link_flair_text", "")
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
