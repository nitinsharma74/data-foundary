import requests
import csv
import time
import json
from datetime import datetime, timedelta, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate

# ================= CONFIG =================

SUBREDDIT = "crunchyroll"
POSTS_URL = f"https://www.reddit.com/r/{SUBREDDIT}/new.json"

DAYS_BACK = 1  # Number of days to look back for posts

OUTPUT_FILE = f"reddit_sentiment_analysis/fixtures/reddit_crunchyroll_{DAYS_BACK}days.csv"

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


def add_sentiment_to_comments(comments, analyzer):
    """Recursively add individual VADER sentiment scores to each comment.
    
    Args:
        comments: List of comment dictionaries
        analyzer: SentimentIntensityAnalyzer instance
    
    Modifies comments in-place by adding 'sentiment' field with compound score (-1.0 to 1.0)
    """
    for comment in comments:
        if comment.get("body"):
            score = analyzer.polarity_scores(comment["body"])["compound"]
            comment["sentiment"] = round(score, 4)
        
        if comment.get("replies"):
            add_sentiment_to_comments(comment["replies"], analyzer)


# ================= SENTIMENT =================

def sentiment_score(texts, analyzer):
    """Calculate average sentiment score across multiple texts.
    
    Args:
        texts: List of text strings to analyze
        analyzer: SentimentIntensityAnalyzer instance
    
    Returns:
        Average compound sentiment score across all texts (0.0 if empty)
    """
    if not texts:
        return 0.0

    total = 0.0
    for t in texts:
        total += analyzer.polarity_scores(t)["compound"]

    return total / len(texts)


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
    sia = SentimentIntensityAnalyzer()
    llm = ChatOllama(
        model="llama3.2:1b",
        temperature=0.0,
        reasoning=None,
        num_predict=256
    )
    prompt = PromptTemplate.from_template(
        "Summarize this Reddit post and its top comments in 2-3 sentences. "
        "Capture the main topic and overall reaction, focus on the positive and negative sentiments mentioned in the post and comments.\n\n"
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
        post["sentiment_score"] = round(
            sentiment_score([post.get("post_summary", "")], sia),
            4,
        )

    print("=== Writing CSV ===")

    fields = [
        "id", "title", "selftext", "author", "ups", "downs", "score",
        "num_comments", "created_utc", "upvote_ratio", "link_flair_text",
        "is_self", "is_video", "url", "permalink", "sentiment_score",
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
                "sentiment_score": p.get("sentiment_score"),
                "post_summary": p.get("post_summary", ""),
                "comments": json.dumps(p.get("comments", []))
            })

    print(f"\nDONE -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
