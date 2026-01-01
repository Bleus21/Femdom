import os
import time
from typing import Set, Dict, Optional
from datetime import datetime, timezone, timedelta
from atproto import Client, models

# ---------------- CONFIG ---------------- #

USERNAME = os.getenv("BSKY_USERNAME_FD")
PASSWORD = os.getenv("BSKY_PASSWORD_FD")

REPOSTED_FILE = "reposted_FD.txt"

LIST_FEMDOM_URI = "at://did:plc:jaka644beit3x4vmmg6yysw7/app.bsky.graph.list/3m3iga6wnmz2p"

MAX_REPOSTS_PER_RUN = 100
MAX_PER_USER_PER_RUN = 6
DELAY_SECONDS = 2
CLEANUP_DAYS = 7
LOOKBACK_HOURS = 12  # 🔥 alleen posts van laatste 12 uur


# --------------- STATE ---------------- #

def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_FILE):
        return set()
    with open(REPOSTED_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_reposted(rep: Set[str]):
    with open(REPOSTED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(rep)))


def login() -> Client:
    c = Client()
    c.login(USERNAME, PASSWORD)
    return c


# -------------- HELPERS --------------- #

def parse_time(t: Optional[str]) -> Optional[datetime]:
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(t)
    except:
        return None


def get_list_members(c: Client, uri: str):
    dids = []
    cursor = None
    while True:
        try:
            r = c.app.bsky.graph.get_list(
                models.AppBskyGraphGetList.Params(list=uri, limit=100, cursor=cursor)
            )
        except:
            break
        for i in r.items:
            s = i.subject
            did = s if isinstance(s, str) else getattr(s, "did", None)
            if did:
                dids.append(did)
        if not r.cursor:
            break
        cursor = r.cursor
    return dids


def media_ok(rec) -> bool:
    e = getattr(rec, "embed", None)
    if not e:
        return False
    # geen quotes
    if hasattr(e, "record"):
        return False
    if getattr(e, "images", None):
        return True
    if hasattr(e, "video") or hasattr(e, "media"):
        return True
    return False


def valid(item) -> bool:
    post = item.post
    rec = getattr(post, "record", None)
    if not post or not rec:
        return False
    if getattr(rec, "reply", None):
        return False
    if getattr(item, "reason", None) is not None:
        return False
    return media_ok(rec)


def hashtag_ok(item) -> bool:
    text = getattr(item.post.record, "text", "") or ""
    return "#femdom" in text.lower()


def get_time(item) -> datetime:
    dt = parse_time(getattr(item.post, "indexed_at", None))
    return dt or datetime.now(timezone.utc)


# -------------- CLEANUP OLD ------------- #

def cleanup(c: Client, days: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    deleted = 0
    cursor = None

    while True:
        r = c.app.bsky.feed.get_author_feed(
            models.AppBskyFeedGetAuthorFeed.Params(actor=USERNAME, limit=100, cursor=cursor)
        )
        if not r.feed:
            break

        for f in r.feed:
            post = f.post
            rec = getattr(post, "record", None)
            if not post or not rec:
                continue
            if getattr(rec, "$type", "") != "app.bsky.feed.repost":
                continue

            dt = parse_time(getattr(post, "indexed_at", None))
            if not dt or dt > cutoff:
                continue

            try:
                c.delete_repost(post.uri)
                deleted += 1
            except:
                pass

        if not r.cursor:
            break
        cursor = r.cursor

    return deleted


# -------------- REPOST LOGIC ------------- #

def repost(c: Client, item, per_user: Dict[str, int], reposted: Set[str]) -> bool:
    post = item.post
    uri = post.uri
    cid = post.cid
    did = post.author.did

    if per_user.get(did, 0) >= MAX_PER_USER_PER_RUN:
        return False
    if uri in reposted:
        return False

    try:
        c.repost(uri=uri, cid=cid)
        try:
            c.like(uri=uri, cid=cid)
        except:
            pass
    except:
        return False

    reposted.add(uri)
    per_user[did] = per_user.get(did, 0) + 1
    time.sleep(DELAY_SECONDS)
    return True


# -------------- PROCESS FEMDOM ------------- #

def process_femdom(c: Client, reposted: Set[str]) -> int:
    per_user: Dict[str, int] = {}
    remaining = MAX_REPOSTS_PER_RUN
    members = get_list_members(c, LIST_FEMDOM_URI)
    posts = []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    for did in members:
        if per_user.get(did, 0) >= MAX_PER_USER_PER_RUN:
            continue

        try:
            r = c.app.bsky.feed.get_author_feed(
                models.AppBskyFeedGetAuthorFeed.Params(
                    actor=did,
                    limit=20,
                    filter="posts_with_media",
                )
            )
        except:
            continue

        for item in r.feed:
            if not valid(item):
                continue

            dt = get_time(item)
            # 🔥 alleen posts uit de laatste 12 uur
            if dt < cutoff:
                continue

            if not hashtag_ok(item):
                continue

            posts.append(item)

    # tijdlijn volgorde: oud -> nieuw
    posts.sort(key=get_time)

    done = 0
    for item in posts:
        if remaining <= 0:
            break
        if repost(c, item, per_user, reposted):
            done += 1
            remaining -= 1

    return done


# -------------- MAIN ------------- #

def main():
    reposted = load_reposted()

    try:
        c = login()
    except:
        print("Login fout")
        return

    removed = cleanup(c, CLEANUP_DAYS)
    done = process_femdom(c, reposted)
    save_reposted(reposted)

    print(f"✔ Run klaar | Cleanup(>7d): {removed} | Femdom(last {LOOKBACK_HOURS}h): {done}")


if __name__ == "__main__":
    main()