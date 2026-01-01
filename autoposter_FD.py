import os
import time
import random
from typing import List, Set, Dict, Optional
from datetime import datetime, timezone, timedelta
from atproto import Client, models

# ================= CONFIG ================= #

USERNAME = os.getenv("BSKY_USERNAME_FD")
PASSWORD = os.getenv("BSKY_PASSWORD_FD")

REPOSTED_FILE = "reposted_FD.txt"

MAX_REPOSTS_PER_RUN = 100
MAX_PER_USER_PER_RUN = 6
DELAY_SECONDS = 2
CLEANUP_DAYS = 7

# Lijst NIET REPOSTEN
LIST_EXCLUDE_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbacohly3f26"
)

# PROMO lijst (BOVENAAN)
LIST_PROMO_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbadkrmbg72j"
)

# Femdom accounts → alleen repost als post #femdom bevat
LIST_FEMDOM_URI = (
    "at://did:plc:jaka644beit3x4vmmg6yysw7/"
    "app.bsky.graph.list/3m3iga6wnmz2p"
)


# ============= STATE OPSLAG ============== #

def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_FILE):
        return set()
    with open(REPOSTED_FILE, "r", encoding="utf-8") as f:
        return {l.strip() for l in f if l.strip()}


def save_reposted(rep: Set[str]) -> None:
    try:
        with open(REPOSTED_FILE, "w", encoding="utf-8") as f:
            for u in sorted(rep):
                f.write(u + "\n")
    except Exception:
        pass


def client_login() -> Client:
    client = Client()
    client.login(USERNAME, PASSWORD)
    return client


# ------------- BASE FUNCTIONS ------------- #

def parse_time(t: Optional[str]) -> Optional[datetime]:
    if not t:
        return None
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        return datetime.fromisoformat(t)
    except Exception:
        return None


def get_list_dids(client: Client, uri: str) -> List[str]:
    dids: List[str] = []
    cursor: Optional[str] = None

    while True:
        try:
            resp = client.app.bsky.graph.get_list(
                models.AppBskyGraphGetList.Params(list=uri, limit=100, cursor=cursor)
            )
        except Exception:
            break

        for item in resp.items:
            subject = getattr(item, "subject", None)
            if isinstance(subject, str):
                did = subject
            else:
                did = getattr(subject, "did", None)
            if did:
                dids.append(did)

        if not resp.cursor:
            break
        cursor = resp.cursor

    return dids


def media_ok(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    # Quoteposts hebben embed.record
    if hasattr(embed, "record"):
        return False
    if getattr(embed, "images", None):
        return True
    if hasattr(embed, "video") or hasattr(embed, "media"):
        return True
    return False


def is_valid(item, excluded_dids: Set[str]) -> bool:
    post = getattr(item, "post", None)
    record = getattr(post, "record", None) if post else None
    if not post or not record:
        return False

    author = getattr(post, "author", None)
    if getattr(author, "did", None) in excluded_dids:
        return False

    # Geen replies
    if getattr(record, "reply", None):
        return False

    # Geen reposts (reason != None)
    if getattr(item, "reason", None) is not None:
        return False

    # Moet media hebben, geen pure link/quote
    if not media_ok(record):
        return False

    return True


def has_femdom(item) -> bool:
    record = getattr(item.post, "record", None)
    txt = getattr(record, "text", "") or ""
    return "#femdom" in txt.lower()


def get_post_time_from_item(item) -> datetime:
    post = item.post
    dt = parse_time(getattr(post, "indexed_at", None))
    # fallback nu - maar normaal heeft alles indexed_at
    return dt or datetime.now(timezone.utc)


# ------------ CLEANUP >7 DAGEN ------------ #

def cleanup_old(client: Client, days: int) -> int:
    try:
        promo_dids = set(get_list_dids(client, LIST_PROMO_URI))
    except Exception:
        promo_dids = set()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    deleted = 0
    cursor: Optional[str] = None

    while True:
        resp = client.app.bsky.feed.get_author_feed(
            models.AppBskyFeedGetAuthorFeed.Params(
                actor=USERNAME, limit=100, cursor=cursor
            )
        )

        if not resp.feed:
            break

        for item in resp.feed:
            post = getattr(item, "post", None)
            record = getattr(post, "record", None) if post else None
            if not post or not record:
                continue

            if getattr(record, "$type", "") != "app.bsky.feed.repost":
                continue

            dt = parse_time(getattr(post, "indexed_at", None))
            if not dt or dt > cutoff:
                continue

            # originele subject
            subject = getattr(record, "subject", None)
            subj_uri = getattr(subject, "uri", None) if subject else None
            original_did: Optional[str] = None

            if subj_uri:
                try:
                    pr = client.app.bsky.feed.get_posts(
                        models.AppBskyFeedGetPosts.Params(uris=[subj_uri])
                    )
                    for p in pr.posts:
                        if p.uri == subj_uri:
                            original_did = getattr(getattr(p, "author", None), "did", None)
                            break
                except Exception:
                    original_did = None

            # promo creators NIET verwijderen
            if original_did in promo_dids:
                continue

            try:
                client.delete_repost(post.uri)
                deleted += 1
            except Exception:
                pass

        if not resp.cursor:
            break
        cursor = resp.cursor

    return deleted


# ------------ REPOST ACTION ------------- #

def do_repost(
    client: Client,
    item,
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    promo: bool = False,
) -> bool:
    post = item.post
    uri = post.uri
    cid = post.cid
    author = getattr(post, "author", None)
    did = getattr(author, "did", "") if author else ""

    # per-user limiet
    if per_user_counts.get(did, 0) >= MAX_PER_USER_PER_RUN:
        return False

    # dubbele reposts vermijden (voor niet-promo)
    if not promo and uri in reposted:
        return False

    # Promo: un-repost als we zelf al gerepost hebben
    if promo:
        viewer = getattr(post, "viewer", None)
        existing = getattr(viewer, "repost", None)
        if existing:
            try:
                if isinstance(existing, str):
                    client.delete_repost(existing)
                else:
                    client.delete_repost(getattr(existing, "uri", None))
            except Exception:
                pass

    try:
        client.repost(uri=uri, cid=cid)
        try:
            client.like(uri=uri, cid=cid)
        except Exception:
            pass
    except Exception:
        return False

    reposted.add(uri)
    if did:
        per_user_counts[did] = per_user_counts.get(did, 0) + 1

    time.sleep(DELAY_SECONDS)
    return True


# ------------ PROCESS PROMO (TIJDLIJN) ------------ #

def process_promo(
    client: Client,
    excluded_dids: Set[str],
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    remaining: int,
) -> (int, int):
    if remaining <= 0:
        return 0, remaining

    members = get_list_dids(client, LIST_PROMO_URI)
    all_items = []

    # posts van alle promo-accounts ophalen
    for did in members:
        if per_user_counts.get(did, 0) >= MAX_PER_USER_PER_RUN:
            continue
        if did in excluded_dids:
            continue

        try:
            resp = client.app.bsky.feed.get_author_feed(
                models.AppBskyFeedGetAuthorFeed.Params(
                    actor=did,
                    limit=10,
                    filter="posts_with_media",
                )
            )
        except Exception:
            continue

        for item in resp.feed:
            if is_valid(item, excluded_dids):
                all_items.append(item)

    # alles in TIJDLIJN volgorde: oud -> nieuw
    all_items.sort(key=get_post_time_from_item)

    done = 0
    for item in all_items:
        if remaining <= 0:
            break
        if do_repost(client, item, per_user_counts, reposted, promo=True):
            done += 1
            remaining -= 1

    return done, remaining


# --------- PROCESS #FEMDOM ACCOUNTS (TIJDLIJN) --------- #

def process_femdom(
    client: Client,
    excluded_dids: Set[str],
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    remaining: int,
) -> (int, int):
    if remaining <= 0:
        return 0, remaining

    members = get_list_dids(client, LIST_FEMDOM_URI)
    all_items = []

    for did in members:
        if per_user_counts.get(did, 0) >= MAX_PER_USER_PER_RUN:
            continue
        if did in excluded_dids:
            continue

        try:
            resp = client.app.bsky.feed.get_author_feed(
                models.AppBskyFeedGetAuthorFeed.Params(
                    actor=did,
                    limit=20,
                    filter="posts_with_media",
                )
            )
        except Exception:
            continue

        for item in resp.feed:
            if not is_valid(item, excluded_dids):
                continue
            if not has_femdom(item):
                continue
            all_items.append(item)

    # Volgorde: echte TIJDLIJN (oud -> nieuw)
    all_items.sort(key=get_post_time_from_item)

    done = 0
    for item in all_items:
        if remaining <= 0:
            break
        if do_repost(client, item, per_user_counts, reposted, promo=False):
            done += 1
            remaining -= 1

    return done, remaining


# ================= MAIN ================= #

def main() -> None:
    reposted = load_reposted()

    try:
        client = client_login()
    except Exception:
        print("Login mislukt")
        return

    removed = cleanup_old(client, CLEANUP_DAYS)
    excluded_dids = set(get_list_dids(client, LIST_EXCLUDE_URI))

    per_user_counts: Dict[str, int] = {}
    remaining = MAX_REPOSTS_PER_RUN
    total = 0

    # 1. PROMO (bovenaan, maar nu in echte tijdlijn-volgorde binnen promo)
    promo_count, remaining = process_promo(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total += promo_count

    # 2. #femdom uit LIST_FEMDOM_URI, ook in globale tijdlijn-volgorde
    femdom_count, remaining = process_femdom(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total += femdom_count

    save_reposted(reposted)

    print(
        f"Run klaar | Cleanup(excl promo):{removed} | "
        f"Promo(tijdlijn):{promo_count} | Femdom(tijdlijn):{femdom_count} | "
        f"Totaal:{total}"
    )


if __name__ == "__main__":
    main()