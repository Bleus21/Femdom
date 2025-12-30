import os
import time
import random
from typing import List, Set, Dict, Optional
from datetime import datetime, timezone, timedelta

from atproto import Client, models


# --- Config ---------------------------------------------------------

USERNAME = os.getenv("BSKY_USERNAME_FD")
PASSWORD = os.getenv("BSKY_PASSWORD_FD")

REPOSTED_FILE = "reposted_FD.txt"

MAX_REPOSTS_PER_RUN = 100
MAX_PER_USER_PER_RUN = 6
DELAY_SECONDS = 2
CLEANUP_DAYS = 7  # reposts ouder dan 7 dagen verwijderen

# Feed
FEED_URI = (
    "at://did:plc:jaka644beit3x4vmmg6yysw7/"
    "app.bsky.feed.generator/aaagefhd3alla"
)

# Lijst NIET REPOSTEN
LIST_EXCLUDE_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbacohly3f26"
)

# PROMO-lijst
LIST_PROMO_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbadkrmbd72j"
)


# --- State helpers --------------------------------------------------


def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_FILE):
        return set()
    reposted: Set[str] = set()
    with open(REPOSTED_FILE, "r", encoding="utf-8") as f:
        for line in f:
            uri = line.strip()
            if uri:
                reposted.add(uri)
    return reposted


def save_reposted(reposted: Set[str]) -> None:
    # Minimal, geen extra info in file
    try:
        with open(REPOSTED_FILE, "w", encoding="utf-8") as f:
            for uri in sorted(reposted):
                f.write(uri + "\n")
    except Exception:
        # Als wegschrijven faalt, script niet laten crashen
        pass


def create_client() -> Client:
    if not USERNAME or not PASSWORD:
        raise RuntimeError("BSKY_USERNAME_FD of BSKY_PASSWORD_FD ontbreekt.")
    client = Client()
    client.login(USERNAME, PASSWORD)
    return client


# --- Helpers: tijd / cleanup ----------------------------------------


def parse_iso_datetime(value: str) -> Optional[datetime]:
    """
    Veilige ISO8601-parser met support voor 'Z'.
    """
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def cleanup_old_reposts(client: Client, days: int = CLEANUP_DAYS) -> int:
    """
    Verwijder oude reposts ouder dan X dagen,
    MAAR niet wanneer de oorspronkelijke auteur in de promo-lijst zit.
    """
    # Promo-leden ophalen (DIDs)
    promo_members = set(get_list_members_dids(client, LIST_PROMO_URI))

    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    deleted = 0
    cursor: Optional[str] = None

    while True:
        params = models.AppBskyFeedGetAuthorFeed.Params(
            actor=USERNAME,  # handle van deze bot
            limit=100,
            cursor=cursor,
        )
        resp = client.app.bsky.feed.get_author_feed(params)
        if not resp.feed:
            break

        for item in resp.feed:
            post = getattr(item, "post", None)
            if not post:
                continue

            record = getattr(post, "record", None)
            if not record:
                continue

            # Alleen repost records opruimen
            if getattr(record, "$type", "") != "app.bsky.feed.repost":
                continue

            indexed_at = getattr(post, "indexed_at", None)
            dt = parse_iso_datetime(indexed_at) if indexed_at else None
            if not dt or dt > threshold:
                # Niet oud genoeg of geen datum
                continue

            # Originele subject (de post die gerepost is)
            subject = getattr(record, "subject", None)
            subj_uri = getattr(subject, "uri", None) if subject else None

            original_author_did: Optional[str] = None
            if subj_uri:
                try:
                    posts_resp = client.app.bsky.feed.get_posts(
                        models.AppBskyFeedGetPosts.Params(uris=[subj_uri])
                    )
                    for p in posts_resp.posts:
                        if p.uri == subj_uri:
                            auth = getattr(p, "author", None)
                            original_author_did = getattr(auth, "did", None)
                            break
                except Exception:
                    original_author_did = None

            # 👉 Als originele auteur in promo-lijst zit → NIET verwijderen
            if original_author_did in promo_members:
                continue

            # Oude repost verwijderen
            try:
                client.delete_repost(post.uri)
                deleted += 1
            except Exception:
                continue

        if not resp.cursor:
            break
        cursor = resp.cursor

    return deleted


# --- Lijsten & filters ----------------------------------------------


def _extract_did_from_list_item(item) -> Optional[str]:
    """
    Veilige DID-extractie uit list-item (subject kan object of string zijn).
    """
    subject = getattr(item, "subject", None)
    if not subject:
        return None

    if isinstance(subject, str):
        return subject

    return getattr(subject, "did", None)


def get_excluded_dids(client: Client) -> Set[str]:
    """
    Haalt alle DIDs op uit de 'niet reposten'-lijst.
    """
    excluded: Set[str] = set()
    cursor: Optional[str] = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=LIST_EXCLUDE_URI,
            limit=100,
            cursor=cursor,
        )
        resp = client.app.bsky.graph.get_list(params)

        for item in resp.items:
            did = _extract_did_from_list_item(item)
            if did:
                excluded.add(did)

        if not resp.cursor:
            break
        cursor = resp.cursor

    return excluded


def has_media_embed(record) -> bool:
    """
    True als er echte media is (geen pure link of quote).
    """
    embed = getattr(record, "embed", None)
    if not embed:
        # fallback: soms embed op post ipv record
        post = getattr(record, "post", None)
        embed = getattr(post, "embed", None) if post else None
        if not embed:
            return False

    # Quote-posts hebben een eigen record in embed
    if hasattr(embed, "record"):
        return False

    images = getattr(embed, "images", None)
    if images:
        return True

    if hasattr(embed, "video") or hasattr(embed, "media"):
        return True

    # External-only link -> niet tellen
    return False


def is_allowed_post(feed_item, excluded_dids: Set[str]) -> bool:
    """
    Toetst alle voorwaarden:
    - geen text-only
    - geen reply
    - geen repost
    - geen quotepost
    - geen link-only
    - geen accounts uit exclude-list
    """
    post = getattr(feed_item, "post", None)
    if not post:
        return False

    author = getattr(post, "author", None)
    author_did = getattr(author, "did", None)
    if author_did in excluded_dids:
        return False

    record = getattr(post, "record", None)
    if not record:
        return False

    # Geen replies
    if getattr(record, "reply", None):
        return False

    # Geen reposts (timeline/list-feed reason)
    if getattr(feed_item, "reason", None) is not None:
        return False

    # Moet media bevatten en geen quotes/links-only
    if not has_media_embed(record):
        return False

    return True


def repost_post(
    client: Client,
    feed_item,
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    allow_repeat_for_promo: bool = False,
) -> bool:
    """
    Repost + per-user limit + tracking.
    Bij promo mag hij oude reposts opnieuw doen (met un-repost).
    Geeft de post ook meteen een like.
    """
    post = feed_item.post
    uri = post.uri
    cid = post.cid

    author = getattr(post, "author", None)
    author_did = getattr(author, "did", "") if author else ""

    # Per-user limit (over gehele run)
    if per_user_counts.get(author_did, 0) >= MAX_PER_USER_PER_RUN:
        return False

    # Feed: geen dubbele reposts
    if not allow_repeat_for_promo and uri in reposted:
        return False

    # Promo: als wijzelf al een repost hebben staan -> un-repost eerst
    if allow_repeat_for_promo:
        viewer = getattr(post, "viewer", None)
        existing_repost = getattr(viewer, "repost", None)
        if existing_repost:
            try:
                # Afhankelijk van type: string of object met uri
                if isinstance(existing_repost, str):
                    client.delete_repost(existing_repost)
                else:
                    existing_uri = getattr(existing_repost, "uri", None)
                    if existing_uri:
                        client.delete_repost(existing_uri)
            except Exception:
                # Als dit faalt, toch nog een repost proberen
                pass

    try:
        # Repost
        client.repost(uri=uri, cid=cid)
        # Like erbij
        try:
            client.like(uri=uri, cid=cid)
        except Exception:
            # Like moet niet de run laten falen
            pass
    except Exception:
        # Minimal logging -> geen trace
        return False

    # Tracking
    reposted.add(uri)
    if author_did:
        per_user_counts[author_did] = per_user_counts.get(author_did, 0) + 1

    time.sleep(DELAY_SECONDS)
    return True


# --- Verwerking: feed & promo --------------------------------------


def process_feed(
    client: Client,
    excluded_dids: Set[str],
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    remaining: int,
) -> int:
    if remaining <= 0:
        return 0

    params = models.AppBskyFeedGetFeed.Params(
        feed=FEED_URI,
        limit=100,
    )
    resp = client.app.bsky.feed.get_feed(params)

    # Oud -> nieuw
    items = sorted(resp.feed, key=lambda i: i.post.indexed_at)

    done = 0
    for item in items:
        if remaining <= 0:
            break
        if not is_allowed_post(item, excluded_dids):
            continue
        if repost_post(client, item, per_user_counts, reposted, allow_repeat_for_promo=False):
            done += 1
            remaining -= 1

    return done


def get_list_members_dids(client: Client, list_uri: str) -> List[str]:
    """
    Haal alle DIDs uit een list.
    """
    dids: List[str] = []
    cursor: Optional[str] = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=list_uri,
            limit=100,
            cursor=cursor,
        )
        resp = client.app.bsky.graph.get_list(params)

        for item in resp.items:
            did = _extract_did_from_list_item(item)
            if did:
                dids.append(did)

        if not resp.cursor:
            break
        cursor = resp.cursor

    return dids


def pick_random_recent_media_post(
    client: Client,
    actor: str,
    excluded_dids: Set[str],
) -> Optional[object]:
    """
    Pak max 5 recente posts met media van een actor en kies er 1 random uit
    die door de filters komt.
    """
    params = models.AppBskyFeedGetAuthorFeed.Params(
        actor=actor,
        limit=5,
        filter="posts_with_media",
    )
    resp = client.app.bsky.feed.get_author_feed(params)

    candidates = [item for item in resp.feed if is_allowed_post(item, excluded_dids)]
    if not candidates:
        return None

    return random.choice(candidates)


def process_promo_list(
    client: Client,
    excluded_dids: Set[str],
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    remaining: int,
) -> int:
    if remaining <= 0:
        return 0

    members = get_list_members_dids(client, LIST_PROMO_URI)

    # Random volgorde van accounts
    random.shuffle(members)

    done = 0

    # Per actor 1 random post uit laatste 5, in gehusselde volgorde
    for did in members:
        if remaining <= 0:
            break

        if did in excluded_dids:
            continue

        if per_user_counts.get(did, 0) >= MAX_PER_USER_PER_RUN:
            continue

        item = pick_random_recent_media_post(client, did, excluded_dids)
        if not item:
            continue

        if repost_post(client, item, per_user_counts, reposted, allow_repeat_for_promo=True):
            done += 1
            remaining -= 1

    return done


# --- Main -----------------------------------------------------------


def main() -> None:
    reposted = load_reposted()

    try:
        client = create_client()
    except Exception:
        print("Kon niet inloggen op Bluesky.")
        return

    # 0. Opruimen: oude reposts verwijderen (met promo-exceptie)
    removed_old = cleanup_old_reposts(client, CLEANUP_DAYS)

    excluded_dids = get_excluded_dids(client)

    per_user_counts: Dict[str, int] = {}
    remaining = MAX_REPOSTS_PER_RUN
    total_done = 0

    # 1. Feed (oud → nieuw)
    done_feed = process_feed(client, excluded_dids, per_user_counts, reposted, remaining)
    total_done += done_feed
    remaining -= done_feed

    # 2. Promo-lijst (random volgorde, random post per account)
    done_promo = process_promo_list(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total_done += done_promo

    save_reposted(reposted)

    # Minimal logging
    print(
        f"Run klaar. Oude reposts verwijderd (excl. promo): {removed_old}, "
        f"Feed: {done_feed}, Promo: {done_promo}, Totaal nieuwe reposts: {total_done}"
    )


if __name__ == "__main__":
    main()