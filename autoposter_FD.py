import os
import time
import random
from typing import List, Set, Dict, Optional

from atproto import Client, models


# --- Config ---------------------------------------------------------

USERNAME = os.getenv("BSKY_USERNAME_FD")
PASSWORD = os.getenv("BSKY_PASSWORD_FD")

REPOSTED_FILE = "reposted_FD.txt"

MAX_REPOSTS_PER_RUN = 100
MAX_PER_USER_PER_RUN = 6
DELAY_SECONDS = 2

# Feed + lists
FEED_URI = (
    "at://did:plc:jaka644beit3x4vmmg6yysw7/"
    "app.bsky.feed.generator/aaagefhd3alla"
)

LIST_VERIFIED_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbacohly3f26"
)

LIST_PROMO_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbaaypwo5r2u"
)

LIST_EXCLUDE_URI = (
    "at://did:plc:o47xqce6eihq6wj75ntjftuw/"
    "app.bsky.graph.list/3mbadkrmbg72j"
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
        # fallback: soms embed op post in plaats van record
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

    # External-only link -> niet
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
    """
    post = feed_item.post
    uri = post.uri
    cid = post.cid

    author = getattr(post, "author", None)
    author_did = getattr(author, "did", "") if author else ""

    # Per-user limit (over gehele run)
    if per_user_counts.get(author_did, 0) >= MAX_PER_USER_PER_RUN:
        return False

    # Feed + verified: geen dubbele reposts
    if not allow_repeat_for_promo and uri in reposted:
        return False

    # Promo: als wijzelf al een repost hebben staan -> un-repost eerst
    if allow_repeat_for_promo:
        viewer = getattr(post, "viewer", None)
        existing_repost = getattr(viewer, "repost", None)
        if existing_repost:
            try:
                if isinstance(existing_repost, str):
                    client.delete_repost(existing_repost)
                else:
                    existing_uri = getattr(existing_repost, "uri", None)
                    if existing_uri:
                        client.delete_repost(existing_uri)
            except Exception:
                # Als dit faalt, gewoon toch alsnog een repost proberen
                pass

    try:
        client.repost(uri=uri, cid=cid)
    except Exception:
        # Minimal logging -> geen trace
        return False

    # Tracking
    reposted.add(uri)
    if author_did:
        per_user_counts[author_did] = per_user_counts.get(author_did, 0) + 1

    time.sleep(DELAY_SECONDS)
    return True


# --- Verwerking van feed + lijsten ---------------------------------


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


def process_verified_list(
    client: Client,
    excluded_dids: Set[str],
    per_user_counts: Dict[str, int],
    reposted: Set[str],
    remaining: int,
) -> int:
    if remaining <= 0:
        return 0

    params = models.AppBskyFeedGetListFeed.Params(
        list=LIST_VERIFIED_URI,
        limit=100,
    )
    resp = client.app.bsky.feed.get_list_feed(params)

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
    done = 0

    # Volgorde: lijstvolgorde, per actor 1 random post uit laatste 5
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

    excluded_dids = get_excluded_dids(client)

    per_user_counts: Dict[str, int] = {}
    remaining = MAX_REPOSTS_PER_RUN
    total_done = 0

    # 1. Feed
    done_feed = process_feed(client, excluded_dids, per_user_counts, reposted, remaining)
    total_done += done_feed
    remaining -= done_feed

    # 2. Verified list
    done_verified = process_verified_list(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total_done += done_verified
    remaining -= done_verified

    # 3. Promo list
    done_promo = process_promo_list(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total_done += done_promo

    save_reposted(reposted)

    # Nog steeds minimale logging, maar nu zie je per blok hoeveel er gedaan is
    print(
        f"Run klaar. Feed: {done_feed}, Verified: {done_verified}, "
        f"Promo: {done_promo}, Totaal: {total_done}"
    )


if __name__ == "__main__":
    main()
