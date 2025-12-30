import os
import time
import random
from typing import List, Set, Dict

from atproto import Client, models


# --- Config ---------------------------------------------------------

USERNAME = os.getenv("BSKY_USERNAME_FD")
PASSWORD = os.getenv("BSKY_PASSWORD_FD")

REPOSTED_FILE = "reposted_FD.txt"

MAX_REPOSTS_PER_RUN = 100
MAX_PER_USER_PER_RUN = 6
DELAY_SECONDS = 2

# Feed + lists omgezet naar AT-URI
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


# --- Helpers: state -------------------------------------------------


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
    # Bewust minimalistisch, geen extra info
    with open(REPOSTED_FILE, "w", encoding="utf-8") as f:
        for uri in sorted(reposted):
            f.write(uri + "\n")


# --- Helpers: Bluesky client ----------------------------------------


def create_client() -> Client:
    if not USERNAME or not PASSWORD:
        raise RuntimeError("BSKY_USERNAME_FD of BSKY_PASSWORD_FD ontbreekt.")
    client = Client()
    client.login(USERNAME, PASSWORD)
    return client


def get_excluded_dids(client: Client) -> Set[str]:
    """
    Haal DIDs op van de 'niet reposten' lijst.
    """
    excluded: Set[str] = set()
    cursor = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=LIST_EXCLUDE_URI,
            limit=100,
            cursor=cursor,
        )
        resp = client.app.bsky.graph.get_list(params)
        for item in resp.items:
            subject = getattr(item, "subject", None)
            did = getattr(subject, "did", None)
            if did:
                excluded.add(did)

        if not resp.cursor:
            break
        cursor = resp.cursor

    return excluded


# --- Helpers: filters -----------------------------------------------


def has_media_embed(record) -> bool:
    """
    Check of een post echte media bevat (geen pure linkpost).
    """
    embed = getattr(record, "embed", None)
    if not embed:
        return False

    # Quote posts hebben een 'record' veld in embed -> skip
    if hasattr(embed, "record"):
        return False

    # Images
    images = getattr(embed, "images", None)
    if images:
        return True

    # Video / media-achtige eigenschappen
    if hasattr(embed, "video") or hasattr(embed, "media"):
        return True

    # Pure external link zonder media -> skip
    return False


def is_allowed_post(feed_item, excluded_dids: Set[str]) -> bool:
    """
    Past alle regels toe:
    - geen text only
    - geen reply
    - geen repost (reason != None)
    - geen quote
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

    # Geen repost-items (feed reason is repost)
    if getattr(feed_item, "reason", None) is not None:
        return False

    # Embed check: geen quotes, geen pure links, wel media
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
    Voert de repost uit + respecteert per-user limit en file-tracking.

    allow_repeat_for_promo:
        - False: sla posts over die al in reposted.txt staan
        - True: gebruikt delete_repost + nieuwe repost als de viewer al gerepost heeft
    """
    post = feed_item.post
    uri = post.uri
    cid = post.cid

    author_did = getattr(post.author, "did", None) or ""
    if per_user_counts.get(author_did, 0) >= MAX_PER_USER_PER_RUN:
        return False

    # Voor feed + verified: niet opnieuw als al bekend
    if not allow_repeat_for_promo and uri in reposted:
        return False

    # Voor promo-lijst: als viewer = wijzelf al gerepost hebben -> un-repost
    if allow_repeat_for_promo:
        viewer = getattr(post, "viewer", None)
        existing_repost_uri = getattr(viewer, "repost", None)
        if existing_repost_uri:
            try:
                client.delete_repost(existing_repost_uri)
            except Exception:
                # Bij fout: gewoon doorgaan en frisse repost proberen
                pass

    try:
        repost_ref = client.repost(uri=uri, cid=cid)
    except Exception:
        # Minimal logging
        return False

    # Tracken
    reposted.add(uri)
    per_user_counts[author_did] = per_user_counts.get(author_did, 0) + 1

    # Delay ivm rate limits / rust tussen posts
    time.sleep(DELAY_SECONDS)
    return True


# --- Stappen: feed + lijsten ----------------------------------------


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

    # Oud naar nieuw sorteren
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

    # Ook hier oud -> nieuw
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
    dids: List[str] = []
    cursor = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=list_uri,
            limit=100,
            cursor=cursor,
        )
        resp = client.app.bsky.graph.get_list(params)

        for item in resp.items:
            subject = getattr(item, "subject", None)
            did = getattr(subject, "did", None)
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
) -> object | None:
    """
    Haal max 5 posts met media van een actor op en kies er 1 random uit
    die door de filters komt.
    """
    params = models.AppBskyFeedGetAuthorFeed.Params(
        actor=actor,
        filter="posts_with_media",
        limit=5,
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

    # Alle accounts in promo-lijst
    members = get_list_members_dids(client, LIST_PROMO_URI)

    # Volgorde van de lijst zelf is prima; per account random post
    done = 0
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
    except Exception as e:
        # Zo min mogelijk logging
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

    # 2. Femdom verified list
    done_verified = process_verified_list(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total_done += done_verified
    remaining -= done_verified

    # 3. Promo list (random uit laatste 5 per account, met un-repost/repost)
    done_promo = process_promo_list(
        client, excluded_dids, per_user_counts, reposted, remaining
    )
    total_done += done_promo
    remaining -= done_promo

    save_reposted(reposted)

    # Minimal log ivm privacy
    print(f"Run klaar. Totaal reposts: {total_done}")


if __name__ == "__main__":
    main()
