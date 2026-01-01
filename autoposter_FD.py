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


# --------------- STATE ---------------- #

def load_reposted() -> Set[str]:
    return set(open(REPOSTED_FILE).read().splitlines()) if os.path.exists(REPOSTED_FILE) else set()

def save_reposted(rep:Set[str]):
    with open(REPOSTED_FILE,"w",encoding="utf-8") as f:
        f.write("\n".join(sorted(rep)))

def login()->Client:
    c=Client(); c.login(USERNAME,PASSWORD); return c


# -------------- HELPERS --------------- #

def parse_time(t:Optional[str]):
    if not t:return None
    if t.endswith("Z"):t=t[:-1]+"+00:00"
    try:return datetime.fromisoformat(t)
    except:return None

def get_list(c,uri):
    dids=[];cur=None
    while True:
        try:r=c.app.bsky.graph.get_list(models.AppBskyGraphGetList.Params(list=uri,limit=100,cursor=cur))
        except:break
        for i in r.items:
            s=i.subject;d=s if isinstance(s,str) else getattr(s,"did",None)
            if d:dids.append(d)
        if not r.cursor:break
        cur=r.cursor
    return dids

def media_ok(rec):
    e=getattr(rec,"embed",None)
    if not e:return False
    if hasattr(e,"record"):return False
    return bool(getattr(e,"images",None) or hasattr(e,"video") or hasattr(e,"media"))

def valid(item):
    p=item.post;r=getattr(p,"record",None)
    if not p or not r:return False
    if getattr(r,"reply",None):return False
    if getattr(item,"reason",None) is not None:return False
    return media_ok(r)

def hashtag_ok(item):
    t=getattr(item.post.record,"text","") or ""
    return "#femdom" in t.lower()

def ts(item):
    return parse_time(item.post.indexed_at) or datetime.now(timezone.utc)


# -------------- CLEANUP OLD ------------- #

def cleanup(c,days):
    cutoff=datetime.now(timezone.utc)-timedelta(days=days)
    rem=0;cur=None
    while True:
        r=c.app.bsky.feed.get_author_feed(models.AppBskyFeedGetAuthorFeed.Params(actor=USERNAME,limit=100,cursor=cur))
        for f in r.feed:
            p=f.post;r1=getattr(p,"record",None)
            if not p or not r1:continue
            if getattr(r1,"$type","")!="app.bsky.feed.repost":continue
            dt=parse_time(p.indexed_at)
            if dt and dt<cutoff:
                try:c.delete_repost(p.uri);rem+=1
                except:pass
        if not r.cursor:break
        cur=r.cursor
    return rem


# -------------- REPOST LOGIC ------------- #

def repost(c,item,per,rep):
    p=item.post;uri=p.uri;cid=p.cid;did=p.author.did
    if per.get(did,0)>=MAX_PER_USER_PER_RUN:return False
    if uri in rep:return False

    try:
        c.repost(uri=uri,cid=cid)
        try:c.like(uri=uri,cid=cid)
        except:pass
    except:return False

    rep.add(uri)
    per[did]=per.get(did,0)+1
    time.sleep(DELAY_SECONDS)
    return True


# -------------- MAIN PROCESS ------------- #

def process(c,rep):
    per={};rem=MAX_REPOSTS_PER_RUN
    users=get_list(c,LIST_FEMDOM_URI)
    posts=[]

    for did in users:
        if per.get(did,0)>=MAX_PER_USER_PER_RUN:continue
        try:r=c.app.bsky.feed.get_author_feed(models.AppBskyFeedGetAuthorFeed.Params(actor=did,limit=20,filter="posts_with_media"))
        except:continue

        for i in r.feed:
            if valid(i) and hashtag_ok(i):
                posts.append(i)

    posts.sort(key=ts) # timeline: oud → nieuw

    done=0
    for i in posts:
        if rem<=0:break
        if repost(c,i,per,rep):done+=1;rem-=1

    return done


# -------------- RUN ------------- #

def main():
    rep=load_reposted()
    try:c=login()
    except:print("Login fout");return

    removed=cleanup(c,CLEANUP_DAYS)
    done=process(c,rep)
    save_reposted(rep)

    print(f"✔ Run klaar | Cleanup:{removed} | Femdom:{done}")


if __name__=="__main__":
    main()