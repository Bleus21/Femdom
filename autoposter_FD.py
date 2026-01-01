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

# Lijst NIET REP0STEN
LIST_EXCLUDE_URI = "at://did:plc:o47xqce6eihq6wj75ntjftuw/app.bsky.graph.list/3mbacohly3f26"

# PROMO lijst (BOVENAAN)
LIST_PROMO_URI   = "at://did:plc:o47xqce6eihq6wj75ntjftuw/app.bsky.graph.list/3mbadkrmbg72j"

# Femdom accounts → alleen repost als post #femdom bevat
LIST_FEMDOM_URI  = "at://did:plc:jaka644beit3x4vmmg6yysw7/app.bsky.graph.list/3m3iga6wnmz2p"


# ============= STATE OPSLAG ============== #

def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_FILE): return set()
    with open(REPOSTED_FILE,"r",encoding="utf-8") as f:
        return {l.strip() for l in f if l.strip()}

def save_reposted(rep:Set[str]):
    try:
        with open(REPOSTED_FILE,"w",encoding="utf-8") as f:
            for u in sorted(rep): f.write(u+"\n")
    except: pass

def client_login()->Client:
    c=Client(); c.login(USERNAME,PASSWORD); return c


# ------------- BASE FUNCTIONS ------------- #

def parse_time(t:str):
    if not t: return None
    if t.endswith("Z"): t=t[:-1]+"+00:00"
    try: return datetime.fromisoformat(t)
    except: return None

def get_list_dids(c,uri)->List[str]:
    dids=[]; cur=None
    while True:
        try:
            r=c.app.bsky.graph.get_list(models.AppBskyGraphGetList.Params(list=uri,limit=100,cursor=cur))
        except: break
        for i in r.items:
            s=getattr(i,"subject",None)
            did=s if isinstance(s,str) else getattr(s,"did",None)
            if did: dids.append(did)
        if not r.cursor: break
        cur=r.cursor
    return dids

def media_ok(rec)->bool:
    e=getattr(rec,"embed",None)
    if not e: return False
    if hasattr(e,"record"): return False
    if getattr(e,"images",None): return True
    if hasattr(e,"video") or hasattr(e,"media"): return True
    return False

def is_valid(item,exclude)->bool:
    p=getattr(item,"post",None); r=getattr(p,"record",None) if p else None
    if not p or not r: return False
    if getattr(p.author,"did",None) in exclude: return False
    if getattr(r,"reply",None): return False
    if getattr(item,"reason",None): return False
    return media_ok(r)

def has_femdom(item)->bool:
    txt=getattr(item.post.record,"text","") or ""
    return "#femdom" in txt.lower()

# ------------ CLEANUP >7 DAGEN ------------ #

def cleanup_old(c:Client,days:int):
    try: promo=set(get_list_dids(c,LIST_PROMO_URI))
    except: promo=set()

    cut=datetime.now(timezone.utc)-timedelta(days=days)
    delcount=0; cur=None

    while True:
        r=c.app.bsky.feed.get_author_feed(models.AppBskyFeedGetAuthorFeed.Params(actor=USERNAME,limit=100,cursor=cur))
        for f in r.feed:
            p=getattr(f,"post",None); rec=getattr(p,"record",None) if p else None
            if not p or not rec or getattr(rec,"$type","")!="app.bsky.feed.repost": continue

            dt=parse_time(p.indexed_at)
            if not dt or dt>cut: continue

            subj=getattr(rec,"subject",None); u=getattr(subj,"uri",None) if subj else None
            orig=None
            if u:
                try:
                    P=c.app.bsky.feed.get_posts(models.AppBskyFeedGetPosts.Params(uris=[u]))
                    for pp in P.posts:
                        if pp.uri==u: orig=getattr(pp.author,"did",None)
                except: pass

            if orig in promo: continue

            try: c.delete_repost(p.uri); delcount+=1
            except: pass

        if not r.cursor: break
        cur=r.cursor

    return delcount


# ------------ REPOST ACTION ------------- #

def do_repost(c,item,per,rep,promo=False):
    p=item.post; uri=p.uri; cid=p.cid
    did=getattr(p.author,"did","")
    if per.get(did,0)>=MAX_PER_USER_PER_RUN: return False
    if not promo and uri in rep: return False

    if promo:
        v=getattr(p,"viewer",None); ex=getattr(v,"repost",None)
        if ex:
            try: c.delete_repost(ex if isinstance(ex,str) else ex.uri)
            except: pass

    try:
        c.repost(uri=uri,cid=cid)
        try: c.like(uri=uri,cid=cid)
        except: pass
    except: return False

    rep.add(uri); per[did]=per.get(did,0)+1
    time.sleep(DELAY_SECONDS)
    return True


# ------------ PROCESS PROMO FIRST ------------ #

def process_promo(c,exclude,per,rep,rem):
    done=0
    mem=get_list_dids(c,LIST_PROMO_URI)
    random.shuffle(mem)
    for did in mem:
        if rem<=0: break
        if did in exclude or per.get(did,0)>=MAX_PER_USER_PER_RUN: continue

        r=c.app.bsky.feed.get_author_feed(models.AppBskyFeedGetAuthorFeed.Params(actor=did,limit=5,filter="posts_with_media"))
        cand=[i for i in r.feed if is_valid(i,exclude)]

        if cand and do_repost(c,random.choice(cand),per,rep,promo=True):
            done+=1; rem-=1

    return done,rem


# --------- PROCESS #FEMDOM ACCOUNTS --------- #

def process_femdom(c,exclude,per,rep,rem):
    done=0
    mem=get_list_dids(c,LIST_FEMDOM_URI)
    for did in mem:
        if rem<=0: break
        if did in exclude or per.get(did,0)>=MAX_PER_USER_PER_RUN: continue

        r=c.app.bsky.feed.get_author_feed(models.AppBskyFeedGetAuthorFeed.Params(actor=did,limit=20,filter="posts_with_media"))
        items=[i for i in r.feed if is_valid(i,exclude) and has_femdom(i)]

        for item in sorted(items,key=lambda x:x.post.indexed_at):
            if rem<=0: break
            if do_repost(c,item,per,rep,promo=False):
                done+=1; rem-=1

    return done,rem


# ================= MAIN ================= #

def main():
    rep=load_reposted()

    try: c=client_login()
    except: print("Login mislukt"); return

    removed=cleanup_old(c,CLEANUP_DAYS)
    exclude=set(get_list_dids(c,LIST_EXCLUDE_URI))

    per={}; rem=MAX_REPOSTS_PER_RUN; total=0

    p,rem=process_promo(c,exclude,per,rep,rem); total+=p
    f,rem=process_femdom(c,exclude,per,rep,rem); total+=f

    save_reposted(rep)

    print(f"Run klaar | Cleanup:{removed} | Promo:{p} | Femdom:{f} | Totaal:{total}")


if __name__=="__main__":
    main()