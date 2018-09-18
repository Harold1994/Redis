import redis
import bisect
import uuid
import time
import math
def add_update_contact(conn, user, contact):
    ac_list = 'recent:' + user
    pipeline = conn.pipeline()
    pipeline.lrem(ac_list, contact)
    pipeline.lpush(ac_list, contact)
    pipeline.ltrim(ac_list, 0, 99)
    pipeline.execute()

def remove_contact(conn, user, contact):
    conn.lrem('recent:'+user, contact)

def fetch_autocomplete_list(conn, user, prefix):
    candidates = conn.lrange('recent:' + user , 0, -1)
    matches = []
    for candidate in candidates:
        if candidate.lower().satrtwith(prefix):
            matches.append(candidate)
        return matches

valid_characters = '`abcdefghijklmnopqrstuvwxyz{'
def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters,prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '{', prefix+'{'

def auto_complete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    start += identifier
    end += identifier
    zset_name = 'members:'+guild

    conn.zadd(zset_name, start, 0, end, 0)
    pipe = conn.pipeline()
    while 1:
        try:
            pipe.watch(zset_name)
            sindex = pipe.zrank(zset_name, start)
            eindex = pipe.zrank(zset_name, end)
            erange = min(sindex + 9, eindex - 2)
            pipe.multi()
            pipe.zrem(zset_name, start, end)
            pipe.zrange(zset_name, sindex, erange)
            items = pipe.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue

    return [item for item in items if '{' not in item]

def join_guild(conn, guild, user):
    conn.zadd('members:'+guild, user, 0)

def leave_guild(conn, guild, user):
    conn.zrem('members:' + guild, user)

def acquire_lock(conn, lockname, acquire_timeout = 10):
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):
            return identifier
        time.sleep(.001)
    return False

def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:%s"%buyerid
    seller = "user:%s"%sellerid
    item = "%s:%s"%(itemid, sellerid)
    inventory = "inventory:%s"%buyerid

    locked = acquire_lock(conn, 'market:')
    if not locked:
        return False
    pipe = conn.pipeline(True)

    try:
        pipe.zscore('market:', item)
        pipe.hget(buyer,'funds')
        price,funds =  pipe.execute()
        if pipe is None or price>funds:
            return None
        pipe.incrby(seller,'funds', int(price))

        pipe.incrby(buyer,'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.rrem('market:', item)
        pipe.execute()
        return True
    finally:
        release_lock(conn, 'market:', locked)

def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'locked:'+identifier
    lockname = 'locked:'+lockname
    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False

def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = "lock:" + lockname
    lock_timeout = int(math.ceil(lock_timeout))
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lock_timeout)

        time.sleep(.001)
    return False

def acquire_semaphore(conn, semname, limit, timeout = 10):
    identifier = str(uuid.uuid4())
    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zadd(semname, identifier, now)
    pipeline.zrank(semname, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier
    conn.zrem(semname, identifier)
    return None

def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)

def acquire_fair_semaphore(conn, semname, limit, timeout = 10):
    identifier = str(uuid.uuid4())
    czset = semname + ":owner"
    ctr = semname + ":counter"

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now-timeout)
    pipeline.zinterstore(czset, {czset:1, semname:0})
    pipeline.incr(ctr)

    counter = pipeline.execute()[-1]

    pipeline.zadd(semname, identifier, now)
    pipeline.zadd(czset, identifier, counter)

    pipeline.zrank(czset, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier

    pipeline.zrem(semname, identifier)
    pipeline.zrem(czset, identifier)
    pipeline.execute()
    return None

def release_fair_semaphore(conn, semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname+":owner", identifier)
    return pipeline.execute()[0]

def refresh_fair_semaphore(conn, semname, identifier):
    if conn.zadd(semname, identifier, time.time()):
        release_fair_semaphore(conn, semname, identifier)
        return False
    return True




