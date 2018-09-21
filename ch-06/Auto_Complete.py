import os
from collections import deque,defaultdict

import redis
import bisect
import uuid
import time
import math
import json

QUIT = False
pipe = inv = item = buyer = seller = inventory = None

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

def acquire_semaphore_with_lock(conn, semname, limit, timeout = 100):
    identifier = acquire_lock(conn, semname, acquire_timeout=.01)
    if identifier :
        try:
            return acquire_fair_semaphore(conn,semname, limit, timeout)
        finally:
            release_fair_semaphore(conn, semname, identifier)


def send_sold_email_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id':seller,
        'item_id':item,
        'price':price,
        'buyer_id':buyer,
        'time':time.time()
    }
    conn.rpush('queue:email', json.dumps(data))

def process_sold_email_queue(conn):
    while not QUIT:
        packed = conn.blpop(['queue:email'], 30)
        if not packed:
            continue
        print(json.loads(packed[0]))
        print('-----------------------')
        to_send = json.loads(packed[1])
        print(to_send)
        try:
            fetch_data_and_send_sold_eamil(to_send):
        except EmailSendError as err:
            log_err("Failed to send sold email ", err, to_send)
        else:
            log_success('Send sold email ', to_send)

def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_err("Unknown callback %s"%name)
            continue
        callbacks[name](*args)


def worker_watch_queues(conn, queues, callbacks):
    while not QUIT:
        packed = conn.blpop(queues, 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_err("Unknown callback %s" % name)
            continue
        callbacks[name](*args)

def execute_later(conn, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd('delayed:', item, time.time()+delay)
    else:
        conn.rpush('queue:' + queue, item)
    return identifier

def pull_queue(conn):
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0, withscores=True)
        if not item or item[0][1] > time.time():
            time.sleep(.01)
            continue
        item = item[0]
        identifier, queue, function, args = json.loads(item)

        locked = acquire_lock(conn, identifier)
        if not locked:
            continue
        if conn.zrem('delayed:', item):
            conn.rpush('queue:' + queue, item)
        release_lock(conn, identifier, locked)


def create_chat(conn, sender, recipients, message, chat_id = None):
    chat_id = chat_id or str(conn.incr('ids:chat:'))
    recipients.append(sender)
    recipientsd = dict((r,0) for r in recipients)

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:'+chat_id, **recipientsd)
    for rec in recipients:
        pipeline.zadd('seen:'+rec, chat_id, 0)
    pipeline.execute()
    return send_message(conn, chat_id, sender, message)

def send_message(conn, chat_id, sender, message):
    identifier = acquire_lock(conn,'chat:'+chat_id)
    if not identifier:
        raise  Exception("could not get lock")
    try:
        mid = conn.incr('ids:'+chat_id)
        ts = time.time()
        packed = json.dumps({
            'id':mid,
            'ts':ts,
            'sender':sender,
            'message':message,
        })
        conn.zadd('msgs:'+chat_id, packed, mid)
    finally:
        release_lock(conn, 'chat:'+chat_id, identifier)
    return chat_id

def fetch_pending_messages(conn, recipient):
    seen = conn.zrange('seen:'+recipient, 0, -1, withscores=True)
    pipeline = conn.pipeline(True)
    for chat_id, seen_id in seen:
        pipeline.zrangebyscore('msga:'+chat_id, seen_id+1, 'inf')
    chat_info = zip(seen, pipeline.execute())

    for i,((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue

        messages[:] = map(json.loads, messages)
        seen_id = messages[-1]['id']
        conn.zadd('chat:' + chat_id, recipient, seen_id)

        min_id = conn.zrange('chat:'+chat_id, 0, 0, withscores=True)
        pipeline.zadd('seen:'+recipient, chat_id,seen_id)

        if min_id:
            pipeline.zremrangebyscore('msgs:' + chat_id, 0, min_id[0][1])
        chat_info[i] = (chat_id, messages)
        pipeline.execute()
    return chat_info

def join_chat(conn, chat_id, user):
    message_id = int(conn.get('ids:' + chat_id))
    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:'+chat_id, user, message_id)
    pipeline.zadd('seen:'+user, chat_id, message_id)
    pipeline.execute()

def leave_chat(conn, chat_id, user):
    pipeline = conn.pipeline(True)
    pipeline.zrem('chat:'+chat_id, user)
    pipeline.zrem('seen:'+user, chat_id)
    pipeline.zcard('chat:'+chat_id)

    if not pipeline.execute()[-1]:
        pipeline.delete('msgs:' + chat_id)
        pipeline.delete('ids:' + chat_id)
        pipeline.execute
    else:
        oldest = conn.zrange('chat:'+chat_id, 0,0,withscores=True)
        conn.zrembyrange('msgs:'+chat_id, 0, oldest[0][1])

aggregates = defaultdict(lambda : defaultdict(int))
def daily_country_aggregate(conn, line):
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = find_city_by_ip_local(ip)[2]
        aggregates[day][country] += 1
        return
    for day, aggregate in aggregates.items():
        conn.zadd('daily:country:' + day, **aggregate)
        del aggregates[day]

def copy_logs_to_redis(conn, path, channel, count=10, limit = 2**30, quit_when_down=True):
    bytes_in_redis = 0
    waiting = deque()
    create_chat(conn,'source',map(str, range(count)), '', channel)
    count = str(count)
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)

        fsize = os.stat(full_path).st_size
        while bytes_in_redis + fsize > limit:
            cleaned = _clean(conn, channel, waiting, count)
            if cleaned:
                bytes_in_redis -= cleaned
            else:
                time.sleep(.25)

        with open(full_path,'rb') as inp:
            block = ' '
            while block:
                block = inp.read(2**17)
                conn.append(channel+logfile, block)
        send_message(conn, channel, 'source',logfile)
        bytes_in_redis += fsize
        waiting.append((logfile, fsize))
        if quit_when_down:
            send_message(conn, channel, 'source',":done")

        while waiting:
            cleaned = _clean(conn, channel, waiting, count)  # G
            if cleaned:  # G
                bytes_in_redis -= cleaned  # G
            else:  # G
                time.sleep(.25)

def _clean(conn, channel, waiting, count):
    if not waiting:  # H
        return 0  # H
    w0 = waiting[0][0]  # H
    if conn.get(channel + w0 + ':done') == count:  # H
        conn.delete(channel + w0, channel + w0 + ':done')  # H
        return waiting.popleft()[1]  # H
    return 0















