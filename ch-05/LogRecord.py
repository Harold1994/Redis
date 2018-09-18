import redis
import logging
import time
import bisect
import uuid
import unittest
import contextlib
import json
import csv
import random
from datetime import datetime
import threading
QUIT = False
SAMPLE_COUNT = 100

LAST_CHECKED = None
IS_UNDER_MAINTENCE = False

config_connection = None

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical',
}


SEVERITY.update((name, name) for name in list(SEVERITY.values()))


def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'recent:%s:%s'%(name,severity)
    message = time.asctime()+ ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination,message)
    pipe.ltrim(destination,0,99)
    pipe.execute()

#根据log出现的频率决定消息的排列顺序
def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "common:%s:%s"%(name, severity)
    start_key = destination + ':start'
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()
            existing = pipe.get(start_key)
            pipe.multi()
            if existing and str(existing) < hour_start:
                pipe.rename(destination, destination+':last')
                pipe.rename(start_key, destination+":pstart")
                pipe.set(start_key, hour_start)
            elif not existing:
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, message)
            log_recent(pipe, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue

PRECISION = [1, 5, 560, 300, 3600]

def update_counter(conn, name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now/prec) * prec
        hash = "%s:%s"%(prec,name)
        pipe.zadd('known:', hash, 0)
        pipe.hincrby('count:'+hash, pnow, count)
    pipe.execute()

def get_counter(conn, name, precision):
    hash = "%s:%s"%(precision,name)
    data = conn.hgetall('count:'+ hash)
    to_return = []
    for key,value in data.items():
        to_return.append((int(key), int(value)))
    to_return.sort()
    return to_return

def clean_counters(conn):
    pipe = conn.pipeline()
    passes = 0
    while not QUIT:
        start = time.time()
        index = 0
        while index < conn.zcard('known:'):
            hash = conn.zrange('known', index, index)
            index += 1
            if not hash:
                break
            hash = hash[0]
            prec = int(hash.partition(':')[0])
            bprec = int(prec // 60) or 1
            if passes % bprec:
                continue
            hkey = 'count:' + hash
            cutoff = time.time() - SAMPLE_COUNT*prec
            samples = map(int, conn.hkeys(hkey))
            samples.sort()
            remove = bisect().bisect_right(samples, cutoff)
            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', hash)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.Redis.exceptions.WatchError:
                        pass
            passes += 1
            duration = min(int(time.time() - start) + 1, 60)
            time.sleep(max(60 - duration,1))

def update_stats(conn, context, type, value, timeout=5):
    destination = 'stats:%s:%s'%(context, type)
    start_key = destination + ":start"
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    while time.time()<end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat

            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing<hour_start:
                pipe.rename(destination, destination+":last")
                pipe.rename(start_key, destination + ":pstart")
                pipe.set(start_key, hour_start)
            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            pipe.zadd(tkey1, 'min', value)
            pipe.zadd(tkey2, 'max', value)
            pipe.zunionstore(destination, [destination, tkey1], aggregate='min')
            pipe.zunionstore(destination, [destination, tkey2], aggregate='max')

            pipe.delete(tkey1, tkey2)
            pipe.zincrby(destination, 'count')
            pipe.zincrby(destination, 'sum', value)
            pipe.zincrby(destination, 'sumsq', value*value)
            return pipe.execute()[-3:]
        except redis.exceptions.WatchError:
            continue

def get_stats(conn, context, type):
    key = 'stats:%s:%s'%(context, type)
    data = dict(conn.zrange(key, 0, -1, withscores=True))
    data['average'] = data.get(b'sum') / data.get(b'count')                    #C
    numerator = data[b'sumsq'] - data[b'sum'] ** 2 / data[b'count']        #D
    data['stddev'] = (numerator / (data[b'count'] - 1 or 1)) ** .5       #E
    return data

@contextlib.contextmanager
def access_time(conn, context):
    start = time.time()
    yield
    delta = time.time() - start
    stats = update_stats(conn, context, "AccessTime", delta)
    average = stats[1]/stats[0]
    pipe = conn.pipeline(True)
    pipe.zadd('slowest:AccessTime', context, average)
    pipe.zremrangebyrank('slowest:AccessTime',0,-101)
    pipe.execute()

def process_view(conn, callback):
    with access_time(conn, request.path):
        return callback()

def ip_to_score(ip_address):
    score = 0
    for v in ip_address.split('.'):
        score = score*256 + int(v,10)
    return score

def import_ips_to_redis(conn, filename):
    csv_file = csv.reader(open(filename, 'rb'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ''
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip)
        else:
            continue
        city_id = row[2] + '_' + str(count)
        conn.zadd('ip2cityid:', city_id, start_ip)

def import_cities_to_redis(conn, filename):
    for row in csv.reader(open(filename,'rb')):
        if len(row)<4 or not row[0].isdigit():
            continue
        row = [i.decode('latin-1') for i in row]
        city_id = row[0]
        country = row[1]
        region = row[2]
        city = row[3]
        conn.hset('cityid2city', city_id, json.dumps([city, region, country]))

def find_city_by_ip(conn, ip_address):
    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)
    city_id = conn.zrevrangebyscore('ip2score', ip_address, 0, start=0, num=1)
    if not city_id:
        return None
    city_id = city_id[0].partitionm('_')[0]
    return json.loads(conn.hget('cityid2city:', city_id))

def is_under_maintenance(conn):
    global LAST_CHECKED, IS_UNDER_MAINTENCE
    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENCE = bool(conn.get('is-under-maintenance'))
    return IS_UNDER_MAINTENCE

def set_config(conn, type, component, config):
    conn.set('config:%s:%s'%(type, component), json.dumps(config))


CONFIGS = {}
CHECKED = {}
def get_config(conn, type, component, wait = 1):
    key = 'config:%s:%s'%(type, component)
    if CHECKED.get(key) < time.time() - wait:
        config = json.loads(conn.get(key) or {})
        config = dict((str(k), config[k]) for k in config)
        old_config = CONFIGS.get(key)
        if config != old_config:
            CONFIGS[key] = config
    return CONFIGS.get(key)
REDIS_CONNECTIONS = {}

import functools
def redis_connection(component, wait=1):
    key = 'config:redis:' + component
    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            _config = get_config(config_connection, 'redis', component, wait)
            config = {}
            for k, v in _config.items():
                config[k.encode('utf-8')] = v
            if config != old_config:
                REDIS_CONNECTIONS[key] = redis.Redis(**config)
            return function(REDIS_CONNECTIONS[key], *args, **kwargs)
        return call
    return wrapper

class test:
    pass

class request:
    pass

class TestCh05(unittest.TestCase):
    def setUp(self):
        global config_connection
        import redis

        self.conn = config_connection = redis.Redis(db=15)
        self.conn.flushdb()

    def tearDowm(self):
        self.conn.flushdb()
        del self.conn
        global config_connection, QUIT, SAMPLE_COUNT
        config_connection = None
        QUIT=False
        SAMPLE_COUNT = 100
        print()
        print()
    def test_config(self):
        print("Let's set a config and then get a connection from that config...")
        set_config(self.conn, 'redis', 'test', {'db':15})
        @redis_connection('test')
        def test(conn2):
            return bool(conn2.info())
        print( "We can run commands from the configured connection:", test())

'''
    def test_log_recent(self):
        import pprint
        conn = self.conn
        print("write a log for recent log")
        for msg in range(5):
            log_recent(conn, 'test', 'this is msg %s'%msg)
        recent = conn.lrange('recent:test:info', 0, -1)
        print('the current recent msg include:',len(recent))
        print('these mesg r:')
        pprint.pprint(recent[:10])
        self.assertTrue(len(recent)>=5)

    def test_log_common(self):
        import pprint
        conn = self.conn
        print("write on common logs")
        for count in range(1,6):
            for i in range(count):
                log_common(conn, 'test', 'message-%s'%count)
        common = conn.zrevrange('common:test:info', 0, -1, withscores=True)

        print("the number of msg is:", len(common))
        print("these common meaasges are:")
        pprint.pprint(common)
        self.assertTrue(len(common) >= 5)

    def test_counters(self):
        import pprint
        global QUIT, SAMPLE_COUNT
        conn = self.conn
        print("test counters")
        now = time.time()
        for delta in range(10):
            update_counter(conn, 'test', count=random.randrange(1,5), now = now + delta)
        counter = get_counter(conn, 'test', 1)
        print('per_second counters',len(counter))
        print('these counter s include:')
        pprint.pprint(counter[:10])
        self.assertTrue(len(counter)>=2)
        print()
        tt = time.time()
        def new_tt():
            return tt + 2*86400
        time.time = new_tt
        print('clean_counters')
        SAMPLE_COUNT = 0
        t = threading.Thread(target=clean_counters, args=(conn,))
        t.setDaemon(1)
        t.start()
        time.sleep(1)
        QUIT = True
        time.time = tt
        counter = get_counter(conn, 'test', 86400)
        print("clean all counters : ?", not counter)
        self.assertFalse(bool(counter))

    def test_stats(self):
        import pprint
        conn = self.conn
        print("add data to statistics")
        for i in range(5):
            r = update_stats(conn, 'temp', 'example', random.randrange(5,15))
        print('have aggregate statistics:', r)
        rr = get_stats(conn, 'temp', 'example')
        print('can fetch manually:')
        pprint.pprint(rr)
        self.assertTrue(rr[b'count']>=5)

    def test_access_time(self):
        import pprint
        conn = self.conn
        print('calcutlate access time')
        for i in range(10):
            with access_time(conn, 'req-%s'%i):
                time.sleep(.5 + random.random())
        print('the slowest access time are:')
        actimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores=True)
        pprint.pprint(actimes[:10])
        self.assertTrue(len(actimes)>=10)
        print()

        def cb():
            time.sleep(1 + random.random())
        print("use the callback version:")
        for i in range(5):
            request.path = 'cbreq-%s'%i
            process_view(conn, cb)
        print('the slowest access time are:')
        atimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores = True)
        pprint.pprint(atimes[:10])
        self.assertTrue(len(actimes) >= 10)'''



if __name__ == '__main__':
    unittest.main()
