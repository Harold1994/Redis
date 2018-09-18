import json
import threading
import time
import unittest
from urllib import parse
import uuid

def check_token(conn, token):
    return conn.hget('login:', token)

def update_token ( conn, token, user, item=None ):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:'+token, item, timestamp)
        conn.zremrangebyrank('viewed:'+token, 0, -26)
        conn.zincrby('viewed:', item, -1)#记录所有商品的浏览次数， 根据浏览次数对商品排序，-1的原因是zset递增排序
QUIT = False
LIMIT = 10000000

def clean_sessions(conn):
    while not QUIT:
        size = conn.zcard("recent:")
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index-1)

        session_keys = []
        for token in tokens:
            print(token)
            session_keys.append('viewed:' + str(token))

        conn.delete(*session_keys)
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)


def add_to_cart(conn, session, item, count):
    if count <= 0:
        conn.hrem('cart:' + session, item)
    else:
        conn.hset('cart:' + session, item, count)


def clean_full_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size-LIMIT, 100)
        sessions = conn.zrange('recent:',0, end_index)

        session_keys = []
        for sess in sessions:
            session_keys.append('viewed:'+sess)
            session_keys.append('cart:' + sess)

        conn.delete(*session_keys)
        conn.hdel('login:',*sessions)
        conn.zrem('recent:',*sessions)

def cache_request(conn, request, callback):
    if not can_cache(conn,request):
        return callback(request)
    page_key = 'cache:'+hash_request(request)
    content = conn.get(page_key)
    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)
    return content


def schedule_row_cache(conn, row_id, delay):
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:', row_id, time.time())

def cache_rows(conn):
    while not QUIT:
        next = conn.zrange('schedule:',0,0,withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(0.05)
            continue
        row_id = next[0][0]
        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:'+row_id)
            continue
        row = Inventory.get(row_id)
        conn.zadd('schedule:', row_id, now+delay)
        conn.set('inv:'+str(row_id), repr(row.to_dict()))


class Inventory(object):
    def __init__(self, id):
        self.id = id

    @classmethod
    def get(cls, id):
        return Inventory(id)

    def to_dict(self):
        return {'id':self.id, 'data':'data to cache...', 'cached':time.time()}

def rescale_viewed(conn):
    while not QUIT:
        conn.zremrangebyrank('viewed:', 0 , -20001)
        conn.zinterstore('viewed:', {'viewed:':.5})
        time.sleep(300)

def can_cache(conn, request):
    item_id = extract_item_id(request)
    if not item_id or is_dynamic(request):
        return False
    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000

def extract_item_id(request):
    parsed = parse.urlparse(request)
    query = parse.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]

def is_dynamic(request):
    parsed = parse.urlparse(request)
    query = parse.parse_qs(parsed.query)
    return '_' in query

def hash_request(request):
    return str(hash(request))


class TestCh02(unittest.TestCase):
    #setUp()和tearDown(),这两个方法会分别在每调用一个测试方法的前后分别被执行。
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        conn = self.conn
        to_del = (
            conn.keys('login*') + conn.keys('recent:*') + conn.keys('viewed:*') +
            conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
            conn.keys('schedule:*') + conn.keys('inv:*'))
        if to_del:
            self.conn.delete(*to_del)
        del self.conn
        global QUIT, LIMIT
        QUIT = False
        LIMIT = 10000000
        print()
        print()

    '''def test_login_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        update_token(conn, token, 'harold', 'itemX')
        print("we just logged-in/update token:", token)
        print("For user:", 'harold')
        print()

        print("What username do we get when we look-up that token?")
        r = check_token(conn, token)
        print(r)
        print()
        self.assertTrue(r)
        print("Let's drop the maximum number of cookies to 0 to clean them out")
        print("We will start a thread to do the cleaning, while we stop it later")

        LIMIT = 0
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1)#确保主线程死了子线程也会死
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The clean sessions thread is still alive?!?")

        s = conn.hlen('login:')
        print("the current number of sessions still available is:", s)
        self.assertFalse(s)

    def test_cache_request(self):
        conn = self.conn
        token = str(uuid.uuid4())
        def callback(request):
            return "content for " + request
        update_token(conn, token, 'harold', 'itemX')
        url = 'http://test.com/?item=itemX'
        print("chche a simple request against", url)
        result = cache_request(conn, url, callback)
        print("init content:", repr(result))#repr() 函数将对象转化为供解释器读取的形式
        print()

        self.assertTrue(result)

        print("To test that we've cached the request, we'll pass a bad callback")
        result2 = cache_request(conn, url, None)
        print("We ended up getting the same response!", repr(result2))
        self.assertEquals(result, result2)

        self.assertFalse(can_cache(conn, 'http://test.com/'))
        self.assertFalse(can_cache(conn, 'http://test.com/?item=itemX&_=1234536'))
    '''
    def test_cache_rows(self):
        import pprint
        conn = self.conn
        global QUIT

        print("schedule caching of itemX every 5 seconds")
        schedule_row_cache(conn, 'itemX', 5)
        print("schedule looks like:")
        s = conn.zrange('schedule:', 0, -1, withscores=True)
        pprint.pprint(s)
        self.assertTrue(s)

        print("We'll start a caching thread that will cache the data...")
        t = threading.Thread(target=cache_rows, args=(conn,))
        t.setDaemon(1)
        t.start()

        time.sleep(1)
        print("data looks like:")
        r = conn.get('inv:itemX')
        print(repr(r))

        self.assertTrue(r)

        print()
        print("will check in 5 s")
        time.sleep(5)
        print("notice taht the data has changed")
        r2 = conn.get('inv:itemX')
        print(repr(r2))
        print()
        self.assertTrue(r2)
        self.assertTrue(r!=r2)

        print("force un-caching")
        schedule_row_cache(conn, 'itemX', -1)
        time.sleep(1)
        r = conn.get('inv:itemX')
        print(
        "The cache was cleared?", not r)
        print()
        self.assertFalse(r)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The database caching thread is still alive?!?")

if __name__ == '__main__':
    unittest.main()
