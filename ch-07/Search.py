#encodeing=utf-8
import redis
import unittest
import uuid
import json
import re
import math


AVERAGE_PRE_1K={}
STOP_WORDS=set('''able about across after all almost also am among
an and any are as at be because been but by can cannot could dear did
do does either else ever every for from get got had has have he her
hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us
wants was we were what when where which while who whom why will with
would yet you your'''.split())

WORDS_RE = re.compile("[a-z']{2,}")
def tokenize(content):
    words = set()                                                       #C
    for match in WORDS_RE.finditer(content.lower()):                    #D
        word = match.group().strip("'")                                 #E
        if len(word) >= 2:                                              #F
            words.add(word)                                             #F
    return words - STOP_WORDS

def index_document(conn, docid, content):
    words = tokenize(content)
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, docid)
    return len(pipeline.execute())

def _set_common_(conn, method, names, ttl=30, execute=True):
    id = str(uuid.uuid4())
    pipeline = conn.pipeline(True)
    names = ['idx:' + name for name in names]
    getattr(pipeline, method)('ids:' + id, *names)
    pipeline.expire('idx:'+id, ttl)
    if execute:
        pipeline.execute()
    return id

def intersect(conn, items, ttl=30, _execute=True):
    return _set_common_(conn, 'sinterstore', items, ttl, _execute)

def union(conn, items, ttl=30, _execute=True):
    return _set_common_(conn, 'sunionstore', items, ttl, _execute)

def difference(conn, items, ttl=30, _execute=True):
    return _set_common_(conn, 'sdiffstore', items, ttl, _execute)

QUERY_RE = re.compile("[+-]?[a-z]{2,}")

def parse(query):
    unwated = set()
    all = []
    current = set()
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '+-':
            word = word[1:]
        else:
            prefix = None

        word = word.strip("'")
        if word in STOP_WORDS or len(word)<2:
            continue

        if prefix == '-':
            unwated.add(word)
            continue

        if current and not prefix:
            all.append(list(current))
            current = set()
        current.add(word)
    if current:
        all.append(list(current))
    return  all, list(unwated)

def parse_and_search(conn, query, ttl=30):
    all,unwanted = parse(query)
    if not all:
        return None
    to_interset = []
    for sync in all:
        if len(sync)>1:
            to_interset.append(union(conn, sync, ttl = ttl))
        else:
            to_interset.append(sync[0])
    if len(to_interset)>1:
        intersect_result = intersect(conn, to_interset, ttl=ttl)
    else:
        intersect_result = to_interset[0]

    if unwanted:
        unwanted.insert(0, intersect_result)
        return(difference(conn, unwanted, ttl=ttl))
    return intersect_result

def search_and_sort(conn, query, id=None, ttl=300, sort='-updated', start=0, num=20):
    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    by = 'kb:doc:*->' + sort
    alpha = sort not in ('updated','id','created')
    if id and not conn.expire(id, ttl):
        id = None
    if not id:
        id = parse_and_search(conn, query, ttl=ttl)
    pipeline = conn.pipeline(True)
    pipeline.scard('idx'+id)
    pipeline.sort('idx:'+id, by=by, alpha=alpha, desc=desc, start=start, num=num)
    results = pipeline.execute()
    return results[0], results[1], id

def search_and_zsort(conn, query, id = None, ttl=300, update=1, vote=0, start=0, num=20, desc=True):
    if id and not conn.expire(id, ttl):
        id=None
    if not id:
        id = parse_and_search(conn, query, ttl)
        scored_search = {
            id:0,
            'sort:update':update,
            'sort:votes':vote
        }
        id = zintersect(conn, scored_search, ttl)
    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:'+id)
    if desc:
        pipeline.zrevrange('idx:'+id, start, start+num-1)
    else:
        pipeline.zrange('idx:'+id, start, start+num-1)
    results = pipeline.execute()
    return results[0],results[1],id

def _zset_common(conn, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())
    #pop:删除指定给定键所对应的值，返回这个值并从字典中把它移除
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline(True) if execute else conn
    #zinterstore中的权重，在这里用字典的值表示
    for key in scores.keys():
        scores['idx:'+key] = scores.pop(key)
    getattr(pipeline, method)('idx:'+id, scores, **kw)
    pipeline.expire('idx:'+id, ttl)
    if execute:
        pipeline.execute()
    return id

def zintersect(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)

def zunion(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)

def string_to_score(string, ignore_case=False):
    if ignore_case:
        string = string.lower()
    pieces = list(map(ord, string[:6]))
    print(pieces)
    while len(pieces)<6:
        pieces.append(-1)
    score = 0
    for piece in pieces:
        score = score*257 + piece + 1

    return score * 2 + (len(string)>6)

def to_char_map(set):
    out = {}
    for pos, val in enumerate(sorted(set)):
        out[val] = pos -1
    return out

LOWER = to_char_map(set[-1]) | set(range(ord('a'), ord('z')+1))
ALPHA = to_char_map(set(LOWER)) | set(range(ord('A'), ord('Z')+1))
LOWER_NUMERIC = to_char_map(set(LOWER)) | set(range(ord('0'), ord('9')+1))
ALPHA_NUMERIC = to_char_map(set(ALPHA)) | set(ALPHA)

def cpc_to_ecpm(views, clicks, cpc):
    return 1000.*cpc*clicks/views
def cpa_to_ecpm(views, actions, cpa):
    return 1000. * cpa * actions / views

TO_ECPM = {
    'cpc':cpc_to_ecpm,
    'cpa':cpa_to_ecpm,
    'cpm':lambda *args:args[-1]
}

def index_ad(conn, id, locations, content, type, value):
    pipeline = conn.pipeline(True)
    for location in locations:
        pipeline.sadd('idx:req:'+location, id)

    words = tokenize(content)
    for word in words:
        pipeline.zadd('idx:'+word, id, 0)

    rvalue = TO_ECPM[type](1000, AVERAGE_PRE_1K.get(type, 1), value)
    pipeline.hset('type', id, type)
    pipeline.zadd('idx:ad:value:',id, rvalue)
    pipeline.zadd('ad:base_value:', id, value)
    pipeline.sadd('terms:'+id, *list(words))
    pipeline.execute()

def target_ads(conn, locations, content):
    pipeline = conn.pipeline(True)
    matched_ads, base_ecpm = match_location(pipeline, locations)
    words, target_ads = finish_scoring(pipeline, matched_ads, base_ecpm, content)
    pipeline.incr('ads:served:')
    pipeline.zrevrange('idx:'+target_ads, 0, 0)
    target_id, targeted_ad = pipeline.execute()[-2:]
    if not targeted_ad:
        return None, None
    ad_id = targeted_ad[0]
    record_targeting_result(conn, target_id, ad_id, words)
    return target_id, ad_id

def finish_scoring(pipe, matched, base, content):
    bonus_ecmp = {}
    words = tokenize(content)
    for word in words:
        word_bonus = zintersect(pipe, {matched:0, word:1}, _execute=False)
        bonus_ecmp[word_bonus]=1
    if bonus_ecmp:
        minimum = zunion(pipe, bonus_ecmp, aggregate="MIN", _execute=False)
        maximum = zunion(pipe, bonus_ecmp, aggregate="MAX", _execute=False)
        return words, zunion(pipe, {base:1,minimum:.5, maximum:.5},_execute=False)
    return words, base

def match_location(pipeline, locations):
    required = ['req:'+ loc for loc in locations]
    match_ads = union(pipeline, required, ttl=300, _execute=False)
    return match_ads, zintersect(pipeline, {match_ads:0, 'ad:value:':1}, _execute=False)

def record_targeting_result(conn, target_id, ad_id, words):
    pipeline = conn.pipeline(True)
    terms = conn.smembers('terms:' + ad_id)
    matched = list(words & terms)
    if matched:
        matched_key = 'terms:matched:%s'%target_id
        pipeline.sadd(matched_key, *matched)
        pipeline.expire(matched_key, 900)

    type = conn.hget('type:', ad_id)
    pipeline.incr('type:%s:views:'%type)
    for word in matched:
        pipeline.zincr('views:%s'%ad_id, word)
    pipeline.zincrby('viewwd:%s'%ad_id,'')
    if not pipeline.execute()[-1]%100
        update_cpms(conn, ad_id)

def record_click(conn, target_id, ad_id, action=False):
    pipeline = conn.pipeline(True)
    click_key = 'click:%s'%ad_id
    match_key = 'terms:matched:%s'%target_id
    type = conn.hget('type:', ad_id)
    if type=='cpa':
        pipeline.expire(match_key,900)
        if action:
            click_key='action%s'%ad_id
    if action and type == 'cpa':
        pipeline.incr('type:%s:actions:'%type)
    else:
        pipeline.incr('type:%s:clicks:'%type)
    matched = list(conn.smembers(match_key))
    matched.append('')
    for word in matched:
        pipeline.zincrby(click_key, word)
    pipeline.execute()
    update_cpms(conn, ad_id)





class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'
    def setUp(self):
        self.conn = redis.Redis(db=15, decode_responses=True)
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def test_index_document(self):
        print("tokenize some document")
        tokens = tokenize(self.content)
        print("these tokens are:" , tokens)
        self.assertTrue(tokens)
        print("And now we are indexing that content...")
        r = index_document(self.conn, 'test', self.content)
        self.assertEqual(r, len(tokens))
        for t in tokens:
            self.assertEqual(self.conn.smembers('idx:' + t), set(['test']))

    def test_set_operations(self):
        index_document(self.conn, 'test', self.content)
        r = intersect(self.conn, ['content', 'indexed'])
        print(type(self.conn.smembers('idx:' + r)))
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = intersect(self.conn, ['content', 'ignored'])
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = union(self.conn, ['content', 'ignored'])
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'ignored'])
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'indexed'])
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

    def test_parse_query(self):
        query = 'test query without stopwords'
        self.assertEqual(parse(query),([[x] for x in query.split()],[]))
        query = 'test +query without -stopwords'
        self.assertEqual(parse(query), ([['test','query'],['without']],['stopwords']))

    def test_parse_and_search(self):
        index_document(self.conn, 'test', self.content)
        r = parse_and_search(self.conn, 'content')
        self.assertEqual(self.conn.smembers("idx:"+r), set(['test']))
        print(set(['test']))
        r = parse_and_search(self.conn, 'content indexed random')
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content +indexed random')
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed +random')
        self.assertEqual(self.conn.smembers('idx:' + r), set(['test']))
        r = parse_and_search(self.conn, 'content indexed -random')
        self.assertEquals(self.conn.smembers('idx:' + r), set())

    def test_search_with_sort(self):
        print("And now let's test searching with sorting...")

        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)
        self.conn.hmset('kb:doc:test', {'updated': 12345, 'id': 10})
        self.conn.hmset('kb:doc:test2', {'updated': 54321, 'id': 1})

        r = search_and_sort(self.conn, "content")
        self.assertEqual(r[1], ['test2', 'test'])

        r = search_and_sort(self.conn, "content", sort='-id')
        self.assertEqual(r[1], ['test', 'test2'])
        print("Which passed!")

    def test_search_and_zscore(self):
        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)

        self.conn.zadd('idx:sort:update','test',12345,'test2',54321)
        self.conn.zadd('idx:sort:votes','test', 10, 'test2',1)
        r = search_and_zsort(self.conn, "content", desc=False)
        self.assertEqual(r[1], ['test','test2'])
        r = search_and_zsort(self.conn, "content", desc=False, update=0, vote=1)
        self.assertEqual(r[1], ['test2','test'])

    def test_string_to_score(self):
        words = 'these are some words that will be sorted'.split()
        pairs = [(word, string_to_score(word)) for word in words]
        pairs2 = list(pairs)
        pairs2.sort(key=lambda x : x[1])
        pairs.sort()
        self.assertEqual(pairs, pairs2)


if __name__ == '__main__':
    unittest.main()