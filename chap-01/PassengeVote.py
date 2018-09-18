import now as now
import redis
import time
import unittest
ONE_WEEK_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLE_PER_PAGE = 50

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return
    article_id = article.partition(':')[-1]#partition() 方法用来根据指定的分隔符将字符串进行分割。
    if conn.sadd('voted:',article_id, user):
        conn.zincrby('score:',article, VOTE_SCORE)
        conn.hincrby(article,'votes',1)


def post_article(conn, user, title, link):
    article_id = str(conn.incr('article:'))
    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    article = 'article:' + article_id
    conn.hmset(article, {
        'title':title,
        'link':link,
        'poster':user,
        'tiem':now,
        'votes': 1,
    })
    conn.zadd('score:', article, now+VOTE_SCORE)
    conn.zadd('time:', article, now)
    return article_id

#获取文章
def get_articles(conn, page, order='score'):
    start = (page-1) * ARTICLE_PER_PAGE
    end = start + ARTICLE_PER_PAGE - 1
    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles

def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn, group, page, order='score:'):
    key = order + group#为每个群租创建一个键
    if not conn.exists(key):
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)