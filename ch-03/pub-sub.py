import redis
import time
import threading
import unittest
conn = redis.Redis()
def publisher(n):
    time.sleep(1)
    for i in range(n):
        conn.publish('channel',i)
        time.sleep(1)

def run_pubsun():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0;
    for item in pubsub.listen():
        print(item)
        count+=1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break

def nptrans():
    print(conn.incr('notrans:'))
    time.sleep(2)
    conn.incr('notrans',-1)

def trans():
    pipeline = conn.pipeline()
    pipeline.incr('trans:')
    time.sleep(1)
    pipeline.incr('trans:',-1)
    print(pipeline.execute()[0])

if __name__ == '__main__':
    if 1:
        for i in range(3):
            threading.Thread(target=trans).start()
        time.sleep(.5)
        print(conn.get('trans:'))