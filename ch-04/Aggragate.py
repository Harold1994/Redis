import redis
import os
import unittest
import uuid
import time

#将被处理日志信息写入redis
#callback回调函数接受一个redis连接和一个日志行做参数
def process_logs(conn, path, callback):
    #获取当前文件处理进度
    current_file, offset = conn.mget('progress:file','progress:position')
    pipe = conn.pipeline()
    #如果在一个内部函数里，对在外部作用域（但不是在全局作用域）的变量进行引用，那么内部函数就被认为是闭包
    #更新正在处理的日志文件的名字和偏移量
    def update_progress():
        pipe.mset({
            'progress:file':fname,
            'progress:position':offset
        })
        pipe.execute()

    for fname in sorted(os.listdir(path)):
        #略过已处理日志文件
        if fname < current_file:
            continue
        inp = open(os.path.join(path, fname),'rb')

        #在接着处理一个因为崩溃而未完成处理的日志文件时，略过已处理内容
        if fname == current_file:
            inp.seek(int(offset,10))
        else:
            offset = 0
        current_file = None

        for lno, line in enumerate(inp):
            #处理日志行
            callback(pipe, line)
            offset += int(offset) + len(line)
            if not (lno+1)%1000:
                update_progress()
            update_progress()
            inp.close()

#以主从服务器连接作为参数
def wait_for_sync(mconn, sconn):
    identifier  =str(uuid.uuid4())
    #添加令牌到主服务器
    mconn.add('sync:wait',identifier, time.time())
    #等待从服务器完成同步
    while not sconn.info()['master_link_status'] != 'up':
        time.sleep(0.05)
    #等待从服务器接受数据更新
    while not sconn.zscore('sync:wait', identifier):
        time.sleep(.001)
    deadline = time.time() + 1.01#最多等一秒
    while time.time()<deadline:
        if sconn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)
    # 清理刚刚创建的新令牌以及之前的令牌
    mconn.zrem('sync:wait', identifier)
    mconn.zremrangebyscore('sync:wait', 0, time.time()-900)

def list_item(conn, item_id, sellerid, price):
    inventory = "inventory:%s" % sellerid
    item = "%s.%s"%(item_id, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()
    while time.time()<end:
        try:
            pipe.watch(inventory)
            if not pipe.sismember(inventory, item_id):
                pipe.unwatch()
                return None
            pipe.muli()
            pipe.zadd('market:', item, price)
            pipe.zrem(inventory, item_id)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
        return False

def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    buyer = "users:%s"%buyerid
    seller = "users:%s"%sellerid
    item = "%s.%s"%(itemid,sellerid)
    inventory = "inventory:%s"%buyerid
    end = time.time()+10
    pipe = conn.pipeline()
    while time.time() < end:
        try:
            pipe.watch("market:", buyer)
            price = pipe.zscore('market:',item)
            funds = int(pipe.hget(buyer, 'funds'))
            if price!=lprice or price >funds:
                pipe.unwatch()
                return None
            pipe.multi()
            pipe.hincrby(seller,'funds',int(price))
            pipe.hincrby(buyer,'funds', int(-price))
            pipe.sadd(inventory,itemid)
            pipe.srem('market:', item)
            return True
        except redis.exceptions.WatchError:
            pass
    return False

