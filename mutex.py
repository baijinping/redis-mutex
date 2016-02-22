# -*- coding: utf-8 -*-
"""
Module Description:
Date: 2016/2/22
Author:Bai Jin Ping
"""
import random
import time
import threading

import redis


LOCK_LUA_SCRIPT = """
    if redis.call('exists', KEYS[1]) == 0 then
        return redis.call('setex', KEYS[1], unpack(ARGV))
    end
"""

RELEASE_LUA_SCRIPT = """
    if redis.call('get',KEYS[1]) == ARGV[1] then
        return redis.call('del',KEYS[1])
    else
        redis.log(redis.LOG_WARNING, 'Try release other mutex key ' .. KEYS[1])
        return 0
    end
"""


class Mutex(object):
    """
    key互斥锁
    """
    DEFAULT_OVERTIME = 1

    def __init__(self, key, overtime_sec=DEFAULT_OVERTIME,
                 db=None, need_lock=True):
        if not key:
            raise ValueError('Mutex Key Invalided!')
        if overtime_sec < 0.5:
            raise ValueError('Mutex Overtime(second) Too Short!')
        if not isinstance(overtime_sec, int):
            raise ValueError('Mutex Overtime Is Not a Integer!')

        self.key = key
        self.overtime_sec = overtime_sec
        self.need_lock = need_lock

        # 互斥锁的标识，避免解锁时移除了非当前线程持有的锁
        # 因为有可重入特性，因此在进行lock前才设置
        self.identifier = 0

        if db is None:
            db = self._get_default_redis_conn()
        self.db = db

    def _get_default_redis_conn(self):
        """
        获取默认的redis连接
        :return:
        """
        return redis.Redis()

    def lock(self):
        # 设置互斥锁的标识值
        self.identifier = random.randint(1, 99999)
        while True:
            rtn = self.db.eval(LOCK_LUA_SCRIPT, 1, *(self.key, self.lock_timeout, self.identifier))
            if rtn == 'OK':
                break
            time.sleep(0.1)

    def unlock(self):
        self.db.eval(RELEASE_LUA_SCRIPT, 1, *(self.key, self.identifier))

    def __enter__(self):
        if self.need_lock:
            self.lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.need_lock:
            self.unlock()

    @property
    def lock_timeout(self):
        return self.overtime_sec


class RMutex(Mutex):
    """
    可重入互斥锁
    """
    local = threading.local()

    def __init__(self, key, overtime_sec=Mutex.DEFAULT_OVERTIME,
                 need_lock=True):
        Mutex.__init__(self, key, overtime_sec=overtime_sec,
                       need_lock=need_lock)

        if self.key in self.local.__dict__:
            self.need_lock = False

    def _incr_lock(self):
        if self.key not in self.local.__dict__:
            self.local.__dict__[self.key] = 1
        else:
            self.local.__dict__[self.key] += 1

    def _decr_lock(self):
        if self.key not in self.local.__dict__:
            raise RuntimeError('Cannot Decrease Lock Counter Without Holding!')

        if self.local.__dict__[self.key] == 1:
            del self.local.__dict__[self.key]
        else:
            self.local.__dict__[self.key] -= 1

    def __enter__(self):
        if self.need_lock:
            self.lock()
            self._incr_lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.need_lock:
            self.unlock()
            self._decr_lock()
