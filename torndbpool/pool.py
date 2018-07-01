import threading
from . import torndb
from queue import Queue
from functools import partial


class PoolException(Exception):
    pass


class Pool(object):

    def __init__(self, pool_size=100):
        """
        :param pool_size: 线程池大小, 默认是100, 这个参数应该多大比较合适, 取决
        于mysql的max_connections参数的设定以及操作系统的限制, 例如linux的
        open files, open process等因素, 更多讨论 请参考:
        https://stackoverflow.com/questions/39976756/the-max-connections-in-mysql-5-7
        """
        self.lock = threading.RLock()
        self.queue = Queue()
        self.actives = []
        self.pool_size = pool_size

    def get(self, connect_factory=None, timeout=30):
        with self.lock:

            if not self.idles():
                if self.pool_size > len(self.actives):
                    conn = connect_factory()
                else:
                    conn = self.queue.get(timeout=timeout)
            else:
                conn = self.queue.get(timeout=timeout)

            self.actives.append(conn)
            return conn

    def release(self, conn):
        with self.lock:

            if conn not in self.actives:
                raise PoolException("Release unknown connection.")

            index = self.actives.index(conn)
            self.actives.pop(index)
            self.queue.put(conn)

    def idles(self):
        with self.lock:
            return self.queue.qsize()

    def connect(self, host, database, user=None, password=None,
                max_idle_time=7 * 3600, connect_timeout=30, time_zone="+0:00",
                charset="utf8", sql_mode="TRADITIONAL"):

        connect_factory = partial(Connection, **{
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "max_idle_time": max_idle_time,
            "connect_timeout": connect_timeout,
            "time_zone": time_zone,
            "charset": charset,
            "sql_mode": sql_mode,
            "pool": self
        })
        return self.get(connect_factory)

    def __str__(self):
        return "<{} pool_size={} idles={} actives={} >".format(
                    self.__class__.__name__,  self.pool_size,
                    self.idles(),  len(self.actives))


class Connection(torndb.Connection):

    def __init__(self, pool, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.pool = pool

    def release(self):
        self.pool.release(self)
