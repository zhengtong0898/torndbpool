# -.- coding:utf-8 -.-
import threading
from . import torndb
from queue import Queue
from functools import partial


class PoolException(Exception):
    pass


class Pool(object):

    def __init__(self, host, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=30,
                 time_zone="+8:00", charset="utf8mb4",
                 sql_mode="TRADITIONAL", pool_size=100, **kwargs):
        """
        :param pool_size: 线程池大小, 默认是100, 这个参数应该多大比较合适, 取决
        于mysql的max_connections参数的设定以及操作系统的限制, 例如linux的
        open files, open process等因素, 更多讨论 请参考:
        https://stackoverflow.com/questions/39976756/the-max-connections-in-mysql-5-7
        :param kwargs: 更多参数请参考这里 https://github.com/PyMySQL/PyMySQL/blob/master/pymysql/connections.py#L116
        """
        self.cond = threading.Condition(threading.RLock())
        self._idles = Queue()
        self._busies = []
        self.pool_size = pool_size
        self.db_kwargs = dict(host=host, database=database, user=user,
                               password=password, max_idle_time=max_idle_time,
                               connect_timeout=connect_timeout,
                               time_zone=time_zone, charset=charset,
                               sql_mode=sql_mode, pool=self, **kwargs)

    def connect(self):
        connect_factory = partial(Connection, **self.db_kwargs)
        return self._connect(connect_factory)

    def _connect(self, connect_factory=None, timeout=10):
        with self.cond:

            if self.idles():
                conn = connect_factory()
            else:
                if self._idles.qsize():
                    conn = self._idles.get()
                else:
                    self.cond.wait(timeout)
                    if self._idles.qsize():
                        conn = self._idles.get()
                    else:
                        raise PoolException("Acquire connection time out.")

            self._busies.append(conn)
            return conn

    def release(self, conn):
        with self.cond:
            
            if conn not in self._busies:
                raise PoolException("Release unknown connection.")
            
            index = self._busies.index(conn)
            self._busies.pop(index)
            self._idles.put(conn)
            self.cond.notify()

    def idles(self):
        with self.cond:
            return self.pool_size - len(self._busies)

    def __str__(self):
        return "<{} pool_size={} idles={} busies={} >".format(
                    self.__class__.__name__,  self.pool_size,
                    self.idles(),  len(self._busies))


class Connection(torndb.Connection):

    def __init__(self, pool, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.pool = pool

    def release(self):
        self.pool.release(self)
