# torndbpool
```
# -.- coding:utf-8 -.-
from torndbpool.pool import Pool
import time
import threading

pool = Pool(host="localhost:3306", database="database",
            user="user", password="password", pool_size=3)


def test_release_connect(pol):
    conn = pol.connect()
    time.sleep(3)
    conn.release()


threading.Thread(target=test_release_connect, args=(pool, )).start()
threading.Thread(target=lambda x: x.connect(), args=(pool, )).start()
threading.Thread(target=lambda x: x.connect(), args=(pool, )).start()
threading.Thread(target=test_release_connect, args=(pool, )).start()
threading.Thread(target=lambda x: x.connect(), args=(pool, )).start()

```
