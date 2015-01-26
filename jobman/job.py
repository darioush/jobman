from __future__ import absolute_import
import redis
import json
import signal
import time

from multiprocessing import Process
from multiprocessing import Pipe

from dothestuff import foo

class TimeoutException(Exception):
    pass

def run_with_limited_time(func, args, kwargs, time):
    def target(conn):
        try:
            out_files, out_result = func(*args, **kwargs)
            conn.send(None)
            conn.send((out_files, out_result))
        except Exception as ex:
            conn.send(ex)
        finally:
            conn.close()

    parent_conn, child_conn = Pipe()
    p = Process(target=target, args=(child_conn,))
    p.start()
    p.join(time)
    if p.is_alive():
        p.terminate()
        raise TimeoutException
    ex = parent_conn.recv()
    if ex:
        raise ex
    return parent_conn.recv()

class Job(object):
    def __init__(self, input, jobkey=None, timeout=30):
        self.input = input
        self.jobkey = jobkey
        self.out_files = None
        self.out_obj = None
        self.timeout = int(timeout)

    def push(self, redis):
        jobid = redis.incr('jobid:maxid')
        self.jobkey = 'jobs:%d' % jobid
        redis.hmset(self.jobkey, {'input': json.dumps(self.input), 'timeout': self.timeout})
        print self.jobkey, redis.hgetall(self.jobkey)
        redis.rpush('jobs:queue', self.jobkey)

    @staticmethod
    def pull(redis):
        jobkey = redis.lpop('jobs:queue')
        jobdescr = redis.hgetall(jobkey)
        return Job(jobkey=jobkey, **jobdescr)

    def run(self):
        try:
            st_time = time.time()
            self.out_files, self.out_obj = run_with_limited_time(foo, (json.loads(self.input),), {}, self.timeout)
            self.time = time.time() - st_time
            self.exception = None
        except TimeoutException as ex:
            self.out_files, self.out_obj = None, None
            self.time = -1
            self.exception = ex
        except Exception as ex:
            self.out_files, self.out_obj = None, None
            self.exception = ex
            self.time = time.time() - st_time

    def report(self):
        exception = None if self.exception is None else {
            'message': self.exception.message,
            'type': self.exception.__class__.__name__
        }
        return {
            'out_files': self.out_files,
            'out_obj': self.out_obj,
            'time': int(self.time*1000),
            'exception': exception,
        }


if __name__ == "__main__":
    j = Job({'l': 2, 'sleep_for': 50}, timeout=2)
    redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    j.push(redis)
    j_ = Job.pull(redis)
    j_.run()
    print json.dumps(j_.report())

