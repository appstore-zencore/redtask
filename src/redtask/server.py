import os
import uuid
import time
import redis
import logging
import platform
import threading
from rjs import JsonStorage
from zencore.utils.system import get_main_ipaddress
from zencore.utils.magic import select
from zencore.utils.magic import import_from_string


logger = logging.getLogger(__name__)


class WorkerStateManager(object):
    def __init__(self, connection, worker_name, expire=30, prefix=""):
        self.connection = connection
        self.worker_name = worker_name
        self.prefix = prefix
        self.expire = expire
        self.worker_info_storage = JsonStorage(self.connection, prefix=self.prefix)

    def get_worker_id(self):
        return self.worker_name + "#" + str(os.getpid())

    def get_worker_key(self):
        return "worker:info:" + self.get_worker_id()

    def update(self):
        info = {
            "name": self.worker_name,
            "hostname": platform.node(),
            "mainip": get_main_ipaddress(),
            "pid": os.getpid(),
            "tid": threading.get_ident(),
        }
        worker_key = self.get_worker_key()
        self.worker_info_storage.update(worker_key, info, self.expire)

    def delete(self):
        worker_key = self.get_worker_key()
        self.worker_info_storage.delete(worker_key)


class TaskStateManager(object):
    """Task ID life cycle.
    """
    def __init__(self, connection, prefix=""):
        self.connection = connection
        self.prefix = prefix

    def task_id_clean(self, task_id):
        if task_id:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")
        return task_id

    def make_key(self, key):
        return self.prefix + key

    def make_task_queue_key(self, queue):
        return self.make_key("queue:" + queue)

    def make_worker_running_queue_key(self, worker_id):
        return self.make_key("worker:running:queue:" + worker_id)

    def make_worker_finished_queue_key(self, worker_id):
        return self.make_key("worker:finished:queue:" + worker_id)

    def publish(self, queue, task_id):
        task_queue_key = self.make_task_queue_key(queue)
        self.connection.rpush(task_queue_key, task_id)

    def pull(self, queue, worker_id, timeout=0):
        task_queue_key = self.make_task_queue_key(queue)
        worker_running_queue_key = self.make_worker_running_queue_key(worker_id)
        if timeout:
            task_id = self.connection.brpoplpush(task_queue_key, worker_running_queue_key, timeout=timeout)
        else:
            task_id = self.connection.rpoplpush(task_queue_key, worker_running_queue_key)
        return self.task_id_clean(task_id)

    def mark_finished(self, worker_id, task_id):
        worker_finished_queue_key = self.make_worker_finished_queue_key(worker_id)
        self.connection.lpush(worker_finished_queue_key, task_id)

    def pull_finished(self, worker_id, timeout=0):
        worker_finished_queue_key = self.make_worker_finished_queue_key(worker_id)
        worker_running_queue_key = self.make_worker_running_queue_key(worker_id)
        if timeout:
            task_id = self.connection.brpoplpush(worker_finished_queue_key, worker_running_queue_key, timeout=timeout)
        else:
            task_id = self.connection.rpoplpush(worker_finished_queue_key, worker_running_queue_key)
        return self.task_id_clean(task_id)

    def close_finished(self, worker_id, task_id):
        worker_running_queue_key = self.make_worker_running_queue_key(worker_id)
        num = self.connection.lrem(worker_running_queue_key, task_id, 2)
        if num:
            return True
        return False


class TaskManage(object):
    """Task state & data manager.
    """
    def __init__(self, connection, prefix=""):
        self.connection = connection
        self.prefix = prefix
        self.task_data_prefix = prefix + "task:"
        self.state_manager = TaskStateManager(self.connection, self.prefix)
        self.task_storage = JsonStorage(self.connection, self.task_data_prefix)

    def publish(self, queue, task_id, task_data):
        task = {
            "id": task_id,
            "data": task_data,
            "published_time": time.time(),
            "status": "PUBLISHED",
        }
        self.task_storage.update(task_id, task)
        self.state_manager.publish(queue, task_id)
        return task_id

    def pull(self, queue, worker_id, timeout=0):
        task_id = self.state_manager.pull(queue, worker_id, timeout=timeout)
        if not task_id:
            return None
        task = {
            "pulled_time": time.time(),
            "worker_id": worker_id,            
            "status": "PULLED",
        }
        self.task_storage.update(task_id, task)
        return self.task_storage.get(task_id)

    def mark_finished(self, worker_id, task_id):
        self.state_manager.mark_finished(worker_id, task_id)
        task = {
            "finished_time": time.time(),
            "status": "FINISHED",
        }
        self.task_storage.update(task_id, task)

    def pull_finished(self, worker_id, timeout=0):
        return self.state_manager.pull_finished(worker_id, timeout=timeout)

    def close_finished(self, worker_id, task_id):
        closed = self.state_manager.close_finished(worker_id, task_id)
        if closed:
            task = {
                "status": "CLOSED",
                "closed_time": time.time(),
            }
            self.task_storage.update(task_id, task)
        return closed

    def get(self, task_id):
        return self.task_storage.get(task_id)

    def update(self, key, data=None, expire=None):
        return self.task_storage.update(key, data, expire)

    def delete(self, key):
        return self.task_storage.delete(key)

    def delete_field(self, key, field):
        return self.task_storage.delete_field(key, field)


class TaskServer(object):
    """Task server.
    """
    def __init__(self, config):
        self.config = config
        self.prefix = select(config, "task-server.prefix")
        self.connection_config = select(config, "task-server.redis")
        self.connection = self.make_connection(self.connection_config)
        self.worker_name = select(config, "task-server.worker.name") or str(uuid.uuid4())
        self.worker_expire = select(config, "task-server.worker.expire") or 30
        self.queue = select(config, "task-server.queue")
        self.pull_timeout = select(config, "task-server.pull-timeout") or 1
        self.threads = select(config, "task-server.threads")
        self.worker_flag = threading.Semaphore(self.threads)
        self.handler_class = select(config, "task-server.handler.class")
        self.handler_options = select(config, "task-server.handler.options")
        self.executor = import_from_string(self.handler_class)(self.handler_options)
        self.worker_state_manager = WorkerStateManager(self.connection, self.worker_name, self.worker_expire, self.prefix)
        self.task_manager = TaskManage(self.connection, self.prefix)
        self.stop_flag = False
        self.worker_keepalive_thread = None
        self.dead_worker_clean_thread = None
        self.pull_thread = None
        self.pull_finished_thread = None

    def make_connection(self, config):
        url = select(config, "url")
        options = select(config, "options") or {}
        return redis.Redis.from_url(url, **options)

    def worker_keepalive_thread_main(self):
        while not self.stop_flag:
            self.worker_state_manager.update()
            time.sleep(self.worker_expire/3)

    def start_worker_keepalive_thread(self):
        self.worker_keepalive_thread = threading.Thread(target=self.worker_keepalive_thread_main)
        self.worker_keepalive_thread.setDaemon(True)
        self.worker_keepalive_thread.start()

    def dead_worker_clean_thread_main(self):
        while not self.stop_flag:
            time.sleep(1)

    def start_dead_worker_clean_thread(self):
        self.dead_worker_clean_thread = threading.Thread(target=self.dead_worker_clean_thread_main)
        self.dead_worker_clean_thread.setDaemon(True)
        self.dead_worker_clean_thread.start()

    def task_process_main(self, task):
        try:
            task_id = task["id"]
            result = self.executor.execute(task)
            info = {
                "result": result,
            }
            self.task_manager.update(task_id, info)
        except Exception as error:
            logger.exception("Task process failed: task={}".format(task))
            try:
                task_id = task["id"]
                info = {
                    "error": {
                        "code": -1,
                        "message": "Unknown exception.",
                        "data": str(error),
                        "time": time.time(),
                    }
                }
                self.task_manager.update(task_id, info)
            except Exception:
                logger.exception("Task process failed and update task with error failed too...")
        finally:
            try:
                task_id = task["id"]
                self.task_manager.mark_finished(self.worker_state_manager.get_worker_id(), task_id)
            except:
                logger.exception("Mark task finished failed: task={}.".format(task))
            self.worker_flag.release()

    def start_task_process(self, task):
        try:
            t = threading.Thread(target=self.task_process_main, args=[task])
            t.setDaemon(True)
            t.start()
        except Exception:
            self.worker_flag.release()
            logger.exception("Start task process failed, release a flag.")

    def pull_thread_main(self):
        while not self.stop_flag:
            flag = self.worker_flag.acquire(timeout=1)
            if not flag:
                continue
            task = self.task_manager.pull(self.queue, self.worker_state_manager.get_worker_id(), timeout=self.pull_timeout)
            if task:
                self.start_task_process(task)

    def start_pull_thread(self):
        self.pull_thread = threading.Thread(target=self.pull_thread_main)
        self.pull_thread.setDaemon(True)
        self.pull_thread.start()

    def pull_finished_thread_main(self):
        worker_id = self.worker_state_manager.get_worker_id()
        while not self.stop_flag:
            task_id = self.task_manager.pull_finished(worker_id, self.pull_timeout)
            if task_id:
                self.task_manager.close_finished(worker_id, task_id)

    def start_pull_finished_thread(self):
        self.pull_finished_thread = threading.Thread(target=self.pull_finished_thread_main)
        self.pull_finished_thread.setDaemon(True)
        self.pull_finished_thread.start()

    def start(self):
        self.stop_flag = False
        self.start_worker_keepalive_thread()
        self.start_dead_worker_clean_thread()
        self.start_pull_thread()
        self.start_pull_finished_thread()

    def serve_forever(self, timeout=0):
        stime = time.time()
        while not self.stop_flag:
            if timeout and time.time() - stime > timeout:
                break
            time.sleep(1)

    def stop(self):
        self.stop_flag = True
