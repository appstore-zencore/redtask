import os
import uuid
import time
import redis
import platform
import threading
from rjs import JsonStorage
from zencore.utils.system import get_main_ipaddress
from zencore.utils.magic import select


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

    def pull(self, queue, worker_id):
        task_queue_key = self.make_task_queue_key(queue)
        worker_running_queue_key = self.make_worker_running_queue_key(worker_id)
        task_id = self.connection.rpoplpush(task_queue_key, worker_running_queue_key)
        return self.task_id_clean(task_id)

    def mark_finished(self, worker_id, task_id):
        worker_finished_queue_key = self.make_worker_finished_queue_key(worker_id)
        self.connection.lpush(worker_finished_queue_key, task_id)

    def pull_finished(self, worker_id):
        worker_finished_queue_key = self.make_worker_finished_queue_key(worker_id)
        worker_running_queue_key = self.make_worker_running_queue_key(worker_id)
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

    def pull(self, queue, worker_id):
        task_id = self.state_manager.pull(queue, worker_id)
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

    def pull_finished(self, worker_id):
        return self.state_manager.pull_finished(worker_id)

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
    """
config.yml

queue: hello
redis:
    url: redis://app02
    options:
prefix: "ansible-gateway:"
worker:
    name: 1a3c1921-07b5-476e-a53a-3a1f53b676a5
    expire: 30

server = TaskServer(config)

def on_exit(sig, frame):
    server.stop()

server.start()
signal.signal(signal.SIGTERM, on_exit)
server.serve_forever()

    """
    def __init__(self, config):
        self.config = config
        self.prefix = select(config, "prefix")
        self.connection_config = select(config, "redis")
        self.connection = self.make_connection(self.connection_config)
        self.worker_name = select(config, "worker.name") or str(uuid.uuid4())
        self.worker_expire = select(config, "worker.expire") or 30
        self.queue = select(config, "queue")
        self.worker_state_manager = WorkerStateManager(self.connection, self.worker_name, self.worker_expire, self.prefix)
        self.task_manager = TaskManage(self.connection, self.prefix)
        
    def make_connection(self, config):
        url = select(config, "url") or "redis://localhost/0"
        options = select(config, "options") or {}
        return redis.Redis.from_url(url, **options)

    def worker_state_manage_main(self):
        while not self.stop:
            self.worker_state_manager.update()
            time.sleep(self.worker_expire/2)

    def start_worker_state_manager(self):
        self.worker_state_manager_thread = threading.Thread(target=self.worker_state_manage_main)
        self.worker_state_manager_thread.setDaemon(True)
        self.worker_state_manager_thread.start()

    def worker_pull_main(self):
        while not self.stop:
            task_id = self.task_manager.pull(self.queue, self.worker_state_manager.get_worker_id())
            if task_id:
                task = self.task_manager

    def start_worker_pull(self):
        self.worker_pull_thread = threading.Thread(target=self.worker_pull_main)
        self.worker_pull_thread.setDaemon(True)
        self.worker_pull_thread.start()

    def start_pull_finished_worker(self):
        pass

    def start(self):
        self.start_worker_state_manager()
        self.start_pull_worker()
        self.start_pull_finished_worker()

    def serve_forever(self):
        pass

    def stop(self):
        pass

