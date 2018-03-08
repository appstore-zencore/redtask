import time
import uuid
import redis
import multiprocessing
import unittest
import yaml
from zencore.utils.magic import select
from zencore.utils.magic import import_from_string
from .server import TaskManager
from .server import WorkerStateManager
from .server import TaskServer
from .handlers import SimpleHandler
from . import debug


def example_executor(task):
    counter = select(task, "data.counter")
    if counter in [0, 1]:
        raise ValueError("0 & 1 is not correct")
    return counter


class TestRedtask(unittest.TestCase):
    def setUp(self):
        self.connection = redis.Redis()
        self.connection.flushall()

    def test01(self):
        task_id = str(uuid.uuid4())
        task_manager = TaskManager(self.connection, "redtasktest")
        task_manager.publish("test01", task_id, {"method": "debug.ping"})
        task = task_manager.get(task_id)
        assert self.connection.llen("redtasktest:queue:test01") == 1
        assert task["status"] == "PUBLISHED"
        assert task["id"] == task_id
        assert isinstance(task["published_time"], float)

        task = task_manager.pull("test01", "worker01")
        print(task)
        assert task["id"] == task_id
        assert task["status"] == "PULLED"

        task = {
            "result": {
                "success": True,
                "message": "hello",
            },
            "to_be_deleted": "hello",
        }
        task_manager.update(task_id, task)
        task = task_manager.get(task_id)
        assert "to_be_deleted" in task
        assert "result" in task
        assert task["result"]["success"]

        task_manager.delete_field(task_id, "to_be_deleted")
        task = task_manager.get(task_id)
        assert not "to_be_deleted" in task

        task_manager.mark_finished("worker01", task_id)
        task = task_manager.get(task_id)        
        assert task["status"] == "FINISHED"

        task_id_new = task_manager.pull_finished("worker01")
        assert task_id_new == task_id
        
        closed = task_manager.close_finished("worker01", task_id)
        task = task_manager.get(task_id)
        assert closed
        assert "published_time" in task
        assert "pulled_time" in task
        assert "finished_time" in task
        assert task["status"] == "CLOSED"

        task_manager.delete(task_id)
        task = task_manager.get(task_id)
        assert not task

    def test02(self):
        task_id = str(uuid.uuid4())
        task_manager = TaskManager(self.connection, "redtasktest:")
        task = task_manager.pull("test02", "worker02")
        assert task is None

        closed = task_manager.close_finished("worker02", task_id)
        assert closed is False

    def test03(self):
        wsm = WorkerStateManager(self.connection, "redtasktest", "worker03", 1)
        key = wsm.worker_info_storage.make_key(wsm.get_worker_key())
        wsm.update()
        assert self.connection.keys(key)
        time.sleep(2)
        assert not self.connection.keys(key)

    def test04(self):
        wsm = WorkerStateManager(self.connection, "redtasktest", "worker04", 30)
        key = wsm.worker_info_storage.make_key(wsm.get_worker_key())
        wsm.update()
        assert self.connection.keys(key)
        wsm.delete()
        assert not self.connection.keys(key)

    def test05(self):
        e = import_from_string("example_executor")
        assert callable(e)
        e = import_from_string("redtask.tests.example_executor")
        assert callable(e)

    def test06(self):
        config = yaml.load("""
task-executor:
    services:
        debug.ping: redtask.debug.ping
        debug.echo: redtask.debug.echo
task-server:
  name: ctrlstack
  queue-name: run-ansible-playbook
  pool-size: 3
  pull-timeout: 1
  keepalive: 3
  redis:
    url: redis://localhost/0
    options:
      retry_on_timeout: true
      decode_responses: true
        """)
        print(config)
        server = TaskServer(select(config, "task-server"))
        executor = SimpleHandler(select(config, "task-executor"))
        print(executor.services)
        server.register_executor(executor)
        server.start()
        server.task_manager.publish(server.queue_name, "t1", {
            "method": "debug.ping",
        })
        server.task_manager.publish(server.queue_name, "t2", {
            "method": "debug.echo",
            "params": ["t2"]
        })
        server.task_manager.publish(server.queue_name, "t3", {
            "method": "debug.echo",
            "params": {
                "msg": "t3"
            }
        })
        server.task_manager.publish(server.queue_name, "t4", {
            "method": "debug.NotImplementedService",
        })
        server.task_manager.publish(server.queue_name, "t5", {
            "method": "debug.echo",
        })
        server.task_manager.publish(server.queue_name, "t6", {
            "method": "system.listMethods",
        })
        server.task_manager.publish(server.queue_name, "t7", {
            "method": "system.methodSignature",
            "params": ["debug.echo"],
        })
        server.serve_forever(timeout=5)
        server.stop()

        assert server.task_manager.get("t1")["result"] == "pong"
        assert server.task_manager.get("t2")["result"] == "t2"
        assert server.task_manager.get("t3")["result"] == "t3"
        assert "error" in server.task_manager.get("t4")
        assert "error" in server.task_manager.get("t5")
        assert "system.listMethods" in server.task_manager.get("t6")["result"]
        assert "params" in server.task_manager.get("t7")["result"]
