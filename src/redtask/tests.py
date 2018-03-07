import uuid
import redis
import unittest
from .base import TaskManage


class TestRedtask(unittest.TestCase):
    def setUp(self):
        self.connection = redis.Redis()
        self.connection.flushall()

    def test01(self):
        task_id = str(uuid.uuid4())
        task_manager = TaskManage(self.connection, "redtasktest:")
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
