import inspect
import logging
from zencore.utils.magic import select
from zencore.utils.magic import import_from_string


logger = logging.getLogger(__name__)


class TaskExecutor(object):
    def __init__(self, config=None):
        self.config = config or {}
        self.services = {}
        self.register_service("system.listMethods", self.listMethods)
        self.register_service("system.methodSignature", self.methodSignature)
        self.load_services()

    def register_service(self, name, callback):
        logger.debug("Register a service to TaskExecutor: name={} callback={}.".format(name, callback))
        if callable(callback):
            self.services[name] = callback
            return True
        elif isinstance(callback, str):
            service = import_from_string(callback)
            if callable(service):
                self.services[name] = service
                return True
        logger.warn("Register a service failed: name={}.".format(name))
        return False

    def get_service(self, name):
        return self.services.get(name, None)

    def execute(self, task):
        logger.debug("Start to execute a task: task={}.".format(task))
        data = task["data"]
        method = data.get("method")
        params = data.get("params")
        if not method:
            error_message = "Task NOT contain method failed: task={}.".format(task)
            logger.error(error_message)
            raise KeyError(error_message)
        service = self.get_service(method)
        if not service:
            error_message = "Task contain a method not registered: method={}.".format(method)
            logger.error(error_message)
            raise NotImplementedError(error_message)
        if isinstance(params, dict):
            return service(**params)
        elif isinstance(params, (list, set)):
            return service(*params)
        else:
            return service()

    def load_services(self):
        services_map = select(self.config, "services") or {}
        for name, callback in services_map.items():
            self.register_service(name, callback)

    def listMethods(self):
        methods = list(self.services.keys())
        methods.sort()
        return methods

    def methodSignature(self, name):
        service = self.get_service(name)
        sig = inspect.signature(service)
        return {
            "name": name,
            "params": str(sig),
        }
