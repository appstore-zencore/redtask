import inspect
from zencore.utils.magic import select
from zencore.utils.magic import import_from_string


class BaseHandler(object):
    def __init__(self, config=None):
        self.config = config or {}
        self.services = {}
    
    def register_service(self, name, callback):
        if callable(callback):
            self.services[name] = callback
            return True
        elif isinstance(callback, str):
            self.services[name] = import_from_string(callback)
            return True
        else:
            return False

    def get_service(self, name):
        return self.services.get(name, None)

    def execute(self, task):
        data = task["data"]
        method = data.get("method")
        params = data.get("params")
        if not method:
            raise ValueError("Task not given method: task={}.".format(task))
        service = self.get_service(method)
        if not service:
            raise NotImplementedError("Service {} not registered.".format(method))
        if isinstance(params, dict):
            return service(**params)
        elif isinstance(params, (list, set)):
            return service(*params)
        else:
            return service()


class SimpleHandler(BaseHandler):

    def __init__(self, config=None):
        super(SimpleHandler, self).__init__(config)
        self.register_service("system.listMethods", self.listMethods)
        self.register_service("system.methodSignature", self.methodSignature)
        self.load_services()

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
