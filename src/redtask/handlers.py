from zencore.utils.magic import select
from zencore.utils.magic import import_from_string


class BaseHandler(object):
    def __init__(self, config):
        self.config = config
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
        raise NotImplementedError()


class SimpleHandler(BaseHandler):

    def __init__(self, config):
        super(SimpleHandler, self).__init__(config)
        self.services_map = select(self.config, "services")
        self.load_services()

    def load_services(self):
        for name, callback in self.services_map.items():
            self.register_service(name, callback)

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
