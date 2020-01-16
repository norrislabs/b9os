import b9py


class Parameter(object):
    def __init__(self, master_uri, nodename, namespace=None):
        self._master_uri = master_uri
        self._nodename = nodename
        if namespace:
            self._namespace = namespace
        else:
            self._namespace = '@'

    def put(self, name, value):
        result = b9py.ServiceClient.oneshot_service_call(self._namespace,
                                                         'master/parameter',
                                                         None,
                                                         self._create_param_put_message(name, value),
                                                         5556, self._master_uri)
        return result

    def get(self, name):
        result = b9py.ServiceClient.oneshot_service_call(self._namespace,
                                                         'master/parameter',
                                                         None,
                                                         self._create_param_get_message(name),
                                                         5556, self._master_uri)
        return result

    def list(self):
        result = b9py.ServiceClient.oneshot_service_call(self._namespace,
                                                         'master/parameter',
                                                         None,
                                                         self._create_param_list_message(),
                                                         5556, self._master_uri)
        return result

    def _create_param_put_message(self, name, value: b9py.Message):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'PUT',
                                                             'nodename': self._nodename,
                                                             'namespace': self._namespace,
                                                             'type': type(value).__name__,
                                                             'name': name,
                                                             'value': value},
                            self._nodename)

    def _create_param_get_message(self, name):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'GET',
                                                             'namespace': self._namespace,
                                                             'name': name},
                            self._nodename)

    def _create_param_list_message(self):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'LIST',
                                                             'namespace': self._namespace},
                            self._nodename)
