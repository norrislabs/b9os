import b9py


class Parameter(object):
    def __init__(self, broker_uri, nodename, namespace=None, parameter_topic=None):
        self._broker_uri = broker_uri
        self._nodename = nodename

        if namespace:
            self._namespace = namespace
        else:
            self._namespace = '@'

        if parameter_topic:
            # Using an external parameter server
            self._parameter_topic = parameter_topic
            self._port = 5556 if parameter_topic == 'broker/parameter' else None
        else:
            # Use the broker's parameter server
            self._parameter_topic = 'broker/parameter'
            self._port = 5556

    def put(self, name, value):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_put_message(name, value),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def get(self, name):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_get_message(name),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def list(self):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_list_message(),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def save(self, filename, namespace="@"):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_save_message(filename, namespace),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def load(self, filename, namespace="@", publish_change=True):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_load_message(filename, namespace,
                                                                                         publish_change),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def purge(self, namespace="@"):
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         self._parameter_topic,
                                                         None,
                                                         self._create_param_purge_message(namespace),
                                                         self._port,
                                                         None,
                                                         self._broker_uri)
        return result

    def _create_param_put_message(self, name, value: b9py.Message):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'PUT',
                                                             'nodename': self._nodename,
                                                             'namespace': self._namespace,
                                                             'type': type(value).__name__.capitalize(),     # !!!
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

    def _create_param_save_message(self, filename, namespace):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'SAVE',
                                                             'namespace': namespace,
                                                             'filename': filename},
                            self._nodename)

    def _create_param_load_message(self, filename, namespace, publish_change):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'LOAD',
                                                             'namespace': namespace,
                                                             'filename': filename,
                                                             'publish_change': publish_change},
                            self._nodename)

    def _create_param_purge_message(self, namespace):
        return b9py.Message(b9py.Message.MSGTYPE_PARAMETER, {'cmd': 'PURGE',
                                                             'namespace': namespace},
                            self._nodename)
