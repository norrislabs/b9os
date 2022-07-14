import logging
import b9py


ACTION_STATUS_IDLE = 0
ACTION_STATUS_STARTED = 1
ACTION_STATUS_STOPPED = 2
ACTION_STATUS_PAUSED = 3
ACTION_STATUS_PROGRESS = 4
ACTION_STATUS_COMPLETE = 5
ACTION_STATUS_FAILED = 6


class ActionServer(object):
    def __init__(self, b9core: b9py.B9, action_topic, namespace, start_cb, stop_cb, pause_cb=None):
        self._b9 = b9core
        self._topic = action_topic
        self._namespace = namespace
        self._nodename = self._b9.nodename

        self._start_cb = start_cb
        self._stop_cb = stop_cb
        self._pause_cb = pause_cb

        self._current_status = ACTION_STATUS_IDLE

        # Setup action status publisher
        self._status_pub = self._b9.create_publisher("action/" + self._topic + "/status",
                                                     b9py.Message.MSGTYPE_ACTION_STATUS, namespace)
        stat = self._status_pub.advertise()
        if not stat.is_successful:
            logging.error("Action status publisher failed to advertise.")
            return

        # Setup start service
        self._start_srv = self._b9.create_service("action/" + self._topic + "/start",
                                                  b9py.Message.MSGTYPE_ANY,
                                                  self._cb_closure(self._start_cb),
                                                  namespace)
        stat = self._start_srv.advertise()
        if not stat.is_successful:
            logging.error("Action start service failed to advertise.")
            return

        # Setup stop service
        self._stop_srv = self._b9.create_service("action/" + self._topic + "/stop",
                                                 b9py.Message.MSGTYPE_ANY,
                                                 self._cb_closure(self._stop_cb),
                                                 namespace)
        stat = self._stop_srv.advertise()
        if not stat.is_successful:
            logging.error("Action stop service failed to advertise.")
            return

        # Setup optional pause service
        if self._pause_cb:
            self._pause_srv = self._b9.create_service("action/" + self._topic + "/pause",
                                                      b9py.Message.MSGTYPE_ANY,
                                                      self._cb_closure(self._pause_cb),
                                                      namespace)
            stat = self._pause_srv.advertise()
            if not stat.is_successful:
                logging.error("Action pause service failed to advertise.")
                return

    def publish_status(self, status_code: int, status_data):
        msg = b9py.MessageFactory.create_message_action_status(status_code, status_data)
        self._status_pub.publish(msg)
        self._current_status = status_code

    @staticmethod
    def _cb_closure(callback):
        def action_callback(_topic, msg: b9py.Message):
            result = callback(msg.data)
            return b9py.MessageFactory.create_message_any(result)
        return action_callback

    @property
    def current_status(self):
        return self._current_status

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def nodename(self):
        return self._nodename


class ActionClient(object):
    def __init__(self, b9core: b9py.B9, action_topic, namespace, status_cb):
        self._b9 = b9core
        self._topic = action_topic
        self._namespace = namespace
        self._nodename = self._b9.nodename
        self._broker_uri = self._b9.broker_uri

        self._current_status = ACTION_STATUS_IDLE
        self._current_data = None

        self._status_sub = self._b9.create_subscriber("action/" + action_topic + "/status",
                                                      self._cb_closure(status_cb),
                                                      self._namespace)

    def start(self, data=None) -> b9py.B9Status:
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         "action/" + self._topic + "/start",
                                                         self._namespace,
                                                         self._create_action_message(data),
                                                         broker_uri=self._broker_uri)
        return result

    def stop(self, data=None) -> b9py.B9Status:
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         "action/" + self._topic + "/stop",
                                                         self._namespace,
                                                         self._create_action_message(data),
                                                         broker_uri=self._broker_uri)
        return result

    def pause(self, data=None) -> b9py.B9Status:
        result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                         "action/" + self._topic + "/pause",
                                                         self._namespace,
                                                         self._create_action_message(data),
                                                         broker_uri=self._broker_uri)
        return result

    @property
    def current_status(self):
        return self._current_status

    @property
    def current_data(self):
        return self._current_data

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def nodename(self):
        return self._nodename

    def _cb_closure(self, callback):
        def action_status_callback(topic, msg: b9py.Message):
            self._current_status = msg.data["status_code"]
            self._current_data = msg.data["status_data"]
            callback(topic, msg)
        return action_status_callback

    def _create_action_message(self, data: b9py.Message):
        if data:
            return b9py.MessageFactory.create_message_any(data, self._nodename)
        else:
            return b9py.MessageFactory.create_message_null(self._nodename)
