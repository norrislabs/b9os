import asyncio
import b9py


class SubscriberMux(object):
    def __init__(self, b9core: b9py.B9, topic, callback, mux_spec, namespace):
        self._b9 = b9core
        self._callback = callback
        self._mux_spec = mux_spec
        self._subscribers = []

        self._namespace = namespace
        if namespace:
            if namespace == '/':
                self._topic = self._namespace + topic.strip('/')
            else:
                self._namespace = namespace.strip('/')
                self._topic = '/' + self._namespace + '/' + topic.strip('/')
        else:
            self._topic = topic

        self._current_priority = len(mux_spec)

    def _callback_wrapper(self, priority, blocking_time):
        def priority_callback(topic, msg: b9py.Message):
            if priority <= self._current_priority:
                self._current_priority = priority
                self._callback(topic, msg)

                for t in range(int(blocking_time / 0.1)):
                    await asyncio.sleep(0.1)
                    if self._current_priority < priority:
                        return

                self._current_priority = len(self._mux_spec)
        return priority_callback

    def subscribe(self):
        for priority in range(len(self._mux_spec)):
            queue_size, blocking_time = self._mux_spec[priority]

            sub_topic = "{}/{}".format(self._topic, str(priority))
            cb = self._callback_wrapper(priority, blocking_time)
            sub = self._b9.create_subscriber(sub_topic, cb, self._namespace, queue_size=queue_size)
            self._subscribers.append(sub)

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def subscribers(self):
        return self._subscribers
