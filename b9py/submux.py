import logging
from threading import Timer
import b9py


class BlockTimer(object):
    def __init__(self, release_fn):
        self.thread = None
        self._all_done = release_fn

    def start(self, interval):
        self.thread = Timer(interval, self._all_done)
        self.thread.start()

    def cancel(self):
        if self.thread:
            self.thread.cancel()
        self._all_done()


# This is the thing that makes complex emergent behavior possible
class SubscriberMultiplexer(object):
    def __init__(self, b9core: b9py.B9, topic, namespace, callback, mux_spec):
        self._b9 = b9core
        self._topic = topic
        self._namespace = namespace
        self._callback = callback
        self._mux_spec = mux_spec

        self._subscribers = []
        self._blocker = BlockTimer(self._release_block)

        self._current_priority = len(mux_spec)

        # Build out all the subscribers and their callbacks
        for priority in range(len(self._mux_spec)):
            blocking_time, queue_size, rate, sub_topic = self._mux_spec[priority]

            if sub_topic is None:
                sub_topic = "{}/{}".format(self._topic, str(priority))
            cb = self._callback_wrapper(priority, blocking_time)
            sub = self._b9.create_subscriber(sub_topic, cb, self._namespace,
                                             queue_size=queue_size,
                                             rate=rate)
            self._subscribers.append(sub)

    def _release_block(self):
        self._current_priority = len(self._mux_spec)
        logging.debug("Release")

    def _callback_wrapper(self, priority, blocking_time):
        def priority_callback(topic, msg: b9py.Message):
            # Same priority
            if priority == self._current_priority:
                if not self._callback(topic, msg):
                    self._blocker.start(blocking_time)
                    logging.debug("Refresh : {}".format(topic))
                else:
                    self._blocker.cancel()
                    logging.debug("Canceled: {}".format(topic))

            # Higher priority
            elif priority < self._current_priority:
                self._current_priority = priority
                if not self._callback(topic, msg):
                    self._blocker.start(blocking_time)
                    logging.debug("Priority: {}".format(topic))
                else:
                    self._blocker.cancel()
                    logging.debug("Canceled: {}".format(topic))

            # Lower priority
            else:
                logging.debug("Blocked : {}".format(topic))

        return priority_callback

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def mux_spec(self):
        return self._mux_spec

    @property
    def subscribers(self):
        return self._subscribers
