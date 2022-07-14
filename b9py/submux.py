import logging
from threading import Timer
import b9py


class LockoutTimer(object):
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


# This is the thing that makes emergent behavior possible
class SubscriberMultiplexer(object):
    def __init__(self, b9core: b9py.B9, topic, namespace, callback, mux_spec, synchro_pub):
        self._b9 = b9core
        self._topic = topic
        self._namespace = namespace
        self._callback = callback
        self._mux_spec = mux_spec

        self._subscribers = []
        self._locker = LockoutTimer(self._release_block)

        self._current_priority = len(mux_spec)

        # Synchronization publisher
        self._synchro_pub = None
        if synchro_pub:
            self._synchro_pub = self._b9.create_publisher(topic + "/sync", b9py.Message.MSGTYPE_SUBMUX_SYNC, namespace)
            stat = self._synchro_pub.advertise()
            if not stat.is_successful:
                logging.error("Submux sync publisher failed to advertise.")

        # Build out all the subscribers and their callbacks
        for priority in range(len(self._mux_spec)):
            blocking_time, queue_size, rate, sub_topic = self._mux_spec[priority]

            if sub_topic is None:
                new_topic = "{}/{}".format(self._topic, str(priority))
            else:
                new_topic = "{}/{}".format(self._topic, sub_topic)
            cbc = self._callback_closure(priority, sub_topic, blocking_time)
            sub = self._b9.create_subscriber(new_topic, cbc, self._namespace,
                                             queue_size=queue_size,
                                             rate=rate)
            self._subscribers.append(sub)

    def _release_block(self):
        self._current_priority = len(self._mux_spec)
        logging.debug("Lockout ended.")

    def _callback_closure(self, priority, priority_name, blocking_time):
        def priority_callback(topic, msg: b9py.Message):
            # Same or higher priority
            if priority <= self._current_priority:
                self._current_priority = priority

                # Make the subscription callback
                if not self._callback(topic, msg, priority, priority_name):
                    if blocking_time >= 0.0:
                        self._locker.start(blocking_time)
                        logging.debug("Lockout started {}".format(blocking_time))
                else:
                    self._locker.cancel()
                    logging.debug("Lockout canceled.")

                # Publish submux synchronization message
                if self._synchro_pub:
                    sync_msg = b9py.MessageFactory.create_message_submux_sync(msg.data, priority, priority_name)
                    self._synchro_pub.publish(sync_msg)

            # Lower priority
            else:
                # Sorry, no callback for you! Your message will be ignored
                logging.debug("Lockout enforced for {}".format(topic))

        return priority_callback

    def publish_sync_msg(self, msg_data, priority_name):
        if self.has_synchronization_publisher:
            sync_msg = b9py.MessageFactory.create_message_submux_sync(msg_data, -1, priority_name)
            self._synchro_pub.publish(sync_msg)

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

    @property
    def has_synchronization_publisher(self):
        return self._synchro_pub is not None
