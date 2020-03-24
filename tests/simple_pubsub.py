#!/usr/bin/env python3.7

import os
import signal
import b9py
import logging


def subscriber1_cb(topic, message: b9py.Message):
    print("Subscriber 1 - topic: {}, {}".format(topic, message))


def subscriber2_cb(topic, message: b9py.Message):
    print("Subscriber 2 - topic: {}, {}".format(topic, message))


def create_publisher(topic, message_type, namespace, rate=10, queue_size=8):
    new_pub = b9.create_publisher(topic, message_type, namespace, rate, queue_size)
    stat = new_pub.advertise()
    if stat.is_successful:
        return new_pub
    else:
        os.kill(os.getpid(), signal.SIGKILL)


def create_subscriber(topic, callback, namespace, rate=10, queue_size=8, port=None):
    new_sub = b9.create_subscriber(topic, callback, namespace, rate, queue_size, port)
    new_sub.subscribe()
    return new_sub


def publish(publisher, message_type, msg_data):
    msg = b9py.Message(message_type, msg_data)
    publisher.publish(msg)


b9 = b9py.B9('test_pubsub_node')
b9.start_logger(level=logging.DEBUG)

print("Create subscriber for 'test/topic1'...")
sub1 = b9.create_subscriber('test/topic1', subscriber1_cb, None, 1000, 8, None)

print("Create subscriber for 'test/topic2'...")
sub2 = b9.create_subscriber('test/topic2', subscriber2_cb, None, 1000, 8, None)

b9.spin_forever()

# while b9.spin_once():
#    pass
