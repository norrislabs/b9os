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

if b9.is_localhost:
    print("Using master at localhost.")
else:
    print("Using master at {}".format(b9.master_uri))
print("Press Ctrl-C to exit.")

pub1 = create_publisher('test/topic1', b9py.Message.MSGTYPE_ANY, None)
pub2 = create_publisher('test/topic2', b9py.Message.MSGTYPE_ANY, 'N53')

sub1 = create_subscriber('test/topic1', subscriber1_cb, None, 1000, 8)
sub2 = create_subscriber('test/topic2', subscriber2_cb, 'N53', 1000, 8)

publish(pub1, 'String', "this is test 1")
publish(pub1, 'Int', 16)
publish(pub1, 'Bool', True)
publish(pub1, 'List', [1, 2, 3])
publish(pub1, 'Dict', {'A': 1, 'B': 2, 'C': 3})
publish(pub1, 'Bytes', bytes('test1'.encode()))

publish(pub2, 'String', "this is test 2")
publish(pub2, 'Int', 17)
publish(pub2, 'Bool', False)
publish(pub2, 'List', [4, 5, 6])
publish(pub2, 'Dict', {'D': 4, 'E': 5, 'F': 6})
publish(pub2, 'Bytes', bytes('test2'.encode()))

b9.spin_forever()
