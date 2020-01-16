import os
import signal
import b9py
import logging
import argparse


def adder_cb(topic, message: b9py.Message):
    if not args['silent']:
        print("Service: {}, message = {}".format(topic, message))
    return b9py.Message('Int', message.data[0] + message.data[1])


print("\nB9 Adder Service")

ap = argparse.ArgumentParser()
ap.add_argument("-s", "--silent", action="store_true", help="do not display service call messages.")

args = vars(ap.parse_args())

master_uri = os.environ.get('B9_MASTER')
if master_uri is None:
    master_uri = 'localhost'
    print("B9 master URI is unspecified. Using localhost.")

# Init B9 core
b9 = b9py.B9('test_service_node', master_uri)
b9.start_logger(level=logging.DEBUG)

# Setup an adder service
srv = b9.create_service('test/adder', b9py.Message.MSGTYPE_LIST, adder_cb, 'N99')
stat = srv.advertise()
if not stat.is_successful:
    os.kill(os.getpid(), signal.SIGKILL)

print("\nProviding an adder service at '{}'...".format(srv.topic))

b9.spin_forever()
