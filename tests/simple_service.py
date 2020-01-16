import os
import signal
import b9py
import logging


def adder_cb(topic, message: b9py.Message):
    print("Call to service: {}, message = {}".format(topic, message))
    return b9py.Message('Int', message.data[0] + message.data[1])


def timer1_cb():
    print("timer1")


def timer2_cb():
    print("timer2")


# Init B9 core
b9 = b9py.B9('test_service_node', 'localhost')
b9.start_logger(level=logging.DEBUG)

# Setup a few timers just for the hell of it
# timer1 = b9.create_timer(1, timer1_cb)
# timer2 = b9.create_timer(2, timer2_cb)

# Setup an adder service
srv = b9.create_service('test/adder', b9py.Message.MSGTYPE_LIST, adder_cb, 'N88')
stat = srv.advertise()
if not stat.is_successful:
    os.kill(os.getpid(), signal.SIGKILL)

# Create a client for above service
client = b9.create_service_client('test/adder', 'N88')
stat = client.connect()
if not stat.is_successful:
    os.kill(os.getpid(), signal.SIGKILL)

# Try the service out!
# result = client.call_service(b9py.MessageFactory.create_message_list([1, 2]))
# print("Result: {}".format(result))

result = client.call_service(b9py.MessageFactory.create_message_list([40, 2]))
print("Result: {}".format(result.result_data))

b9.shutdown()
