#!/usr/bin/env python3
import os
import signal
import logging

import b9py


def read_frame_cb():
    image_data = bytes(90000)
    msg = b9py.MessageFactory.create_message_image(image_data, pub.name)
#    print(len(image_data))
    pub.publish(msg)


b9 = b9py.B9('timer_test')
b9.start_logger(level=logging.DEBUG)

pub = b9.create_publisher("test/timer", b9py.Message.MSGTYPE_IMAGE)
stat = pub.advertise()
if not stat.is_successful:
    logging.error("Publisher failed. {}".format(stat.status_type))
    os.kill(os.getpid(), signal.SIGKILL)

timer1 = b9.create_timer(1.0 / float(30), read_frame_cb)

b9.spin_forever()
