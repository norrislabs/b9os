#!/usr/bin/env python3

import os
import signal
import logging
import argparse
import b9py


# Control-C handler
def shutdown_handler(_sig, _frame):
    os.kill(os.getpid(), signal.SIGKILL)


def sub_callback(topic, msg: b9py.Message):
    logging.info("{} = {}".format(topic, msg.data))
    if topic.endswith('1'):
        return True


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="test_submux", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="topic namespace")
    ap.add_argument("-t", "--topic", type=str, default="test/topic", help="base topic")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.DEBUG)

    mux_spec = [(10, 2, -1, None), (10, 2, -1, None), (2, 2, -1, None)]
    mux = b9.create_submux(args['topic'], args['namespace'], sub_callback, mux_spec)

    b9.spin_forever()
