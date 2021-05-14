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


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="subscriber", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="topic namespace")
    ap.add_argument("-t", "--topic", type=str, default="test/topic", help="topic")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.DEBUG)

    mux_spec = [(2, 10), (2, 10), (2, 2)]
    mux = b9.create_submux(args['topic'], sub_callback, mux_spec, args['namespace'])
    mux.subscribe()

    b9.spin_forever()
