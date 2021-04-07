#!/usr/bin/env python3

import os
import signal
import logging
import argparse
import b9py


# Control-C handler
def shutdown_handler(_sig, _frame):
    os.kill(os.getpid(), signal.SIGKILL)


def service_cb(_topic, _message):
    return b9py.MessageFactory.create_message_int(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="service", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="topic namespace")
    ap.add_argument("-t", "--topic", type=str, default="test/service", help="topic")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.INFO)

    # Setup service
    srv = b9.create_service(args['topic'], b9py.Message.MSGTYPE_ANY, service_cb, args['namespace'])
    stat = srv.advertise()
    if not stat.is_successful:
        print("Service failed to advertise.")
        os.kill(os.getpid(), signal.SIGKILL)

    b9.spin_forever()
