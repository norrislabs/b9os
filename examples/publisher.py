#!/usr/bin/env python3.7

import os
import signal
import logging
import argparse
import b9py


# Control-C handler
def shutdown_handler(_sig, _frame):
    os.kill(os.getpid(), signal.SIGKILL)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="publisher", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="topic namespace")
    ap.add_argument("-t", "--topic", type=str, default="test/topic", help="topic")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.INFO)

    # Setup publisher
    pub = b9.create_publisher(args['topic'], b9py.Message.MSGTYPE_INT, args['namespace'])
    stat = pub.advertise()
    if not stat.is_successful:
        print("Publisher failed to advertise.")
        os.kill(os.getpid(), signal.SIGKILL)

    b9.spin_forever()
