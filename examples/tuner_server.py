#!/usr/bin python3
import os
import signal
import logging
import argparse

import b9py


# ----------------------------------------------------------------------
# Control-C handler
def shutdown_handler(_sig, _frame):
    logging.info("All done.")
    os.kill(os.getpid(), signal.SIGKILL)


# ----------------------------------------------------------------------
def tuner_cb(_topic, msg: b9py.Message):
    print(msg.data)


# ----------------------------------------------------------------------
def loader_cb(_topic, _msg: b9py.Message):
    result = {'tuners': {'@title': 'Test Tuners',
                         '@name': 'test_tuners',
                         'tuner': [{'@default': '70',
                                    '@end': '180',
                                    '@name': 'tuner1',
                                    '@start': '40',
                                    '@step': '1',
                                    '@title': 'Tuner 1'},
                                   {'@default': '0',
                                    '@end': '10',
                                    '@name': 'tuner2',
                                    '@start': '-10',
                                    '@step': '1',
                                    '@title': 'Tuner 2'}, ]}}
    return b9py.MessageFactory.create_message_dictionary(result)


# ----------------------------------------------------------------------
if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--nodename", type=str, default="test_tuners", help="node name")
    ap.add_argument("-s", "--namespace", type=str, default="", help="topic namespace")
    args = vars(ap.parse_args())

    b9 = b9py.B9(args['nodename'])
    b9.start_logger(level=logging.INFO)

    # Setup tracking tuners
    tuner = b9py.Tuner(b9, args['namespace'], "test_tuners", tuner_cb, loader_cb)

    b9.spin_forever()
