import os
import logging
import xmltodict

import b9py


class Tuner(object):
    def __init__(self, b9core: b9py.B9, namespace, tuner_name, tuner_cb, loader_cb=None):
        self._b9 = b9core
        self._namespace = namespace
        self._nodename = self._b9.nodename
        self._tuner_name = tuner_name

        # Topic for tuning values subscriber and tuner spec loader service
        self._topic = "tuners/" + self._tuner_name

        self._tuners_dir = "configuration"
        self._tuners_ext = "tune"

        # Load tuner spec from this file if no loader callback specified
        self._spec_file = "{}}/" + self._tuner_name + ".{}".format(self._tuners_dir, self._tuners_ext)

        # Setup tuner spec loader service
        # B9tuner will call this service to get the tuners spec
        self._start_srv = self._b9.create_service(self._topic,
                                                  b9py.Message.MSGTYPE_NULL,
                                                  loader_cb if loader_cb else self._load_file_spec_cb,
                                                  self._namespace,
                                                  ret_msg_type=b9py.Message.MSGTYPE_DICT)
        stat = self._start_srv.advertise()
        if not stat.is_successful:
            logging.error("Load tuners spec service for node '{}' failed to advertise.".format(self._nodename))
            return

        # Setup tuner subscriber (tune values from B9tuner are published to this topic)
        self._status_sub = self._b9.create_subscriber(self._topic,
                                                      self._build_tune_values_cb(tuner_cb),
                                                      self._namespace)

    @staticmethod
    def _build_tune_values_cb(tuner_cb):
        def new_cb(topic, msg: b9py.Message):
            logging.info(msg.data)
            tuner_cb(topic, msg)
        return new_cb

    def _load_file_spec_cb(self, _topic, _msg: b9py.Message):
        # Callback to load tuners spec from a file
        # Make sure the specifed tuners spec file is available
        if not os.path.exists(self._spec_file):
            logging.error("Cannot find tuners spec file '{}' for node '{}'.".format(self._spec_file, self._nodename))
            return

        # Open the tuner spec file and read the contents
        with open(self._spec_file, 'r', encoding='utf-8') as file:
            exp_xml = file.read()

        # Parse the XML configuration document
        tuners_spec = xmltodict.parse(exp_xml)
        return b9py.MessageFactory.create_message_dictionary(tuners_spec)
