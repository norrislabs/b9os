import os
import time
import uuid
import signal
import socket
import asyncio
import logging
import colorlog
from colorama import Fore
from netifaces import interfaces, ifaddresses, AF_INET

import b9py
import b9py.message


class B9(object):
    def __init__(self, nodename, broker_uri=None):
        self._nodename = nodename
        self._is_broker = self._nodename.lower() == "broker"
        self._broker_uri = broker_uri

        self._logger = None

        self._init_success = True
        self._init_status = "'{}' is initialized and registered.".format(self._nodename)

        self._host_ip, self._hostname = self.get_ip_hostname()
        self._pid = os.getpid()

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sigs in signals:
            loop.add_signal_handler(sigs, lambda sx=sigs: asyncio.create_task(self._shutdown()))

        self._publishers = []
        self._subscribers = []
        self._have_all_publishers = False
        self._muxes = []

        self._spin_delay = -1.0
        self._check_sub_count = 0
        self._current_sub_count = 0

        # Set a default broker URI if not already specified, and we are not the broker
        if self._broker_uri is None and not self._is_broker:
            # Lookup broker URI from the official environment variable
            self._broker_uri = os.environ.get('B9_BROKER')
            if self._broker_uri is None:
                # Still not specified so we have to assume localhost
                logging.warning("B9 broker URI was not specified. Using localhost.")
                self._broker_uri = 'localhost'

        # Register this node if not the broker and not a CLI tool
        if not self._is_broker:
            if self._nodename.endswith('_cli'):
                self._init_status = ""
            else:
                result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                                 'broker/registration/node',
                                                                 None,
                                                                 self._create_node_reg_message(),
                                                                 5557, self._broker_uri)
                if not result.is_successful:
                    # Registration failed
                    self._init_success = False
                    self._init_status = "Node registration '{}' failed. {}".format(self._nodename,
                                                                                   result.status_type)

    @staticmethod
    def prepend_namespace(namespace, topic):
        if namespace:
            if namespace == '/':
                # Topic is in the bare root namespace
                return namespace + topic.strip('/')
            else:
                # Topic is in root specified namespace
                _namespace = namespace.strip('/')
                return '/' + _namespace + '/' + topic.strip('/')
        else:
            # No namespace - use topic as is
            return topic

    @staticmethod
    def get_ip_hostname():
        ip_list = []
        # Get all the IPs of this machine
        for ifaceName in interfaces():
            addresses = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr': '0.0.0.0'}])]
            ip_list.append(' '.join(addresses))

        # Find the max network id (>= 192.168.48) in list of IPs. Assume it is the robot's network id
        net_ids = list(map(lambda ip: int(ip.split('.')[2]), ip_list))
        max_index = net_ids.index(max(net_ids))
        return ip_list[max_index], socket.gethostname()

    def _create_node_reg_message(self):
        return b9py.Message(b9py.Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'REGISTER',
                                                                     'timestamp': time.time(),
                                                                     'nodename': self._nodename,
                                                                     'IP': self._host_ip,
                                                                     'pid': os.getpid(),
                                                                     'host': self._hostname},
                            self._nodename)

    def start_logger(self, level=logging.INFO, filename=None):
        logging.basicConfig(level=level,
                            filename=filename,
                            filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s\r')

        # Logging to file so setup logging to the console as well
        if filename is not None:
            console = logging.StreamHandler()
            console.setLevel(level)
            formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s\r')
            console.setFormatter(formatter)
            logging.getLogger("").addHandler(console)
        self._logger = logging

        logging.info("B9 OS version: {}".format(b9py.__version__))
        logging.info("B9 Logger has started.")
        logging.info("'{}' is running on {} at {}".format(self._nodename, self._hostname, self._host_ip))
        if not self._is_broker:
            logging.info("'{}' is using broker at {}".format(self._nodename, self._broker_uri))
        logging.info("'{}' process ID (PID) is {}".format(self._nodename, self._pid))
        if len(self._init_status) > 0:
            if self._init_success:
                logging.info(self._init_status)
            else:
                logging.error(self._init_status)

    # Improved logger. Now in Color!
    def start_logger2(self, level=logging.INFO, filename=None):
        logging.basicConfig(level=level,
                            filename=filename,
                            filemode='w')

        if filename is not None:
            console = logging.FileHandler(filename)
            console.setLevel(level)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s\r')
            console.setFormatter(formatter)

            self._logger = logging.getLogger()
            self._logger.handlers = []
            self._logger.addHandler(console)

        # Create a custom formatter that uses colors
        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s',
            log_colors={
                'DEBUG': 'white',
                'INFO': 'green',
                'WARNING': 'bold_yellow',
                'ERROR': 'bold_red',
                'CRITICAL': 'red,bg_white',
            },
            secondary_log_colors={},
            style='%'
        )

        if filename is None:
            self._logger = logging.getLogger()
            self._logger.handlers = []

        # Create a logger and add a StreamHandler with the colored formatter
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        self._logger.info("B9 OS version: {}".format(b9py.__version__))
        self._logger.info("B9 Logger has started.")
        self._logger.info("'{}' is running on {} at {}".format(self._nodename, self._hostname, self._host_ip))
        if not self._is_broker:
            self._logger.info("'{}' is using broker at {}".format(self._nodename, self._broker_uri))
        self._logger.info("'{}' process ID (PID) is {}".format(self._nodename, self._pid))
        if len(self._init_status) > 0:
            if self._init_success:
                self._logger.info(self._init_status)
            else:
                self._logger.error(self._init_status)

    @staticmethod
    def _get_b9_directory(env_name, dir_name):
        b9_dir = None
        if env_name in os.environ:
            b9_dir = os.environ[env_name]
        elif os.path.basename(os.getcwd()).startswith(dir_name):
            b9_dir = os.getcwd()
        elif os.path.isdir('launch'):
            b9_dir = os.getcwd()
        return b9_dir

    @staticmethod
    def get_workspace_directory():
        return B9._get_b9_directory("B9_DIR_WORKSPACE", "b9ws")

    @staticmethod
    def get_os_directory():
        return B9._get_b9_directory("B9_DIR_OS", "b9os")

    @property
    def publishers(self):
        return self._publishers

    @property
    def subscribers(self):
        return self._subscribers

    @property
    def nodename(self):
        return self._nodename

    @property
    def pid(self):
        return self._pid

    @property
    def is_broker(self):
        return self._is_broker

    @property
    def hostname(self):
        return self._hostname

    @property
    def hostip(self):
        return self._host_ip

    @property
    def broker_uri(self):
        return self._broker_uri

    @property
    def have_all_publishers(self):
        return self._have_all_publishers

    @property
    def is_localhost(self):
        return self._broker_uri == "localhost" or self._broker_uri.startswith("127.0")

    @property
    def logger(self) -> logging.RootLogger:
        return self._logger

    @staticmethod
    async def _shutdown():
        """Cleanup all publisher/subscriber tasks"""
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        await asyncio.gather(*tasks)
        asyncio.get_event_loop().stop()

    def shutdown(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._shutdown())
        logging.info("B9 core shutdown.")

    @staticmethod
    def _all_have_publishers(subscribers):
        for subscriber in subscribers:
            if not subscriber.have_publisher:
                return False
        return True

    def spin_forever(self, delay=0.01, quiet=True):
        while self.spin_once(delay, quiet):
            pass
        return False

    def spin_once(self, delay=0.01, quiet=True):
        if delay != self._spin_delay:
            self._check_sub_count = int(1 / delay)
            self._current_sub_count = self._check_sub_count
            self._spin_delay = delay

        try:
            if len(self._subscribers) > 0:
                if not self._all_have_publishers(self._subscribers):
                    # Try subscribing once a second...
                    if self._current_sub_count >= self._check_sub_count:
                        self._current_sub_count = 0
                        for sub in self._subscribers:
                            if not sub.have_publisher:
                                sub.subscribe(quiet=quiet)
                else:
                    if not self._have_all_publishers:
                        logging.info("{} - All subscribers have publishers.".format(self._nodename))
                        self._have_all_publishers = True
            else:
                self._have_all_publishers = True

            asyncio.get_event_loop().run_until_complete(B9._spin_once(delay))
            self._current_sub_count += 1
            return True

        except asyncio.CancelledError:
            return False

    @staticmethod
    async def _spin_once(delay):
        await asyncio.sleep(delay)

    @staticmethod
    async def _periodic(node_name, interval, callback):
        _timer_name = "TMR-{}-{}".format(node_name, str(uuid.uuid1()).split('-')[0])
        try:
            while True:
                await asyncio.sleep(interval)
                callback()
        except asyncio.CancelledError:
            print("'" + _timer_name + "' timer task of has been canceled.", end="\r\n")

    def create_timer(self, interval, callback):
        return asyncio.get_event_loop().create_task(self._periodic(self._nodename, interval, callback))

    def create_publisher(self, topic, message_type, namespace=None, rate=100, queue_size=-1):
        # nodename, broker_uri, topic, message_type, namespace, rate, queue_size
        pub = b9py.Publisher(self._nodename, self._broker_uri,
                             topic, message_type, namespace,
                             rate, queue_size,
                             self._host_ip, self._hostname)
        self._publishers.append(pub)
        print(Fore.CYAN + "Publisher in node '{}' for topic '{}' has been created.".format(self._nodename,
                                                                                           topic), end='')
        print(Fore.RESET)
        return pub

    def create_subscriber(self, topic, callback, namespace=None, rate=-1, queue_size=-1,
                          message_type=b9py.message.Message.MSGTYPE_ANY):
        sub = b9py.Subscriber(self._nodename, self._broker_uri,
                              topic, message_type, callback, namespace,
                              rate, queue_size,
                              self._host_ip, self._hostname)
        self._subscribers.append(sub)
        print(Fore.CYAN + "Subscriber in node '{}' for topic '{}' has been created.".format(self._nodename,
                                                                                            topic), end='')
        print(Fore.RESET)
        return sub

    def create_submux(self, topic, namespace, callback, mux_spec, synchro_pub=True):
        # node_name, broker_uri, topic, callback, mux_spec
        mux = b9py.SubscriberMultiplexer(self, topic, namespace, callback, mux_spec, synchro_pub)
        self._muxes.append(mux)
        print(Fore.CYAN + "Multiplexer in node '{}' for topic '{}' has been created.".format(self._nodename,
                                                                                             topic), end='')
        print(Fore.RESET)
        return mux

    def create_service(self, topic, call_msg_type, callback, namespace=None, port=None,
                       ret_msg_type=b9py.message.Message.MSGTYPE_ANY):
        # node_name, broker_uri, topic, message_type, callback, port, this_host_ip
        srv = b9py.Service(self._nodename, self._broker_uri,
                           topic, call_msg_type,
                           callback, namespace, port, self._host_ip, self._hostname, ret_msg_type)
        print(Fore.CYAN + "Service in node '{}' for topic '{}' has been created.".format(self._nodename,
                                                                                         topic), end='')
        print(Fore.RESET)
        return srv

    def create_service_client(self, topic, namespace=None, srv_port=None, srv_host=None):
        # node_name, broker_uri, topic, srv_port, srv_host
        return b9py.ServiceClient(self._nodename, self._broker_uri, topic,
                                  namespace, srv_port, srv_host)

    def create_parameter_client(self, nodename, namespace=None, parameter_topic=None):
        return b9py.Parameter(self.broker_uri, nodename, namespace, parameter_topic)

    def create_action_server(self, topic, start_cb, stop_cb, pause_cb=None, namespace=None):
        return b9py.ActionServer(self, topic, namespace, start_cb, stop_cb, pause_cb)

    def create_action_client(self, topic, status_cb=None, namespace=None):
        return b9py.ActionClient(self, topic, namespace, status_cb)


class B9Status(object):
    OK = 'OK'
    FAILED = "FAILED"
    ERR_TIMEOUT = 'ERR_TIMEOUT'
    ERR_CONN_REFUSED = 'ERR_CONN_REFUSED'
    ERR_TOPIC_NOTFOUND = 'ERR_TOPIC_NOTFOUND'
    ERR_NOBROKER = 'ERR_NOBROKER'
    ERR_QUEUEFULL = 'ERR_QUEUEFULL'
    ERR_WRONG_MESSAGE = 'ERR_WRONG_MESSAGE'
    ERR_CANCELED = 'ERR_CANCELED'
    ERR_NOT_IMPLEMENTED = "ERR_NOT_IMPLEMENTED"

    def __init__(self, success, status_type=None, status_text=None, result_data=None):
        self._success = success
        self._status_type = status_type
        self._status_text = status_text
        self._result_data = result_data

        if status_type is None:
            if self._success:
                self._status_type = B9Status.OK
            else:
                self._status_type = B9Status.FAILED

    @staticmethod
    def success_status(data=None):
        return B9Status(True, None, None, data)

    @staticmethod
    def failed_status(result_type=FAILED, status_text=None, result_data=None):
        return B9Status(False, result_type, status_text, result_data=result_data)

    @property
    def is_successful(self):
        return self._success

    @property
    def status_type(self):
        return self._status_type

    @property
    def message(self):
        if self._status_text is None:
            if self._success:
                return "Success"
            else:
                return "Failed with error '{}'".format(str(self._status_type))
        else:
            return self._status_text

    @property
    def result_data(self):
        return self._result_data

    def __str__(self):
        return "B9Status: {}".format(self.message)
