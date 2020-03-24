import os
import time
import uuid
import signal
import socket
import asyncio
import logging

import b9py
import b9py.message


class B9(object):
    def __init__(self, nodename, master_uri=None):
        self._nodename = nodename
        self._is_master = self._nodename.lower() == "master"
        self._master_uri = master_uri

        self._init_success = True
        self._init_status = "'{}' is initialized and registered.".format(self._nodename)

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self._host_ip = s.getsockname()[0]
        self._hostname = socket.gethostname()
        self._pid = os.getpid()

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sigs in signals:
            loop.add_signal_handler(sigs, lambda sx=sigs: asyncio.create_task(self._shutdown()))

        self._subscribers = []
        self._have_all_publishers = False

        # Set a default master URI if not already specified and we are not the master
        if self._master_uri is None and not self._is_master:
            # Lookup master URI from the official environment variable
            self._master_uri = os.environ.get('B9_MASTER')
            if self._master_uri is None:
                # Still not specified so we have to assume localhost
                self._master_uri = 'localhost'

        # Register this node if not the master and not a CLI tool
        if not self._is_master:
            if self._nodename.endswith('_cli'):
                self._init_status = ""
            else:
                result = b9py.ServiceClient.oneshot_service_call(self._nodename,
                                                                 'master/registration/node',
                                                                 None,
                                                                 self._create_node_reg_message(),
                                                                 5557, self._master_uri)
                if not result.is_successful:
                    # Registration failed
                    self._init_success = False
                    self._init_status = "Node registration '{}' failed. {}".format(self._nodename,
                                                                                   result.status_type)

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

        logging.info("B9 version: {}".format(b9py.__version__))
        logging.info("B9 Logger has started.")
        logging.info("'{}' is running on {} at {}".format(self._nodename, self._hostname, self._host_ip))
        if not self._is_master:
            logging.info("'{}' is using master at {}".format(self._nodename, self._master_uri))
        logging.info("'{}' process ID (PID) is {}".format(self._nodename, self._pid))
        if len(self._init_status) > 0:
            if self._init_success:
                logging.info(self._init_status)
            else:
                logging.error(self._init_status)

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
    def nodename(self):
        return self._nodename

    @property
    def pid(self):
        return self._pid

    @property
    def is_master(self):
        return self._is_master

    @property
    def hostname(self):
        return self._hostname

    @property
    def hostip(self):
        return self._host_ip

    @property
    def master_uri(self):
        return self._master_uri

    @property
    def have_all_publishers(self):
        return self._have_all_publishers

    @property
    def is_localhost(self):
        return self._master_uri == "localhost" or self._master_uri.startswith("127.0")

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
        try:
            if len(self._subscribers) > 0 and self._have_all_publishers is False:
                while not self._all_have_publishers(self._subscribers):
                    for sub in self._subscribers:
                        if not sub.have_publisher:
                            sub.subscribe(quiet=quiet, retry_interval=0)
                        asyncio.get_event_loop().run_until_complete(B9._spin_once(delay))
            self._have_all_publishers = True
            logging.info("{} - All subscribers have publishers.".format(self._nodename))
            asyncio.get_event_loop().run_forever()
        except asyncio.CancelledError:
            return False

    def spin_once(self, delay=0.01, quiet=True):
        try:
            if len(self._subscribers) > 0:
                if not self._all_have_publishers(self._subscribers):
                    # Try subscribing...
                    for sub in self._subscribers:
                        if not sub.have_publisher:
                            sub.subscribe(quiet=quiet, retry_interval=0)
                else:
                    if not self._have_all_publishers:
                        logging.info("{} - All subscribers have publishers.".format(self._nodename))
                        self._have_all_publishers = True
            else:
                self._have_all_publishers = True
            asyncio.get_event_loop().run_until_complete(B9._spin_once(delay))
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
        # nodename, master_uri, topic, message_type, namespace, rate, queue_size
        return b9py.Publisher(self._nodename, self._master_uri,
                              topic, message_type, namespace,
                              rate, queue_size, self._host_ip, self._hostname)

    def create_subscriber(self, topic, callback, namespace=None, rate=-1, queue_size=-1, pub_port=None, pub_host=None):
        # node_name, master_uri, topic, callback, rate, queue_size, pub_port, pub_host
        sub = b9py.Subscriber(self._nodename, self._master_uri,
                              topic, callback, namespace,
                              rate, queue_size,
                              self._host_ip, self._hostname,
                              pub_port, pub_host,)
        self._subscribers.append(sub)
        return sub

    def create_service(self, topic, message_type, callback, namespace=None, port=None):
        # node_name, master_uri, topic, message_type, callback, port, this_host_ip
        return b9py.Service(self._nodename, self._master_uri,
                            topic, message_type,
                            callback, namespace, port, self._host_ip, self._hostname)

    def create_service_client(self, topic, namespace=None, srv_port=None, srv_host=None):
        # node_name, master_uri, topic, srv_port, srv_host
        return b9py.ServiceClient(self._nodename, self._master_uri,
                                  topic, namespace,
                                  srv_port, srv_host)


class B9Status(object):
    OK = 'OK'
    FAILED = "FAILED"
    ERR_TIMEOUT = 'ERR_TIMEOUT'
    ERR_CONN_REFUSED = 'ERR_CONN_REFUSED'
    ERR_TOPIC_NOTFOUND = 'ERR_TOPIC_NOTFOUND'
    ERR_NOMASTER = 'ERR_NOMASTER'
    ERR_QUEUEFULL = 'ERR_QUEUEFULL'
    ERR_WRONG_MESSAGE = 'ERR_WRONG_MESSAGE'
    ERR_CANCELED = 'ERR_CANCELED'

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
