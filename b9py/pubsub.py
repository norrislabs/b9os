import time
import uuid
import asyncio
import zmq.asyncio
import logging

import b9py


class Publisher(object):
    def __init__(self, nodename, master_uri, topic, message_type, namespace, rate, queue_size,
                 this_host_ip, this_host_name):
        self._node_name = nodename
        self._master_uri = master_uri

        self._namespace = namespace
        if namespace:
            if namespace == '/':
                # Topic is in the bare root namespace
                self._topic = self._namespace + topic.strip('/')
            else:
                # Topic is in root specified namespace
                self._namespace = namespace.strip('/')
                self._topic = '/' + self._namespace + '/' + topic.strip('/')
        else:
            # No namespace - use topic as is
            self._topic = topic

        self._message_type = message_type
        self._this_host_ip = this_host_ip
        self._this_host_name = this_host_name

        # Increased rate by 1.22 to compensate for processing and network
        self._pub_rate = rate
        self._pub_interval = (1000.0 / (self._pub_rate * 1.22)) * .001

        # Setup the asyncio Context thing
        self._ctx = zmq.asyncio.Context()
        self._pub_sock = self._ctx.socket(zmq.PUB)  # Note: adds 2 additional threads
        self._port = None

        self._pub_name = "PUB-{}-{}".format(self._node_name, self._topic)

        # Outgoing message queue
        self._queue = asyncio.Queue(maxsize=queue_size)

    def advertise(self):
        # Register this topic publisher with the Master Topic Name Service
        if self._master_uri is not None:
            # Lookup to see if already registered
            result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                             'master/registration/topic',
                                                             None,
                                                             self._create_req_lookup_message(self._topic),
                                                             5555, self._master_uri)

            if result.is_successful and result.result_data.data['found']:
                # Reuse the port number registered before
                self._port = result.result_data.data['port']

                try:
                    self._pub_sock.bind('tcp://*:{}'.format(self._port))
                except zmq.error.ZMQError as ex:
                    err_msg = "'{}' on node '{}' failed. {}".format(self._pub_name, self._node_name,
                                                                    ex.strerror)
                    logging.error(err_msg)
                    return b9py.B9Status.failed_status(b9py.B9Status.ERR_NOMASTER, err_msg)

                logging.info("'{}' is reusing the port {}".format(self._pub_name, self._port))
            else:
                self._port = self._pub_sock.bind_to_random_port('tcp://*')
                logging.info("'{}' is using the port {}".format(self._pub_name, self._port))

            # Register
            result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                             'master/registration/topic',
                                                             None,
                                                             self._create_pub_reg_message(),
                                                             5555, self._master_uri)
            if not result.is_successful:
                # Service call failed
                logging.error("'{}' on node '{}' failed. {}".format(self._pub_name, self._node_name,
                                                                    result.status_type))
                return result
        else:
            # No ability to subscribe. Give up.
            err_msg = "'{}' on node '{}' failed. {}".format(self._pub_name, self._node_name,
                                                            b9py.B9Status.ERR_NOMASTER)
            logging.error(err_msg)
            return b9py.B9Status.failed_status(b9py.B9Status.ERR_NOMASTER, err_msg)

        # Activate publisher
        loop = asyncio.get_event_loop()
        loop.create_task(self._pub_task())

        # Log and return success
        logging.info("'{}' advertised topic '{}' on node '{}'.".format(self._pub_name, self._topic,
                                                                       self._node_name))
        return b9py.B9Status.success_status()

    def _create_pub_reg_message(self):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION,
                            {'cmd': 'REGISTER', 'sub_cmd': 'PUB',
                             'topic': self._topic, 'message_type': self._message_type,
                             'nodename': self._node_name,
                             'IP': self._this_host_ip, 'port': self._port,
                             'this_ip': self._this_host_ip,
                             'this_host': self._this_host_name},
                            self._node_name)

    def _create_req_lookup_message(self, lookup_topic):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LOOKUP', 'sub_cmd': 'PUB',
                                                                      'topic': lookup_topic},
                            self._node_name)

    @property
    def name(self):
        return self._pub_name

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def port(self):
        return self._port

    def empty(self):
        return self._queue.empty()

    def publish(self, message):
        if self._message_type is None or message.message_type == self._message_type:
            try:
                self._queue.put_nowait(message)
                return b9py.B9Status.success_status()

            except asyncio.QueueFull:
                # Queue is full
                # Pop oldest off and put the new message on
                self._queue.get_nowait()
                self._queue.put_nowait(message)

                err_msg = "'{}' on node '{}' failed. {}".format(self._pub_name, self._node_name,
                                                                b9py.B9Status.ERR_QUEUEFULL)
                logging.debug(err_msg)
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_QUEUEFULL, err_msg)
        else:
            err_msg = "'{}' on node '{}'. Incoming {} != {}. {}".format(self._pub_name, self._node_name,
                                                                        message.message_type, self._message_type,
                                                                        b9py.B9Status.ERR_WRONG_MESSAGE)
            logging.error(err_msg)
            return b9py.B9Status.failed_status(b9py.B9Status.ERR_WRONG_MESSAGE, err_msg)

    def publish_wait(self, message):
        # Publish message
        self.publish(message)

        # Now wait for it to get on its way
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._wait_pub())

    async def _wait_pub(self):
        try:
            while True:
                await asyncio.sleep(0.02)  # 50 Hz

                # Return when the queue is empty
                if self._queue.qsize() == 0:
                    return
        except asyncio.CancelledError:
            logging.debug("'" + self._pub_name + "' publish_wait task of has been canceled.")

    async def _pub_task(self):
        # Give time for subscribers to initialize
        await asyncio.sleep(1)

        try:
            while True:
                # Regulate publishing rate
                if self._pub_interval > 0:
                    await asyncio.sleep(self._pub_interval)
                else:
                    await asyncio.sleep(0.02)  # 50 Hz

                # Publish message to topic if any in the queue
                if self._queue.qsize() > 0:
                    msg_q = await self._queue.get()
                    msg_q.timestamp = time.time()
                    if msg_q.source is None or len(msg_q.source.strip()) == 0:
                        msg_q.source = self._node_name
                    self._pub_sock.send_multipart([self._topic.encode('utf-8'), msg_q.pack()], zmq.DONTWAIT)

        except asyncio.CancelledError:
            logging.debug("'" + self._pub_name + "' publish task of has been canceled.")

        finally:
            self._pub_sock.close()


class Subscriber(object):
    def __init__(self, nodename, master_uri, topic, callback, namespace, rate, queue_size,
                 this_host_ip, this_host_name, pub_port, pub_host):
        self._node_name = nodename
        self._callback = callback

        self._namespace = namespace
        if namespace:
            if namespace == '/':
                self._topic = self._namespace + topic.strip('/')
            else:
                self._namespace = namespace.strip('/')
                self._topic = '/' + self._namespace + '/' + topic.strip('/')
        else:
            self._topic = topic

        self._sub_rate = rate
        self._sub_interval = (1000.0 / self._sub_rate) * .001

        self._this_host_ip = this_host_ip
        self._this_host_name = this_host_name

        self._master_uri = master_uri
        self._pub_host = pub_host
        self._pub_port = pub_port
        self._pub_uri = None
        self._message_type = None

        self._sub_name = "SUB-{}-{}-{}".format(self._node_name,
                                               self._topic,
                                               str(uuid.uuid1()).split('-')[0])

        self._ctx = zmq.asyncio.Context()
        self._sub_sock = None

        self._task_sub = None
        self._task_msg = None

        # Use localhost if publisher host not specified
        if self._pub_host is None:
            self._pub_host = "localhost"

        # Incoming message queue
        self._queue = asyncio.Queue(maxsize=queue_size)

    def subscribe(self, quiet=False):
        sub_reg_msg = None

        self._sub_sock = self._ctx.socket(zmq.SUB)
        self._sub_sock.setsockopt_string(zmq.SUBSCRIBE, self._topic)

        # Use port if specified
        if self._pub_port is not None:
            self._pub_uri = "tcp://{}:{}".format(self._pub_host, self._pub_port)
        else:
            # Otherwise, lookup the publisher's URI using the topic
            if self._master_uri is not None:
                result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                                 'master/registration/topic',
                                                                 None,
                                                                 self._create_req_lookup_message(self._topic),
                                                                 5555, self._master_uri)
                if result.is_successful:
                    # Set the publisher's URI so we can connect
                    if result.result_data.data['found']:
                        self._pub_uri = "tcp://{}:{}".format(result.result_data.data['IP'],
                                                             result.result_data.data['port'])
                        self._message_type = result.result_data.data['message_type']
                    else:
                        # Unknown topic, not registered with master
                        err_msg = "'{}' on node '{}' failed. Topic '{}' {}".format(self._sub_name,
                                                                                   self._node_name,
                                                                                   self._topic,
                                                                                   b9py.B9Status.ERR_TOPIC_NOTFOUND)
                        if not quiet:
                            logging.warning(err_msg)
                        return b9py.B9Status.failed_status(b9py.B9Status.ERR_TOPIC_NOTFOUND, err_msg)

                    # Build subscriber registration message
                    sub_reg_msg = b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION,
                                               {'cmd': 'REGISTER', 'sub_cmd': 'SUB',
                                                'topic': self._topic,
                                                'message_type': result.result_data.data['message_type'],
                                                'nodename': self._node_name,
                                                'IP': result.result_data.data['IP'],
                                                'port': result.result_data.data['port'],
                                                'this_ip': self._this_host_ip,
                                                'this_host': self._this_host_name},
                                               self._node_name)

                else:
                    # Service call failed
                    logging.error("'{}' on node '{}' failed. {}".format(self._sub_name,
                                                                        self._node_name,
                                                                        result.status_type))
                    return result
            else:
                # No ability to subscribe. Give up.
                err_msg = "'{}' on node '{}' failed. {}".format(self._sub_name,
                                                                self._node_name,
                                                                b9py.B9Status.ERR_NOMASTER)
                logging.error(err_msg)
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_NOMASTER, err_msg)

        # Connect to publisher
        self._sub_sock.connect(self._pub_uri)

        # Register the subscription
        result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                         'master/registration/topic',
                                                         None,
                                                         sub_reg_msg,
                                                         5555, self._master_uri)
        if result.is_successful:
            # Activate subscriber
            loop = asyncio.get_event_loop()
            self._task_sub = loop.create_task(self._sub_task())
            self._task_msg = loop.create_task(self._message_task())

            # Log and return success
            logging.info("'{}' subscribed to topic '{}'.".format(self._sub_name, self._topic))
            logging.info("'{}' connected to publisher at {}".format(self._sub_name, self._pub_uri))
            return result

        else:
            # Subscription registration failed.
            err_msg = "Subscription registration for '{}' on node '{}' failed.".format(self._topic, self._node_name)
            logging.error(err_msg)
            return b9py.B9Status.failed_status(b9py.B9Status.FAILED, err_msg)

    def subscribe_wait(self, count=10000, delay=2):
        r = self.subscribe()

        for retry in range(count):
            if r.is_successful:
                break

            if r.status_type == b9py.B9Status.ERR_TOPIC_NOTFOUND:
                time.sleep(delay)
                self._reset()
                r = self.subscribe(quiet=True)
            else:
                break

        return r

    def _reset(self):
        if self._task_sub is not None:
            self._task_sub.cancel()
            self._task_sub = None

        if self._task_msg is not None:
            self._task_msg.cancel()
            self._task_msg = None

        if self._sub_sock is not None:
            self._sub_sock.unsubscribe(self._topic)
            self._sub_sock.close()
            self._sub_sock = None

    def _create_req_lookup_message(self, lookup_topic):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION,
                            {'cmd': 'LOOKUP', 'sub_cmd': 'PUB',
                             'topic': lookup_topic},
                            self._node_name)

    def _create_sub_reg_message(self, sub_topic, ip, port):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION,
                            {'cmd': 'REGISTER', 'sub_cmd': 'SUB', 'topic': sub_topic,
                             'message_type': b9py.Message.MSGTYPE_ANY,
                             'nodename': self._node_name, 'IP': ip, 'port': port},
                            self._node_name)

    @property
    def name(self):
        return self._sub_name

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def publisher_uri(self):
        return self._pub_uri

    async def _sub_task(self):
        try:
            # Keep listening for published messages on topic
            while True:
                [topic, msg_data] = await self._sub_sock.recv_multipart()
                msg_unpacked = b9py.Message.unpack(msg_data)

                if topic.decode("utf-8") != self._topic:
                    logging.error("Topic mismatch! {} != {}".format(topic.decode('utf-8'), self._topic))
                    continue

                if self._message_type is not None and msg_unpacked.message_type != self._message_type:
                    logging.error("Message type mismatch! {} != {}".format(msg_unpacked.message_type,
                                                                           self._message_type))
                    continue
                else:
                    try:
                        self._queue.put_nowait(msg_unpacked)

                    except asyncio.QueueFull:
                        # Queue is full
                        # Pop one off and put the new message on
                        self._queue.get_nowait()
                        self._queue.put_nowait(msg_unpacked)

                        err_msg = "'{}' on '{}' failed. First in queue lost. {}".format(self._sub_name,
                                                                                        self._node_name,
                                                                                        b9py.B9Status.ERR_QUEUEFULL)
                        logging.debug(err_msg)

        except asyncio.CancelledError:
            logging.debug("'" + self._sub_name + "' receive task has been canceled.")

        finally:
            self._sub_sock.close()
            self._ctx.destroy()

    async def _message_task(self):
        try:
            while True:
                # Regulate callback rate
                if self._sub_interval > 0:
                    await asyncio.sleep(self._sub_interval)
                else:
                    await asyncio.sleep(.02)

                # Callback with any new messages
                if self._queue.qsize() > 0:
                    msg_q = await self._queue.get()
                    self._callback(self._topic, msg_q)

        except asyncio.CancelledError:
            logging.debug("'" + self._sub_name + "' message task has been canceled.")
