import uuid
import asyncio
import zmq.asyncio
import logging

import b9py


class Service(object):
    def __init__(self, node_name, master_uri, topic, message_type, callback, namespace, port,
                 this_host_ip, this_host_name):
        self._node_name = node_name
        self._master_uri = master_uri
        self._is_master = self._node_name.lower() == "master"

        self._message_type = message_type
        self._callback = callback
        self._this_host_ip = this_host_ip
        self._this_host_name = this_host_name

        self._namespace = namespace
        if namespace:
            if namespace == '/':
                self._topic = self._namespace + topic.strip('/')
            else:
                self._namespace = namespace.strip('/')
                self._topic = '/' + self._namespace + '/' + topic.strip('/')
        else:
            self._topic = topic

        self._ctx = zmq.asyncio.Context()
        self._rep_sock = self._ctx.socket(zmq.REP)

        # Get a free port for this service if not specified
        if port is None:
            self._port = self._rep_sock.bind_to_random_port('tcp://*')
        else:
            self._port = port
            self._rep_sock.bind("tcp://*:{}".format(self._port))

        self._service_name = "SRV-{}-{}-{}".format(self._node_name, self._topic, self._port)

    def advertise(self):
        # Register this service with the Master Topic Name Service
        if self._master_uri is not None and not self._is_master:
            result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                             'master/registration/topic',
                                                             None,
                                                             self._create_srv_reg_message(),
                                                             5555, self._master_uri)
            if not result.is_successful:
                # Service call failed
                logging.error("'{}' on node '{}' failed. {}".format(self._service_name, self._node_name,
                                                                    result.status_type))
                return result
            else:
                # Log and return success
                logging.info("'{}' advertised topic '{}' on node '{}'.".format(self._service_name, self._topic,
                                                                               self._node_name))

        elif not self._is_master:
            # No ability to advertise. Give up.
            err_msg = "'{}' on node '{}' failed. {}".format(self._service_name, self._node_name,
                                                            b9py.B9Status.ERR_NOMASTER)
            logging.error(err_msg)
            return b9py.B9Status.failed_status(b9py.B9Status.ERR_NOMASTER, err_msg)

        # Activate service
        loop = asyncio.get_event_loop()
        loop.create_task(self._run_server())

        return b9py.B9Status.success_status()

    def _create_srv_reg_message(self):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'REGISTER', 'sub_cmd': 'SRV',
                                                                      'topic': self._topic,
                                                                      'message_type': self._message_type,
                                                                      'nodename': self._node_name,
                                                                      'IP': self._this_host_ip, 'port': self._port,
                                                                      'this_ip': self._this_host_ip,
                                                                      'this_host': self._this_host_name},
                            self._service_name)

    @property
    def name(self):
        return self._service_name

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def port(self):
        return self._port

    async def _run_server(self):
        try:
            while True:

                await asyncio.sleep(.001)

                # Get request
                [request_topic, msg_data] = await self._rep_sock.recv_multipart()
                request = b9py.Message.unpack(msg_data)

                if self._message_type is not None and request.message_type != self._message_type:
                    err_msg = "'{}' on node '{}'. Incoming {} != {}. {}".format(self._service_name,
                                                                                self._node_name,
                                                                                request.message_type,
                                                                                self._message_type,
                                                                                b9py.B9Status.ERR_WRONG_MESSAGE)
                    logging.error(err_msg)
                    response = b9py.MessageFactory.create_message_error(err_msg, self._service_name)
                else:
                    # Pass the request to specified callback
                    response = self._callback(request_topic.decode('utf-8'), request)

                # Return the callback's response to requestor
                response.source = self._service_name
                await self._rep_sock.send_multipart([self._topic.encode('utf-8'), response.pack()])

        except asyncio.CancelledError:
            logging.debug("'" + self._service_name + "' service has been canceled.")

        finally:
            self._rep_sock.close()


class ServiceClient(object):
    def __init__(self, node_name, master_uri, topic, namespace, srv_port, srv_host):
        self._node_name = node_name
        self._master_uri = master_uri

        self._namespace = namespace
        if namespace:
            if namespace == '/':
                self._topic = self._namespace + topic.strip('/')
            else:
                self._namespace = namespace.strip('/')
                self._topic = '/' + self._namespace + '/' + topic.strip('/')
        else:
            self._topic = topic

        self._srv_host = srv_host
        self._srv_port = srv_port
        self._srv_uri = None

        self._srv_client_name = "SCL-{}-{}-{}".format(self._node_name, self._topic,
                                                      str(uuid.uuid1()).split('-')[0])

        self._service_call_timeout = 5     # seconds
        self._max_retry = 3
        self._ctx = zmq.asyncio.Context()
        self._req_sock = self._ctx.socket(zmq.REQ)

        # Use localhost if service host not specified
        if self._srv_host is None:
            self._srv_host = "localhost"

    def connect(self):
        result = b9py.B9Status.success_status()

        # Use port if specified
        if self._srv_port is not None:
            self._srv_uri = "tcp://{}:{}".format(self._srv_host, self._srv_port)
        else:
            # Otherwise, lookup the service's URI using the topic
            if self._master_uri is not None:
                result = b9py.ServiceClient.oneshot_service_call(self._node_name,
                                                                 'master/registration/topic',
                                                                 None,
                                                                 self._create_req_lookup_message(self._topic),
                                                                 5555, self._master_uri)
                if result.is_successful:
                    # Set the service's URI so we can connect
                    if result.result_data.data['found']:
                        self._srv_uri = "tcp://{}:{}".format(result.result_data.data['IP'],
                                                             result.result_data.data['port'])
                    else:
                        # Unknown topic, not registered with master
                        err_msg = "'{}' on node '{}' failed. Topic '{}' {}".format(self._srv_client_name,
                                                                                   self._node_name,
                                                                                   self._topic,
                                                                                   b9py.B9Status.ERR_TOPIC_NOTFOUND)
                        logging.error(err_msg)
                        return b9py.B9Status.failed_status(b9py.B9Status.ERR_TOPIC_NOTFOUND, err_msg)
                else:
                    # Service call failed
                    logging.error("'{}' on node '{}' failed. {}".format(self._srv_client_name,
                                                                        self._node_name,
                                                                        result.status_type))
                    return result
            else:
                # No ability to subscribe. Give up.
                err_msg = "'{}' on node '{}' failed. {}".format(self._srv_client_name,
                                                                self._node_name,
                                                                b9py.B9Status.ERR_NOMASTER)
                logging.error(err_msg)
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_NOMASTER, err_msg)

        self._req_sock.connect(self._srv_uri)
        return result

    def _create_req_lookup_message(self, lookup_topic):
        return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LOOKUP', 'sub_cmd': 'SRV',
                                                                      'topic': lookup_topic},
                            self._srv_client_name)

    @property
    def service_call_timeout(self):
        return self._service_call_timeout

    @service_call_timeout.setter
    def service_call_timeout(self, value):
        self._service_call_timeout = value

    @property
    def max_retry(self):
        return self._max_retry

    @max_retry.setter
    def max_retry(self, value):
        self._max_retry = value

    @property
    def name(self):
        return self._srv_client_name

    @property
    def topic(self):
        return self._topic

    @property
    def namespace(self):
        return self._namespace

    @property
    def service_uri(self):
        return self._srv_uri

    def close(self):
        self._req_sock.setsockopt(zmq.LINGER, 0)
        self._req_sock.close()

    def restart(self):
        # Socket is confused. Close and remove it.
        self._req_sock.setsockopt(zmq.LINGER, 0)
        self._req_sock.close()

        # Create new connection
        self._req_sock = self._ctx.socket(zmq.REQ)
        self._req_sock.connect(self._srv_uri)

    def call_service(self, request):
        loop = asyncio.get_event_loop()
        for retry in range(self._max_retry + 1):
            try:
                result = loop.run_until_complete(self._call_service(request))
                return result

            except asyncio.TimeoutError:
                if retry < self._max_retry:
                    self.restart()
                    print("Service call retry {}".format(retry + 1))
                else:
                    return b9py.B9Status.failed_status(b9py.B9Status.ERR_TIMEOUT)

            except ConnectionRefusedError:
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_CONN_REFUSED)

            except asyncio.CancelledError:
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_CANCELED)

    async def _call_service(self, request_msg):
        # Send request
        await self._req_sock.send_multipart([self._topic.encode('utf-8'), request_msg.pack()])

        # Receive response with a timeout
        [_request_type, msg_data] = await asyncio.wait_for(self._req_sock.recv_multipart(),
                                                           self._service_call_timeout)

        response_msg = b9py.Message.unpack(msg_data)
        return b9py.B9Status.success_status(response_msg)

    @staticmethod
    def oneshot_service_call(nodename, topic, namespace, request, srv_port, srv_host=None):
        client = b9py.ServiceClient(nodename, None, topic, namespace, srv_port, srv_host)
        result = client.connect()
        if result.is_successful:
            sc_result = client.call_service(request)
            client.close()
            return sc_result
        client.close()
        return result
