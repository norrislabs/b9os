import time
import msgpack


class Message(object):
    MSGTYPE_ANY = None
    MSGTYPE_NULL = 'Null'
    MSGTYPE_STRING = 'String'
    MSGTYPE_INT = 'Int'
    MSGTYPE_FLOAT = 'Float'
    MSGTYPE_BOOL = 'Bool'
    MSGTYPE_LIST = 'List'
    MSGTYPE_DICT = 'Dict'

    MSGTYPE_DETECTION = 'Detection'
    MSGTYPE_VECTOR3 = 'Vector3'
    MSGTYPE_TWIST = 'Twist'
    MSGTYPE_IMAGE = 'Image'
    MSGTYPE_POINTCLOUD = 'PointCloud'
    MSGTYPE_OBSTACLEMAP = 'ObstacleMap'

    MSGTYPE_ERROR = 'Error'

    MSGTYPE_TOPIC_REGISTRATION = 'TopicRegistration'
    MSGTYPE_NODE_REGISTRATION = 'NodeRegistration'
    MSGTYPE_PARAMETER = 'Parameter'

    # Message sequence (ID) counter
    _seq = 0

    def __init__(self, message_type, data, source=None, timestamp=None):
        ts = time.time() if timestamp is None else timestamp
        self._header = {'timestamp': ts, 'message_type': message_type, 'source': source, 'seq': Message._seq}
        self._data = data

        Message._seq += 1

    @property
    def timestamp(self):
        return self._header['timestamp']

    @timestamp.setter
    def timestamp(self, value):
        self._header['timestamp'] = value

    @property
    def sequence(self):
        return self._header['seq']

    @property
    def source(self):
        return self._header['source']

    @source.setter
    def source(self, value):
        self._header['source'] = value

    @property
    def message_type(self):
        return self._header['message_type']

    @property
    def data_type(self):
        return type(self._data).__name__

    @property
    def data(self):
        return self._data

    def pack(self):
        msg_p = {'header': self._header, 'data': self._data}
        return msgpack.packb(msg_p, use_bin_type=True)

    @staticmethod
    def unpack(msg_data):
        msg_decoded = msgpack.unpackb(msg_data, raw=False)
        return Message(msg_decoded['header']['message_type'],
                       msg_decoded['data'],
                       msg_decoded['header']['source'],
                       msg_decoded['header']['timestamp'])

    def __str__(self):
        ts = "{0: <18}".format(self.timestamp)
        return "timestamp: {} seq: {}, source: {}, message_type: {}, data_type: {}, data: {}". \
            format(ts,
                   self.sequence,
                   self.source,
                   self.message_type,
                   self.data_type,
                   self.data)


class MessageFactory(object):
    def __init__(self):
        pass

    @staticmethod
    def create_message_string(data, source=None):
        assert (isinstance(data, str))
        return Message(Message.MSGTYPE_STRING, data, source)

    @staticmethod
    def create_message_int(data, source=None):
        assert (isinstance(data, int))
        return Message(Message.MSGTYPE_INT, data, source)

    @staticmethod
    def create_message_float(data, source=None):
        assert (isinstance(data, float))
        return Message(Message.MSGTYPE_FLOAT, data, source)

    @staticmethod
    def create_message_bool(data, source=None):
        assert (isinstance(data, bool))
        return Message(Message.MSGTYPE_BOOL, data, source)

    @staticmethod
    def create_message_list(data, source=None):
        assert (isinstance(data, list))
        return Message(Message.MSGTYPE_LIST, data, source)

    @staticmethod
    def create_message_dictionary(data, source=None):
        assert (isinstance(data, dict))
        return Message(Message.MSGTYPE_DICT, data, source)

    @staticmethod
    def create_message_detection(data, source=None):
        assert (isinstance(data, dict))
        return Message(Message.MSGTYPE_DETECTION, data, source)

    @staticmethod
    def create_message_vector3(x, y, z, source=None):
        assert ((isinstance(x, int) or isinstance(x, float)) and
                (isinstance(y, int) or isinstance(y, float)) and
                (isinstance(z, int) or isinstance(z, float)))
        return Message(Message.MSGTYPE_VECTOR3, {'X': x, 'Y': y, 'Z': z}, source)

    @staticmethod
    def create_message_image(image, source=None):
        assert (isinstance(image, bytes))
        return Message(Message.MSGTYPE_IMAGE, image, source)

    @staticmethod
    def create_message_null(source=None):
        return Message(Message.MSGTYPE_NULL, None, source)

    @staticmethod
    def create_message_pointcloud(points, source=None):
        assert (isinstance(points, list) and
                all(isinstance(item, tuple) for item in points))
        return Message(Message.MSGTYPE_POINTCLOUD, points, source)

    @staticmethod
    def create_message_obstacle_map(robot_width, robot_length, lidar_offset, lidar_height, points, source=None):
        assert (isinstance(points, list) and
                all(isinstance(item, tuple) for item in points))
        return Message(Message.MSGTYPE_OBSTACLEMAP,
                       {'robot_width': robot_width, 'robot_length': robot_length,
                        'lidar_offset': lidar_offset, 'lidar_height': lidar_height,
                        'points': points},
                       source)

    @staticmethod
    def create_message_error(err_msg, source=None):
        assert (isinstance(err_msg, str))
        return Message(Message.MSGTYPE_ERROR, err_msg, source)

    # Topic Registration messages
    @staticmethod
    def create_topic_list_message(reg_type='pub'):
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LIST', 'sub_cmd': reg_type.upper(),
                                                            'topic': 'none'},
                       'b9topic')

    @staticmethod
    def create_topic_lookup_message(lookup_topic):
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LOOKUP', 'sub_cmd': 'PUB',
                                                            'topic': lookup_topic},
                       'b9topic')

    @staticmethod
    def create_topic_unreg_message(lookup_topic):
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'UNREGISTER', 'sub_cmd': 'PUB',
                                                            'topic': lookup_topic},
                       'b9topic')

    # Service Registration messages
    @staticmethod
    def create_srv_list_message():
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LIST', 'sub_cmd': 'SRV',
                                                            'topic': 'none'},
                       'b9service')

    @staticmethod
    def create_srv_lookup_message(lookup_topic):
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LOOKUP', 'sub_cmd': 'SRV',
                                                            'topic': lookup_topic},
                       'b9service')

    @staticmethod
    def create_srv_unreg_message(lookup_topic):
        return Message(Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'UNREGISTER', 'sub_cmd': 'SRV',
                                                            'topic': lookup_topic},
                       'b9service')

    # Node Registration messages
    @staticmethod
    def create_node_lookup_message(nodename):
        return Message(Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'LOOKUP', 'nodename': nodename},
                       'b9node')

    @staticmethod
    def create_node_unreg_message(nodename):
        return Message(Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'UNREGISTER', 'nodename': nodename},
                       'b9node')

    @staticmethod
    def create_node_list_message():
        return Message(Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'LIST'},
                       'b9node')

    @staticmethod
    def create_node_kill_message(nodename, username, password):
        return Message(Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'KILL', 'nodename': nodename,
                                                           'username': username, 'password': password},
                       'b9node')
