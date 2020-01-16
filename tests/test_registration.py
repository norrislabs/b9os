import pprint
import b9py


def _create_srv_reg_message():
    return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'REGISTER', 'sub_cmd': 'SRV',
                                                                  'topic': 'test/topic1',
                                                                  'message_type': b9py.Message.MSGTYPE_ANY,
                                                                  'nodename': 'some_node', 'IP': '192.168.3.999',
                                                                  'port': '12345',
                                                                  'this_ip': '192.168.3.99',
                                                                  'this_host': 'host1.local'},
                        'test_service')


def _create_sub_reg_message():
    return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'REGISTER', 'sub_cmd': 'SUB',
                                                                  'topic': 'test/topic1',
                                                                  'message_type': b9py.Message.MSGTYPE_ANY,
                                                                  'nodename': 'sub_node1', 'IP': '192.168.3.999',
                                                                  'port': '12345',
                                                                  'this_ip': '192.168.3.98',
                                                                  'this_host': 'host2.local'},
                        'test_subscriber')


def _create_req_lookup_message():
    return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LOOKUP', 'sub_cmd': 'SUB',
                                                                  'topic': 'test/topic1'},
                        'test_subscriber')


def _create_req_list_message():
    return b9py.Message(b9py.Message.MSGTYPE_TOPIC_REGISTRATION, {'cmd': 'LIST', 'sub_cmd': 'SUB',
                                                                  'topic': 'none'},
                        'test_subscriber')


# Node registration messages
def _create_node_reg_message():
    return b9py.Message(b9py.Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'REGISTER',
                                                                 'nodename': 'node1',
                                                                 'IP': '192.168.3.998',
                                                                 'pid': '12345',
                                                                 'host': 'host1.local'},
                        'test_node')


def _create_node_reg_message2():
    return b9py.Message(b9py.Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'REGISTER',
                                                                 'nodename': 'node2',
                                                                 'IP': '192.168.3.999',
                                                                 'pid': '54321',
                                                                 'host': 'host2.local'},
                        'test_node')


def _create_node_lookup_message():
    return b9py.Message(b9py.Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'LOOKUP', 'nodename': 'node1'},
                        'test_node')


def _create_node_list_message():
    return b9py.Message(b9py.Message.MSGTYPE_NODE_REGISTRATION, {'cmd': 'LIST'},
                        'test_node')


# Init B9 core
b9 = b9py.B9('registration_test', 'localhost')
print("Node host: {}, IP: {}, PID: {}".format(b9.hostname, b9.hostip, b9.pid))

# Node registration service client
client = b9.create_service_client('master/registration/node', '', 5557)
client.connect()
print("Using node registration service at {}".format(client.service_uri))

# Register a fake nodes
client.service_call_timeout = 3
# result = client.call_service(_create_node_reg_message())
# print("Register node1 result: {}".format(result.message))

# result = client.call_service(_create_node_reg_message2())
# print("Register node2 result: {}".format(result.message))

# Lookup node1
r = client.call_service(_create_node_lookup_message())
if not r.is_successful:
    print('Lookup failed.')
else:
    reg = r.result_data.data
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(reg)

# List all the nodes
r = client.call_service(_create_node_list_message())
if not r.is_successful:
    print('List failed.')
else:
    lst = r.result_data.data
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(lst)

b9.shutdown()
