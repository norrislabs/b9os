import os
import ast
from pyparsing import Word, alphas, nums, Group, OneOrMore, Keyword, QuotedString, pyparsing_common, ParseException

import b9py


class ScriptEngine(object):
    def __init__(self, b9core: b9py.B9, namespace, command_cb=None):
        self._b9 = b9core
        self._namespace = namespace
        self._nodename = self._b9.nodename
        self._command_cb = command_cb

        self._script_exe = None
        self._topic_aliases = {}
        self._cancel = False

        self._publishers = {}
        self._services = {}
        self._actions = {}

        self._script_dir = "scripts"
        self._script_ext = "b9s"
        self._topic_alias_ext = "b9a"

        # Quoted text grammar
        self._double_quote_string = QuotedString('"', unquoteResults=False)
        self._single_quote_string = QuotedString("'", unquoteResults=False)

        # Value grammer
        self._data_parser = Word(alphas + nums + "[]{},.:-_' ")
        self._boolean_parser = (Keyword("true", caseless=True) | Keyword("false", caseless=True)).setParseAction(
            lambda tokens: tokens[0].capitalize())
        self._value_parser = self._double_quote_string | self._single_quote_string | \
            self._boolean_parser | self._data_parser

        # Define the grammar for the various command parameters
        self._subop = Keyword("start")("subop") | Keyword("stop")("subop") | Keyword("pause")("subop")
        self._topic = Word(alphas + nums + "/-_@").setResultsName("topic")
        self._value = self._value_parser.setResultsName("value")
        self._text = self._value_parser.setResultsName("text")
        self._sleep_time = pyparsing_common.number()("sleep_time")

        # Define the grammar for the commands
        alias_command = Group("alias" + self._topic + self._text)
        publish_command = Group("publish" + self._topic + self._value)
        service_command = Group("service" + self._topic + self._value)
        action_command = Group("action" + self._subop + self._topic + self._value)
        sleep_command = Group("sleep" + self._sleep_time)
        print_command = Group("print" + self._text)

        self._command_functions = {
            "alias": self._topic_alias_func,
            "publish": self._publish_func,
            "service": self._service_func,
            "action": self._action_func,
            "sleep": self._sleep_func,
            "print": self._print_func
        }

        # Define the overall grammar
        self._grammar = OneOrMore(
            alias_command | publish_command | service_command
            | action_command | sleep_command | print_command)

    def _lookup_topic(self, lookup: str):
        topic = lookup
        if lookup.startswith('@'):
            if lookup[1:] in self._topic_aliases.keys():
                topic = self._topic_aliases[lookup[1:]]
            else:
                self._b9.logger.error("No topic for alias '{}'".format(topic))
                return None
        return topic

    # Define functions for each command
    # Set topic alias
    def _topic_alias_func(self, params):
        self._b9.logger.debug("'@{}' is alias for topic '{}'".format(params['text'], params['topic']))
        self._topic_aliases[params['text']] = params['topic']
        return b9py.B9Status.success_status()

    # Publish value to topic
    def _publish_func(self, params):
        val = ast.literal_eval(params['value'])
        topic = self._lookup_topic(params['topic'])
        if topic is not None:
            self._b9.logger.debug("Publishing to topic '{}' with value '{}' - {}".format(topic, val, type(val)))

            if topic in self._publishers.keys():
                pub = self._publishers[topic]
            else:
                # Create new publisher
                pub = self._b9.create_publisher(topic, b9py.Message.MSGTYPE_ANY, self._namespace)
                stat = pub.advertise()
                if stat.is_successful:
                    self._publishers[topic] = pub
                else:
                    return stat

            status = pub.publish(b9py.MessageFactory.create_message_any(val))
            if not status.is_successful:
                self._b9.logger.error("Publisher failed to publish: {}".format(status.message))
        else:
            status = b9py.B9Status.failed_status(b9py.B9Status.ERR_TOPIC_NOTFOUND)

        # Callback
        if self._command_cb:
            self._command_cb("publish", params, status)
        return status

    # Call a service
    def _service_func(self, params):
        val = ast.literal_eval(params['value'])
        topic = self._lookup_topic(params['topic'])
        if topic is not None:
            self._b9.logger.debug("Service call to topic '{}' with value '{}' - {}".format(params['topic'],
                                                                                           val,
                                                                                           type(val)))
            if topic in self._services.keys():
                caller = self._services[topic]
            else:
                # Create a new service client
                caller = self._b9.create_service_client(topic, self._namespace)
                stat = caller.connect()
                if stat.is_successful:
                    self._services[topic] = caller
                else:
                    return stat

            msg = None
            if isinstance(val, str):
                msg = b9py.MessageFactory.create_message_string(val)
            elif isinstance(val, bool):
                msg = b9py.MessageFactory.create_message_bool(val)
            elif isinstance(val, int):
                msg = b9py.MessageFactory.create_message_int(val)
            elif isinstance(val, float):
                msg = b9py.MessageFactory.create_message_float(val)
            elif val is None:
                msg = b9py.MessageFactory.create_message_null()
            elif isinstance(val, list):
                msg = b9py.MessageFactory.create_message_list(val)
            elif isinstance(val, dict):
                if 'X' in val.keys() and 'Y' in val.keys() and 'Z' in val.keys():
                    msg = b9py.MessageFactory.create_message_vector3(val['X'], val['Y'], val['Z'])
                else:
                    msg = b9py.MessageFactory.create_message_dictionary(val)

            if msg is None:
                self._b9.logger.error("Bad data type: '{}'.".format(type(val)))
                status = b9py.B9Status.failed_status(b9py.B9Status.ERR_WRONG_MESSAGE)
            else:
                status = caller.call_service(msg)
        else:
            status = b9py.B9Status.failed_status(b9py.B9Status.ERR_TOPIC_NOTFOUND)

        # Callback
        if self._command_cb:
            self._command_cb("service", params, status)

        if status.is_successful:
            result_msg: b9py.Message = status.result_data
            self._b9.logger.debug("Service topic '{}' returned the value '{}' - {}".format(topic, result_msg.data,
                                                                                           type(result_msg.data)))
        else:
            self._b9.logger.error("Service call failed. {}".format(status.status_type))

        return status

    # Start/Stop/Pause an action
    def _action_func(self, params):
        val = ast.literal_eval(params['value'])
        topic = self._lookup_topic(params['topic'])
        if topic is not None:
            if topic in self._actions.keys():
                action_client = self._actions[topic]
            else:
                # Setup action client
                action_client = self._b9.create_action_client(topic, None, self._namespace)
                if action_client:
                    self._actions[topic] = action_client

            # Start action
            if params['subop'] == 'start':
                self._b9.logger.debug("Starting action for topic '{}' with value '{}' - {}".format(params['topic'],
                                                                                                   val,
                                                                                                   type(val)))
                status = action_client.start(val)

            # Stop action
            elif params['subop'] == 'stop':
                self._b9.logger.debug("Stopping action for topic '{}' with value '{}' - {}".format(params['topic'],
                                                                                                   val,
                                                                                                   type(val)))
                status = action_client.stop(val)

            # Pause action
            elif params['subop'] == 'pause':
                self._b9.logger.debug("Pausing action for topic '{}' with value '{}' - {}".format(params['topic'],
                                                                                                  val,
                                                                                                  type(val)))
                status = action_client.pause(val)

            else:
                status = b9py.B9Status.failed_status(b9py.B9Status.ERR_NOT_IMPLEMENTED)

        else:
            status = b9py.B9Status.failed_status(b9py.B9Status.ERR_TOPIC_NOTFOUND)

        # Callback
        if self._command_cb:
            self._command_cb("action", params, status)

        if status.is_successful:
            result_msg: b9py.Message = status.result_data
            self._b9.logger.debug("Action topic '{}' returned the value '{}' - {}".format(topic, result_msg.data,
                                                                                          type(result_msg.data)))
        else:
            self._b9.logger.error("Action call failed. {}".format(status.status_type))

        return status

    # Pause for a specified amount of time in seconds
    def _sleep_func(self, params):
        self._b9.logger.debug(f"Sleeping for {params['sleep_time']} seconds")

        for _ in range(int(params['sleep_time'] / 0.1)):
            if self._cancel:
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_CANCELED, "Script canceled.")
            self._b9.spin_once(0.099)

        # Callback
        if self._command_cb:
            self._command_cb("sleep", params, b9py.B9Status.success_status())
        return b9py.B9Status.success_status()

    # Print a string to the console
    def _print_func(self, params):
        self._b9.logger.info("*** " + params['text'].strip('\"\''))

        # Callback
        if self._command_cb:
            self._command_cb("print", params, b9py.B9Status.success_status())
        return b9py.B9Status.success_status()

    # Define a function to execute a parsed command
    def _execute_command(self, command, alias_only) -> b9py.B9Status:
        result_dict = command.asDict()
        if (alias_only and command[0] in ["alias"]) or not alias_only:
            return self._command_functions[command[0]](result_dict)
        else:
            return b9py.B9Status.failed_status()

    # Define a function to parse and execute a string of commands
    def _parse_and_execute_commands(self, command_string, alias_only) -> b9py.B9Status:
        parsed_commands = self._grammar.parseString(command_string, parseAll=True)
        for command in parsed_commands:
            status = self._execute_command(command, alias_only)
            if not status.is_successful:
                return status

            if self._cancel:
                return b9py.B9Status.failed_status(b9py.B9Status.ERR_CANCELED, "Script canceled.")
            self._b9.spin_once(quiet=False)

        return b9py.B9Status.success_status()

    def _execute(self, script_text, success_msg, alias_only):
        if script_text is None:
            if self._script_exe is None:
                return b9py.B9Status(False, status_text="No script to execute.")
        else:
            self._script_exe = script_text

        try:
            self._parse_and_execute_commands(self._script_exe, alias_only)

        except ParseException as ex:
            return b9py.B9Status(False, status_text="Parsing error on line {}, col {}.".format(ex.lineno, ex.col))

        except SyntaxError as ex:
            return b9py.B9Status(False, status_text="Syntax error on line {}.".format(ex.lineno))

        except ValueError as ex:
            return b9py.B9Status(False, status_text="ValueError: {}".format(ex))

        return b9py.B9Status(True, status_text=success_msg)

    @property
    def script_dir(self):
        return self._script_dir

    @script_dir.setter
    def script_dir(self, dr):
        self._script_dir = dr

    def is_script(self, script_name):
        script_path = "{}/{}.{}".format(self._script_dir, script_name, self._script_ext)
        return os.path.exists(script_path)

    def is_aliases(self, aliases_name=None):
        if aliases_name:
            al_name = aliases_name
        else:
            al_name = self._namespace
        script_path = "{}/{}.{}".format(self._script_dir, al_name, self._topic_alias_ext)
        return os.path.exists(script_path)

    def load_topic_aliases(self, aliases_name=None):
        if aliases_name:
            al_name = aliases_name
        else:
            al_name = self._namespace
        aliases_path = "{}/{}.{}".format(self._script_dir, al_name, self._topic_alias_ext)
        if not os.path.exists(aliases_path):
            error = "Cannot find aliases file '{}'".format(aliases_path)
            return b9py.B9Status(False, status_text=error)

        # Open the aliases file and read the contents
        try:
            with open(aliases_path, 'r', encoding='utf-8') as file:
                _aliases_exe = file.read()
        except IOError as ex:
            error = "Aliases file read error: '{}'".format(ex)
            return b9py.B9Status(False, status_text=error)

        # Execute the aliases file (script with only 'alias' commands) to map topics to aliases
        self._topic_aliases = {}
        stat = self._execute(_aliases_exe, "Loaded aliases file '{}'".format(aliases_path), alias_only=True)
        return stat

    def load_script(self, script_name):
        script_path = "{}/{}.{}".format(self._script_dir, script_name, self._script_ext)
        if not os.path.exists(script_path):
            error = "Cannot find script file '{}'".format(script_path)
            return b9py.B9Status(False, status_text=error)

        # Open the script file and read the contents
        try:
            with open(script_path, 'r', encoding='utf-8') as file:
                self._script_exe = file.read()
        except IOError as ex:
            error = "Script file read error: '{}'".format(ex)
            return b9py.B9Status(False, status_text=error)

        status = "Loaded script file '{}'".format(script_path)
        return b9py.B9Status(True, status_text=status)

    def execute(self, script_text=None) -> b9py.B9Status:
        self._cancel = False
        return self._execute(script_text, "Script completed.", False)

    def cancel(self) -> b9py.B9Status:
        self._cancel = True
        return b9py.B9Status(True, status_text="Script canceled.")
