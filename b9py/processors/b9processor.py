from abc import ABC, abstractmethod
from multiprocessing import Process, Lock
import b9py


class SharedMessage(object):
    def __init__(self):
        self._msg = b9py.MessageFactory.create_message_null()
        self._lock = Lock()

    def update(self, msg: b9py.Message):
        with self._lock:
            self._msg = msg

    def value(self):
        with self._lock:
            return self._msg

    def sequence(self):
        with self._lock:
            return self._msg.sequence


class B9Processor(ABC):
    def __init__(self, nodename, processor_name, shared_message, output_queue, kwargs):
        self._nodename = nodename
        self._processor_name = processor_name
        self._shared_message = shared_message
        self._output_queue = output_queue
        self._kwargs = kwargs

        self._proc = None
        self._last_msg_seq = 0
        self._running = False

        super().__init__()

    def start(self):
        self._running = True
        self._proc = Process(target=self.process)
        self._proc.daemon = True
        self._proc.start()

    def stop(self):
        self._running = False
        self._proc.kill()

    @abstractmethod
    def process(self):
        pass

    def latest_message(self):
        seq = self._shared_message.sequence()
        if seq > 0 and seq != self._last_msg_seq:
            self._last_msg_seq = seq
            return seq, self._shared_message.value()
        return 0, None

    def get_arg(self, key, default=None):
        if key in self._kwargs.keys():
            return self._kwargs[key]
        else:
            return default

    @staticmethod
    def str2bool(v):
        return v.lower() in ("yes", "true", "t", "1")
