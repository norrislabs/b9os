import b9py


class ImageProcessor(b9py.B9Processor):
    def __init__(self, nodename, processor_name, shared_image, output_queue, kwargs):
        super().__init__(nodename, processor_name, shared_image, output_queue, kwargs)
        self._publish = self.str2bool(self.get_arg("publish", True))

    def process(self):
        pass

    def publish_queue(self, detection, source=None):
        src = self._nodename + ("_" + source if source else "")
        det_msg = b9py.MessageFactory.create_message_detection(detection, src)
        self._output_queue.put(det_msg)

    @staticmethod
    def build_detection(object_id, label, score, detection_type, object_size, object_center, image_seq, optional=None):
        return {'id': int(object_id),
                'label': label,
                'detection_type': detection_type,
                'score': int(score),
                'object_size': (int(object_size[0]), int(object_size[1])),
                'object_center': (int(object_center[0]), int(object_center[1])),
                'image_seq': int(image_seq),
                'extra_data': optional}
