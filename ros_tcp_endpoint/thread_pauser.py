import threading


class ThreadPauser:
    def __init__(self):
        self.condition = threading.Condition()
        self.result = None
        self._resumed = False

    def sleep_until_resumed(self, timeout_sec=None):
        with self.condition:
            if not self._resumed:
                self.condition.wait_for(lambda: self._resumed, timeout=timeout_sec)
            return self._resumed

    def resume_with_result(self, result):
        with self.condition:
            if self._resumed:
                return
            self.result = result
            self._resumed = True
            self.condition.notify_all()
