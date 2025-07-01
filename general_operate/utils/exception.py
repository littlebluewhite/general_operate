class GeneralOperateException(Exception):
    def __init__(self, status_code, message_code, message=""):
        self.status_code = status_code
        self.message_code = message_code
        self.message = message
