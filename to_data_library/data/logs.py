import logging


class Client:

    def __init__(self):
        """
        Logging configuration
        """

        self._logger = self._load_logger('to-data-platform-data')

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, value):
        self._logger = value

    @staticmethod
    def _load_logger(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(name)s - %(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger


client = Client()
