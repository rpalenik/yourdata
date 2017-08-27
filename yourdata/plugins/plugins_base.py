# Plugin base classes
import abc

import six


@six.add_metaclass(abc.ABCMeta)
class EncryptBase(object):
    """Base class for encryption plugin
    """

    def __init__(self, max_width=60):
        self.max_width = max_width

    @abc.abstractmethod
    def encrypt(self, data):
        """Run encryption algorithm on payload

        :param data: Payload
        :type data: str
        :returns: encrypted string
        """


@six.add_metaclass(abc.ABCMeta)
class KeyMethodBase(object):
    """Base class for data record key generation
    """

    @abc.abstractmethod
    def generate_key(self, record):
        """Generates key for each record written to the database

        :param record: single JSON formatted record
        :type : python dict
        :returns: string
        """
        return ""


@six.add_metaclass(abc.ABCMeta)
class DataSourceBase(object):
    """Base class for data_sources plugins
    """

    @abc.abstractmethod
    def load(self):
        """Load data
        """
