#  "THE BEER-WARE LICENSE" (Revision 42):
# <jonas@agilefrog.se> wrote this file. As long as you retain this notice you
# can do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return Jonas Ericsson.

"""
A logging handler for MongoDB in Python.

See samples on how to use it.
"""

import datetime
import logging
import pymongo


class MongoHandler(logging.Handler):
    """
    A class which sends records to a MongoDB.
    """

    def __init__(self, host="localhost", port=27017, dbname='logs', collection='logs'):
        """
        Initialize the connection pool and set the desired database.

        :param host: the IP address or hostname for the MongoDb (default: localhost)
        :type host: str
        :param port: the port (default: 27017)
        :type port: int
        :param dbname: the name of the DB to save log records to, will default to 'logs'
        :type dbname: str
        :param collection: the name of the collection to save logs to (default: 'logs')
        :type collection: str
        """
        logging.Handler.__init__(self)
        self.client = pymongo.MongoClient(host, port)
        self.dbname = dbname
        self.collection = collection

    def format(self, record):
        """
        Convert log record to the dictionary representing Mongo Document

        :type record: logging.LogRecord
        :rtype: dict
        """
        message = None
        if isinstance(record.msg, Exception):
            message = self.transform_message(record)
        else:
            message = record.msg

        entry = {
            'timestamp': datetime.datetime.now(),
            'msg': message,
            'level': record.levelname,
            'module': record.module,
            'function': record.funcName,
            'lineno': record.lineno,
        }

        self.add_thread_info(entry, record)
        self.add_multiproc_info(entry, record)
        return entry

    def emit(self, record):
        """
        Emit a record.

        Send the record to a mongo instance as a document.

        :type record: logging.LogRecord
        """
        try:
            self.save(self.format(record))
        except Exception, _:
            self.handleError(record)

    @classmethod
    def transform_message(cls, message):
        """
        Exceptions can't be serialized directly so we need to transform it
        to a string instead. We use the format: Type: message, arguments.

        :type message: str
        :rtype: str
        """
        exception_type = type(message.msg)
        log_message = str(message.msg)
        arguments = message.msg.args
        return "%s: %s, %s" % (exception_type, log_message, arguments)

    @classmethod
    def add_thread_info(cls, entry, record):
        """
        Add thread information in its own key.

        :type entry: dict
        :type record: logging.LogRecord
        """
        entry['thread'] = {
            'thread': record.thread,
            'thread_name': record.threadName,
        }

    @classmethod
    def add_multiproc_info(cls, entry, record):
        """
        Add process information in its own key.

        :type entry: dict
        :type record: logging.LogRecord
        """
        entry['process'] = {
            'process_name': record.processName,
            'process_id': record.process,
        }

    def save(self, entry):
        """
        Save an entry to the collection. We want this in a separate method since
        it makes it easier to stub it with mox.

        :type entry: dict
        """
        self.client[self.dbname][self.collection].save(entry)
