#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# redoer.py - This script implements a daemon process that detects
#   "redo records" and processes them. There are 3 types of threads running:
#     1) A single thread that monitors and logs periodic status.
#     2) A single thread that reads from Senzing's "redo list" and places on
#        an internal queue.
#     3) Multiple threads that read from the internal queue and process each
#        Senzing redo record.
# -----------------------------------------------------------------------------

from glob import glob
from urllib.parse import urlparse, urlunparse
import argparse
import datetime
import json
import linecache
import logging
import multiprocessing
import os
import pika
import signal
import string
import sys
import threading
import time

# Import Senzing libraries.
try:
    from G2ConfigMgr import G2ConfigMgr
    from G2Engine import G2Engine
    from G2Product import G2Product
    import G2Exception
except ImportError:
    pass

__all__ = []
__version__ = "1.1.0"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2020-01-15'
__updated__ = '2020-03-02'

SENZING_PRODUCT_ID = "5010"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# Working with bytes.

KILOBYTES = 1024
MEGABYTES = 1024 * KILOBYTES
GIGABYTES = 1024 * MEGABYTES

# Lists from https://www.ietf.org/rfc/rfc1738.txt

safe_character_list = ['$', '-', '_', '.', '+', '!', '*', '(', ')', ',', '"' ] + list(string.ascii_letters)
unsafe_character_list = [ '"', '<', '>', '#', '%', '{', '}', '|', '\\', '^', '~', '[', ']', '`']
reserved_character_list = [ ';', ',', '/', '?', ':', '@', '=', '&']

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "config_path": {
        "default": "/etc/opt/senzing",
        "env": "SENZING_CONFIG_PATH",
        "cli": "config-path"
    },
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "delay_in_seconds": {
        "default": 0,
        "env": "SENZING_DELAY_IN_SECONDS",
        "cli": "delay-in-seconds"
    },
    "engine_configuration_json": {
        "default": None,
        "env": "SENZING_ENGINE_CONFIGURATION_JSON",
        "cli": "engine-configuration-json"
    },
    "expiration_warning_in_days": {
        "default": 30,
        "env": "SENZING_EXPIRATION_WARNING_IN_DAYS",
        "cli": "expiration-warning-in-days"
    },
    "g2_database_url_generic": {
        "default": "sqlite3://na:na@/var/opt/senzing/sqlite/G2C.db",
        "env": "SENZING_DATABASE_URL",
        "cli": "database-url"
    },
    "kafka_failure_bootstrap_server": {
        "default": None,
        "env": "SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER",
        "cli": "kafka-failure-bootstrap-server",
    },
    "kafka_failure_topic": {
        "default": "senzing-kafka-failure-topic",
        "env": "SENZING_KAFKA_FAILURE_TOPIC",
        "cli": "kafka-failure-topic"
    },
    "kafka_info_bootstrap_server": {
        "default": None,
        "env": "SENZING_KAFKA_INFO_BOOTSTRAP_SERVER",
        "cli": "kafka-info-bootstrap-server",
    },
    "kafka_info_topic": {
        "default": "senzing-kafka-info-topic",
        "env": "SENZING_KAFKA_INFO_TOPIC",
        "cli": "kafka--info-topic"
    },
    "log_license_period_in_seconds": {
        "default": 60 * 60 * 24,
        "env": "SENZING_LOG_LICENSE_PERIOD_IN_SECONDS",
        "cli": "log-license-period-in-seconds"
    },
    "monitoring_period_in_seconds": {
        "default": 60 * 10,
        "env": "SENZING_MONITORING_PERIOD_IN_SECONDS",
        "cli": "monitoring-period-in-seconds",
    },
    "queue_maxsize": {
        "default": 10,
        "env": "SENZING_QUEUE_MAX_SIZE",
        "cli": "queue-max-size"
    },
    "rabbitmq_failure_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_HOST",
        "cli": "rabbitmq-failure-host",
    },
    "rabbitmq_failure_password": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_PASSWORD",
        "cli": "rabbitmq-failure-password",
    },
    "rabbitmq_failure_queue": {
        "default": "senzing-rabbitmq-failure-queue",
        "env": "SENZING_RABBITMQ_FAILURE_QUEUE",
        "cli": "rabbitmq-failure-queue",
    },
    "rabbitmq_failure_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_USERNAME",
        "cli": "rabbitmq-failure-username",
    },
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_info_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_HOST",
        "cli": "rabbitmq-info-host",
    },
    "rabbitmq_info_password": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_PASSWORD",
        "cli": "rabbitmq-info-password",
    },
    "rabbitmq_info_queue": {
        "default": "senzing-rabbitmq-info-queue",
        "env": "SENZING_RABBITMQ_INFO_QUEUE",
        "cli": "rabbitmq-info-queue",
    },
    "rabbitmq_info_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_USERNAME",
        "cli": "rabbitmq-info-username",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "rabbitmq_queue": {
        "default": "senzing-rabbitmq-queue",
        "env": "SENZING_RABBITMQ_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_redo_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_HOST",
        "cli": "rabbitmq-redo-host",
    },
    "rabbitmq_redo_password": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_PASSWORD",
        "cli": "rabbitmq-redo-password",
    },
    "rabbitmq_redo_queue": {
        "default": "senzing-rabbitmq-redo-queue",
        "env": "SENZING_RABBITMQ_REDO_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_redo_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_USERNAME",
        "cli": "rabbitmq-redo-username",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
    },
   "redo_sleep_time_in_seconds": {
        "default": 60,
        "env": "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "resource_path": {
        "default": "/opt/senzing/g2/resources",
        "env": "SENZING_RESOURCE_PATH",
        "cli": "resource-path"
    },
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    },
    "support_path": {
        "default": "/opt/senzing/data",
        "env": "SENZING_SUPPORT_PATH",
        "cli": "support-path"
    },
    "threads_per_process": {
        "default": 4,
        "env": "SENZING_THREADS_PER_PROCESS",
        "cli": "threads-per-process",
    }
}

# Enumerate keys in 'configuration_locator' that should not be printed to the log.

keys_to_redact = [
    "g2_database_url_generic",
    "g2_database_url_specific"
]

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    ''' Parse commandline arguments. '''

    subcommands = {
        'redo': {
            "help": 'Example task #1.',
            "arguments": {
                "--engine-configuration-json": {
                    "dest": "engine_configuration_json",
                    "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                    "help": "Advanced Senzing engine configuration. Default: none"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                }
            },
        },
        'redo-with-info-kafka': {
            "help": 'Example task #1.',
            "arguments": {
                "--engine-configuration-json": {
                    "dest": "engine_configuration_json",
                    "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                    "help": "Advanced Senzing engine configuration. Default: none"
                },
                "--kafka-bootstrap-server": {
                    "dest": "kafka_bootstrap_server",
                    "metavar": "SENZING_KAFKA_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: localhost:9092"
                },
                "--kafka-failure-bootstrap-server": {
                    "dest": "kafka_failure_bootstrap_server",
                    "metavar": "SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: SENZING_KAFKA_BOOTSTRAP_SERVER"
                },
                "--kafka-failure-topic": {
                    "dest": "kafka_failure_topic",
                    "metavar": "SENZING_KAFKA_FAILURE_TOPIC",
                    "help": "Kafka topic for failures. Default: senzing-kafka-failure-topic"
                },
                "--kafka-info-bootstrap-server": {
                    "dest": "kafka_info_bootstrap_server",
                    "metavar": "SENZING_KAFKA_INFO_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: SENZING_KAFKA_BOOTSTRAP_SERVER"
                },
                "--kafka-info-topic": {
                    "dest": "kafka_info_topic",
                    "metavar": "SENZING_KAFKA_INFO_TOPIC",
                    "help": "Kafka topic for info. Default: senzing-kafka-info-topic"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                }
            },
        },
        'redo-with-info-rabbitmq': {
            "help": 'Example task #1.',
            "arguments": {
                "--engine-configuration-json": {
                    "dest": "engine_configuration_json",
                    "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                    "help": "Advanced Senzing engine configuration. Default: none"
                },
                "--monitoring-period-in-seconds": {
                    "dest": "monitoring_period_in_seconds",
                    "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                    "help": "Period, in seconds, between monitoring reports. Default: 300"
                },
                "--rabbitmq-host": {
                    "dest": "rabbitmq_host",
                    "metavar": "SENZING_RABBITMQ_HOST",
                    "help": "RabbitMQ host. Default: localhost:5672"
                },
                "--rabbitmq-username": {
                    "dest": "rabbitmq_username",
                    "metavar": "SENZING_RABBITMQ_USERNAME",
                    "help": "RabbitMQ username. Default: user"
                },
                "--rabbitmq-password": {
                    "dest": "rabbitmq_password",
                    "metavar": "SENZING_RABBITMQ_PASSWORD",
                    "help": "RabbitMQ password. Default: bitnami"
                },
                "--rabbitmq-info-host": {
                    "dest": "rabbitmq_info_host",
                    "metavar": "SENZING_RABBITMQ_INFO_HOST",
                    "help": "RabbitMQ host. Default: SENZING_RABBITMQ_HOST"
                },
                "--rabbitmq-info-password": {
                    "dest": "rabbitmq_info_password",
                    "metavar": "SENZING_RABBITMQ_INFO_PASSWORD",
                    "help": "RabbitMQ password. Default: SENZING_RABBITMQ_PASSWORD"
                },
                "--rabbitmq-info-queue": {
                    "dest": "rabbitmq_info_queue",
                    "metavar": "SENZING_RABBITMQ_INFO_QUEUE",
                    "help": "RabbitMQ queue for info. Default: senzing-rabbitmq-info-queue"
                },
                "--rabbitmq-info-username": {
                    "dest": "rabbitmq_info_username",
                    "metavar": "SENZING_RABBITMQ_INFO_USERNAME",
                    "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
                },
                "--rabbitmq-failure-host": {
                    "dest": "rabbitmq_failure_host",
                    "metavar": "SENZING_RABBITMQ_FAILURE_HOST",
                    "help": "RabbitMQ host. Default: SENZING_RABBITMQ_HOST"
                },
                "--rabbitmq-failure-password": {
                    "dest": "rabbitmq_failure_password",
                    "metavar": "SENZING_RABBITMQ_FAILURE_PASSWORD",
                    "help": "RabbitMQ password. Default: SENZING_RABBITMQ_PASSWORD"
                },
                "--rabbitmq-failure-queue": {
                    "dest": "rabbitmq_failure_queue",
                    "metavar": "SENZING_RABBITMQ_FAILURE_QUEUE",
                    "help": "RabbitMQ queue for failures. Default: senzing-rabbitmq-failure-queue"
                },
                "--rabbitmq-failure-username": {
                    "dest": "rabbitmq_failure_username",
                    "metavar": "SENZING_RABBITMQ_FAILURE_USERNAME",
                    "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
                },
                "--threads-per-process": {
                    "dest": "threads_per_process",
                    "metavar": "SENZING_THREADS_PER_PROCESS",
                    "help": "Number of threads per process. Default: 4"
                }
            },
        },
        'write-to-rabbitmq': {
            "help": 'Read from Senzing Redo, write to RabbitMQ.',
            "arguments": {
                "--engine-configuration-json": {
                    "dest": "engine_configuration_json",
                    "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                    "help": "Advanced Senzing engine configuration. Default: none"
                },
                "--monitoring-period-in-seconds": {
                    "dest": "monitoring_period_in_seconds",
                    "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                    "help": "Period, in seconds, between monitoring reports. Default: 300"
                },
                "--rabbitmq-host": {
                    "dest": "rabbitmq_host",
                    "metavar": "SENZING_RABBITMQ_HOST",
                    "help": "RabbitMQ host. Default: localhost:5672"
                },
                "--rabbitmq-username": {
                    "dest": "rabbitmq_username",
                    "metavar": "SENZING_RABBITMQ_USERNAME",
                    "help": "RabbitMQ username. Default: user"
                },
                "--rabbitmq-password": {
                    "dest": "rabbitmq_password",
                    "metavar": "SENZING_RABBITMQ_PASSWORD",
                    "help": "RabbitMQ password. Default: bitnami"
                },
                "--rabbitmq-redo-queue": {
                    "dest": "rabbitmq_redo_queue",
                    "metavar": "SENZING_RABBITMQ_REDO_QUEUE",
                    "help": "RabbitMQ queue. Default: senzing-rabbitmq-redo-queue"
                },
            },
        },
        'sleep': {
            "help": 'Do nothing but sleep. For Docker testing.',
            "arguments": {
                "--sleep-time-in-seconds": {
                    "dest": "sleep_time_in_seconds",
                    "metavar": "SENZING_SLEEP_TIME_IN_SECONDS",
                    "help": "Sleep time in seconds. DEFAULT: 0 (infinite)"
                },
            },
        },
        'version': {
            "help": 'Print version of program.',
        },
        'docker-acceptance-test': {
            "help": 'For Docker acceptance testing.',
        },
    }

    parser = argparse.ArgumentParser(prog="template-python.py", description="Example python skeleton. For more information, see https://github.com/Senzing/template-python")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    for subcommand_key, subcommand_values in subcommands.items():
        subcommand_help = subcommand_values.get('help', "")
        subcommand_arguments = subcommand_values.get('arguments', {})
        subparser = subparsers.add_parser(subcommand_key, help=subcommand_help)
        for argument_key, argument_values in subcommand_arguments.items():
            subparser.add_argument(argument_key, **argument_values)

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 3xx Warning (i.e. logging.warning())
# 5xx User configuration issues (either logging.warning() or logging.err() for Client errors)
# 7xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


MESSAGE_INFO = 100
MESSAGE_WARN = 300
MESSAGE_ERROR = 700
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "120": "Sleeping for requested delay of {0} seconds.",
    "121": "Adding JSON to failure queue: {0}",
    "125": "G2 engine statistics: {0}",
    "127": "Monitor: {0}",
    "128": "Adding JSON to info queue: {0}",
    "129": "{0} is running.",
    "130": "{0} has exited.",
    "131": "Adding redo record to redo queue: {0}",
    "132": "Using Mixin: {0}",
    "160": "{0} LICENSE {0}",
    "161": "          Version: {0} ({1})",
    "162": "         Customer: {0}",
    "163": "             Type: {0}",
    "164": "  Expiration date: {0}",
    "165": "  Expiration time: {0} days until expiration",
    "166": "          Records: {0}",
    "167": "         Contract: {0}",
    "168": "  Expiration time: EXPIRED {0} days ago",
    "180": "User-supplied Governor loaded from {0}.",
    "292": "Configuration change detected.  Old: {0} New: {1}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "410": "Unknown RabbitMQ error when connecting: {0}.",
    "411": "Unknown RabbitMQ error when adding record to queue: {0} for line {1}.",
    "412": "Could not connect to RabbitMQ host at {1}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details.",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "695": "Unknown database scheme '{0}' in database url '{1}'",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "701": "G2Engine.getRedoRecord() bad return code: {0}",
    "702": "G2Engine.getRedoRecord() G2ModuleNotInitialized: {0} XML: {1}",
    "703": "G2Engine.getRedoRecord() err: {0}",
    "706": "G2Engine.process() bad return code: {0}",
    "707": "G2Engine.process() G2ModuleNotInitialized: {0} XML: {1}",
    "708": "G2Engine.process() G2ModuleGenericException: {0} XML: {1}",
    "709": "G2Engine.process() err: {0}",
    "721": "Running low on workers.  May need to restart",
    "730": "There are not enough safe characters to do the translation. Unsafe Characters: {0}; Safe Characters: {1}",
    "885": "License has expired.",
    "886": "G2Engine.addRecord() bad return code: {0}; JSON: {1}",
    "888": "G2Engine.addRecord() G2ModuleNotInitialized: {0}; JSON: {1}",
    "889": "G2Engine.addRecord() G2ModuleGenericException: {0}; JSON: {1}",
    "890": "G2Engine.addRecord() Exception: {0}; JSON: {1}",
    "891": "Original and new database URLs do not match. Original URL: {0}; Reconstructed URL: {1}",
    "892": "Could not initialize G2Product with '{0}'. Error: {1}",
    "893": "Could not initialize G2Hasher with '{0}'. Error: {1}",
    "894": "Could not initialize G2Diagnostic with '{0}'. Error: {1}",
    "895": "Could not initialize G2Audit with '{0}'. Error: {1}",
    "896": "Could not initialize G2ConfigMgr with '{0}'. Error: {1}",
    "897": "Could not initialize G2Config with '{0}'. Error: {1}",
    "898": "Could not initialize G2Engine with '{0}'. Error: {1}",
    "899": "{0}",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "902": "Thread: {0} Added message to internal queue: {1}",
    "903": "Thread: {0} Processing message: {1}",
    "904": "{0} processed: {1}",
    "905": "{0} processing redo record: {1}",
    "998": "Debugging enabled.",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


def message_info(index, *args):
    return message_generic(MESSAGE_INFO, index, *args)


def message_warning(index, *args):
    return message_generic(MESSAGE_WARN, index, *args)


def message_error(index, *args):
    return message_generic(MESSAGE_ERROR, index, *args)


def message_debug(index, *args):
    return message_generic(MESSAGE_DEBUG, index, *args)


def get_exception():
    ''' Get details about an exception. '''
    exception_type, exception_object, traceback = sys.exc_info()
    frame = traceback.tb_frame
    line_number = traceback.tb_lineno
    filename = frame.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_number, frame.f_globals)
    return {
        "filename": filename,
        "line_number": line_number,
        "line": line.strip(),
        "exception": exception_object,
        "type": exception_type,
        "traceback": traceback,
    }

# -----------------------------------------------------------------------------
# Database URL parsing
# -----------------------------------------------------------------------------


def translate(map, astring):
    new_string = str(astring)
    for key, value in map.items():
        new_string = new_string.replace(key, value)
    return new_string


def get_unsafe_characters(astring):
    result = []
    for unsafe_character in unsafe_character_list:
        if unsafe_character in astring:
            result.append(unsafe_character)
    return result


def get_safe_characters(astring):
    result = []
    for safe_character in safe_character_list:
        if safe_character not in astring:
            result.append(safe_character)
    return result


def parse_database_url(original_senzing_database_url):
    ''' Given a canonical database URL, decompose into URL components. '''

    result = {}

    # Get the value of SENZING_DATABASE_URL environment variable.

    senzing_database_url = original_senzing_database_url

    # Create lists of safe and unsafe characters.

    unsafe_characters = get_unsafe_characters(senzing_database_url)
    safe_characters = get_safe_characters(senzing_database_url)

    # Detect an error condition where there are not enough safe characters.

    if len(unsafe_characters) > len(safe_characters):
        logging.error(message_error(730, unsafe_characters, safe_characters))
        return result

    # Perform translation.
    # This makes a map of safe character mapping to unsafe characters.
    # "senzing_database_url" is modified to have only safe characters.

    translation_map = {}
    safe_characters_index = 0
    for unsafe_character in unsafe_characters:
        safe_character = safe_characters[safe_characters_index]
        safe_characters_index += 1
        translation_map[safe_character] = unsafe_character
        senzing_database_url = senzing_database_url.replace(unsafe_character, safe_character)

    # Parse "translated" URL.

    parsed = urlparse(senzing_database_url)
    schema = parsed.path.strip('/')

    # Construct result.

    result = {
        'scheme': translate(translation_map, parsed.scheme),
        'netloc': translate(translation_map, parsed.netloc),
        'path': translate(translation_map, parsed.path),
        'params': translate(translation_map, parsed.params),
        'query': translate(translation_map, parsed.query),
        'fragment': translate(translation_map, parsed.fragment),
        'username': translate(translation_map, parsed.username),
        'password': translate(translation_map, parsed.password),
        'hostname': translate(translation_map, parsed.hostname),
        'port': translate(translation_map, parsed.port),
        'schema': translate(translation_map, schema),
    }

    # For safety, compare original URL with reconstructed URL.

    url_parts = [
        result.get('scheme'),
        result.get('netloc'),
        result.get('path'),
        result.get('params'),
        result.get('query'),
        result.get('fragment'),
    ]
    test_senzing_database_url = urlunparse(url_parts)
    if test_senzing_database_url != original_senzing_database_url:
        logging.warning(message_warning(891, original_senzing_database_url, test_senzing_database_url))

    # Return result.

    return result

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_g2_database_url_specific(generic_database_url):
    ''' Given a canonical database URL, transform to the specific URL. '''

    result = ""
    parsed_database_url = parse_database_url(generic_database_url)
    scheme = parsed_database_url.get('scheme')

    # Format database URL for a particular database.

    if scheme in ['mysql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}/?schema={schema}".format(**parsed_database_url)
    elif scheme in ['postgresql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}:{schema}/".format(**parsed_database_url)
    elif scheme in ['db2']:
        result = "{scheme}://{username}:{password}@{schema}".format(**parsed_database_url)
    elif scheme in ['sqlite3']:
        result = "{scheme}://{netloc}{path}".format(**parsed_database_url)
    elif scheme in ['mssql']:
        result = "{scheme}://{username}:{password}@{schema}".format(**parsed_database_url)
    else:
        logging.error(message_error(695, scheme, generic_database_url))

    return result


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default. '''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = ['debug']
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = [
        "delay_in_seconds",
        'expiration_warning_in_days',
        'log_license_period_in_seconds',
        'monitoring_period_in_seconds',
        'queue_maxsize',
        'redo_sleep_time_in_seconds',
        'sleep_time_in_seconds',
        'threads_per_process'
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case:  Tailored database URL

    result['g2_database_url_specific'] = get_g2_database_url_specific(result.get("g2_database_url_generic"))

    result['counter_processed_records'] = 0
    result['counter_queued_records'] = 0

    return result


def validate_configuration(config):
    ''' Check aggregate configuration from commandline options, environment variables, config files, and defaults. '''

    user_warning_messages = []
    user_error_messages = []

    if not config.get('g2_database_url_generic'):
        user_error_messages.append(message_error(551))

    # Perform subcommand specific checking.

    subcommand = config.get('subcommand')

    if subcommand in ['task1', 'task2']:
        pass

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warning(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(293))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(697)


def redact_configuration(config):
    ''' Return a shallow copy of config with certain keys removed. '''
    result = config.copy()
    for key in keys_to_redact:
        try:
            result.pop(key)
        except:
            pass
    return result

# -----------------------------------------------------------------------------
# Class: Governor
# -----------------------------------------------------------------------------


class Governor:

    def __init__(self, g2_engine=None, hint=None, *args, **kwargs):
        self.g2_engine = g2_engine
        self.hint = hint

    def govern(self, *args, **kwargs):
        return

# -----------------------------------------------------------------------------
# Class: InfoFilter
# -----------------------------------------------------------------------------


class InfoFilter:

    def __init__(self, g2_engine=None):
        self.g2_engine = g2_engine

    def filter(self, line=None):
        return line

# -----------------------------------------------------------------------------
# Class: MonitorThread
# -----------------------------------------------------------------------------


class MonitorThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, workers=None):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.workers = workers
        # FIXME: self.last_daily = datetime.

    def run(self):
        '''Periodically monitor what is happening.'''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Initialize variables.

        last_processed_records = 0
        last_queued_records = 0
        last_time = time.time()
        last_log_license = time.time()
        log_license_period_in_seconds = self.config.get("log_license_period_in_seconds")

        # Define monitoring report interval.

        sleep_time_in_seconds = self.config.get('monitoring_period_in_seconds')

        # Sleep-monitor loop.

        active_workers = len(self.workers)
        for worker in self.workers:
            if not worker.is_alive():
                active_workers -= 1

        while active_workers > 0:

            time.sleep(sleep_time_in_seconds)

            # Calculate active Threads.

            active_workers = len(self.workers)
            for worker in self.workers:
                if not worker.is_alive():
                    active_workers -= 1

            # Determine if we're running out of workers.

            if (active_workers / float(len(self.workers))) < 0.5:
                logging.warning(message_warning(721))

            # Calculate times.

            now = time.time()
            uptime = now - self.config.get('start_time', now)
            elapsed_time = now - last_time
            elapsed_log_license = now - last_log_license

            # Log license periodically to show days left in license.

            if elapsed_log_license > log_license_period_in_seconds:
                log_license(self.config)
                last_log_license = now

            # Calculate rates.

            processed_records_total = self.config['counter_processed_records']
            processed_records_interval = processed_records_total - last_processed_records

            queued_records_total = self.config['counter_queued_records']
            queued_records_interval = queued_records_total - last_queued_records

            # Construct and log monitor statistics.

            stats = {
                "processed_records_interval": processed_records_interval,
                "processed_records_total": processed_records_total,
                "queued_records_interval": queued_records_interval,
                "queued_records_total": queued_records_total,
                "uptime": int(uptime),
                "workers_total": len(self.workers),
                "workers_active": active_workers,
            }
            logging.info(message_info(127, json.dumps(stats, sort_keys=True)))

            # Log engine statistics with sorted JSON keys.

            g2_engine_stats_response = bytearray()
            self.g2_engine.stats(g2_engine_stats_response)
            g2_engine_stats_dictionary = json.loads(g2_engine_stats_response.decode())
            logging.info(message_info(125, json.dumps(g2_engine_stats_dictionary, sort_keys=True)))

            # Store values for next iteration of loop.

            last_processed_records = processed_records_total
            last_queued_records = queued_records_total
            last_time = now

# -----------------------------------------------------------------------------
# Class: ProcessMixin
# -----------------------------------------------------------------------------


class ProcessMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "ProcessMixin"))

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        This method uses G2Engine.process()
        The method can be sub-classed to call other G2Engine methods.
        '''

        try:
            self.g2_engine.process(redo_record)
        except G2Exception.G2ModuleNotInitialized as err:
            exit_error(707, err, redo_record_bytearray.decode())
        except Exception as err:
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                return_code = self.g2_engine.process(redo_record)
            else:
                exit_error(709, err)

# -----------------------------------------------------------------------------
# Class: ProcessWithInfoMixin
# -----------------------------------------------------------------------------


class ProcessWithInfoMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "ProcessWithInfoMixin"))
        self.g2_engine_flags = 0

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        This method uses G2Engine.processRedoRecordWithInfo()
        '''

        logging.debug(message_debug(905, threading.current_thread().name, redo_record))

        # Transform redo_record string to bytearray.

        redo_record_bytearray = bytearray(redo_record.encode())

        # Additional parameters for processRedoRecordWithInfo().

        info_bytearray = bytearray()

        try:
            self.g2_engine.processRedoRecordWithInfo(redo_record_bytearray, info_bytearray, self.g2_engine_flags)
        except G2Exception.G2ModuleNotInitialized as err:
            self.send_to_failure_queue(redo_record)
            exit_error(707, err, info_bytearray.decode())
        except Exception as err:
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                self.g2_engine.processRedoRecordWithInfo(redo_record_bytearray, info_bytearray)
            else:
                self.send_to_failure_queue(redo_record)
                exit_error(709, err)

        info_json = info_bytearray.decode()

        # Allow user to manipulate the message.

        filtered_info_json = self.filter_info_message(line=info_json)

#         # Put "info" on info queue.

        if filtered_info_json:
            self.send_to_info_queue(filtered_info_json)
            logging.debug(message_debug(904, threading.current_thread().name, filtered_info_json))

# -----------------------------------------------------------------------------
# Class: ProcessWriteToRabbitmqMixin
# -----------------------------------------------------------------------------


class ProcessWriteToRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "ProcessWriteToQueueMixin"))

        # Pull values from configuration.

        rabbitmq_host = self.config.get("rabbitmq_redo_host")
        self.rabbitmq_queue = self.config.get("rabbitmq_redo_queue")
        rabbitmq_username = self.config.get("rabbitmq_redo_username")
        rabbitmq_password = self.config.get("rabbitmq_redo_password")

        # Connect to the RabbitMQ host for failure_channel.

        try:
            credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            self.failure_channel = connection.channel()
            self.failure_channel.queue_declare(queue=self.rabbitmq_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, err, rabbitmq_host)
        except BaseException as err:
            exit_error(410, err)

        # Connect to the RabbitMQ host for info_channel.

        try:
            credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            self.info_channel = connection.channel()
            self.info_channel.queue_declare(queue=self.rabbitmq_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, err, rabbitmq_host)
        except BaseException as err:
            exit_error(410, err)

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        Simply send to RabbitMQ.
        '''
        try:
            self.info_channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_queue,
                body=redo_record,
                properties=pika.BasicProperties(
                    delivery_mode=1  # Make message non-persistent
                )
            )
        except BaseException as err:
            logging.warn(message_warning(411, err, redo_record))
        logging.info(message_info(128, redo_record))

# -----------------------------------------------------------------------------
# Class: InputInternalMixin
# -----------------------------------------------------------------------------


class InputInternalMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "InputInternalMixin"))

    def redo_records(self):
        '''
        Generator that produces Senzing redo records.
        Note: This method uses the "internal queue".
        This method can be sub-classed to use external queues.
        '''
        while True:
            yield self.redo_queue.get()

# -----------------------------------------------------------------------------
# Class: InputKafkaMixin
# -----------------------------------------------------------------------------


class InputKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "InputKafkaMixin"))

    def redo_records(self):
        pass

# -----------------------------------------------------------------------------
# Class: InputRabbitmqMixin
# -----------------------------------------------------------------------------


class InputRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "InputRabbitmqMixin"))

    def redo_records(self):
        pass

# -----------------------------------------------------------------------------
# Class: OutputInternalMixin
# -----------------------------------------------------------------------------


class OutputInternalMixin():
    ''' This is a "null object". '''

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "OutputInternalMixin"))

    def send_to_failure_queue(self, jsonline):
        logging.info(message_info(121, jsonline))

    def send_to_info_queue(self, jsonline):
        logging.info(message_info(128, jsonline))

# -----------------------------------------------------------------------------
# Class: OutputKafkaMixin
# -----------------------------------------------------------------------------


class OutputKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "OutputKafkaMixin"))

    def send_to_failure_queue(self, jsonline):
        '''Default behavior. This may be implemented in the subclass.'''
        logging.info(message_info(121, jsonline))

    def send_to_info_queue(self, jsonline):
        '''Default behavior. This may be implemented in the subclass.'''
        logging.info(message_info(128, jsonline))

# -----------------------------------------------------------------------------
# Class: OutputRabbitmqMixin
# -----------------------------------------------------------------------------


class OutputRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "OutputRabbitmqMixin"))

        # Pull values from configuration.

        rabbitmq_failure_host = self.config.get("rabbitmq_failure_host")
        self.rabbitmq_failure_queue = self.config.get("rabbitmq_failure_queue")
        rabbitmq_failure_username = self.config.get("rabbitmq_failure_username")
        rabbitmq_failure_password = self.config.get("rabbitmq_failure_password")
        rabbitmq_info_host = self.config.get("rabbitmq_info_host")
        self.rabbitmq_info_queue = self.config.get("rabbitmq_info_queue")
        rabbitmq_info_username = self.config.get("rabbitmq_info_username")
        rabbitmq_info_password = self.config.get("rabbitmq_info_password")

        # Connect to the RabbitMQ host for failure_channel.

        try:
            credentials = pika.PlainCredentials(rabbitmq_failure_username, rabbitmq_failure_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_failure_host, credentials=credentials))
            self.failure_channel = connection.channel()
            self.failure_channel.queue_declare(queue=self.rabbitmq_failure_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, err, rabbitmq_failure_host)
        except BaseException as err:
            exit_error(410, err)

        # Connect to the RabbitMQ host for info_channel.

        try:
            credentials = pika.PlainCredentials(rabbitmq_info_username, rabbitmq_info_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_info_host, credentials=credentials))
            self.info_channel = connection.channel()
            self.info_channel.queue_declare(queue=self.rabbitmq_info_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, err, rabbitmq_info_host)
        except BaseException as err:
            exit_error(410, err)

    def send_to_failure_queue(self, jsonline):
        try:
            self.failure_channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_failure_queue,
                body=jsonline,
                properties=pika.BasicProperties(
                    delivery_mode=1  # Make message non-persistent
                )
            )
        except BaseException as err:
            logging.warn(message_warning(411, err, jsonline))
        logging.info(message_info(121, jsonline))

    def send_to_info_queue(self, jsonline):
        try:
            self.info_channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_info_queue,
                body=jsonline,
                properties=pika.BasicProperties(
                    delivery_mode=1  # Make message non-persistent
                )
            )
        except BaseException as err:
            logging.warn(message_warning(411, err, jsonline))
        logging.info(message_info(128, jsonline))

# -----------------------------------------------------------------------------
# Class: QueueInternalMixin
# -----------------------------------------------------------------------------


class QueueInternalMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "QueueInternalMixin"))

    def send_to_redo_queue(self, redo_record):
        self.redo_queue.put(redo_record)

# -----------------------------------------------------------------------------
# Class: QueueKafkaMixin
# -----------------------------------------------------------------------------


class QueueKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "QueueKafkaMixin"))

    def send_to_redo_queue(self, redo_record):
        logging.info(message_info(131, redo_record))

# -----------------------------------------------------------------------------
# Class: QueueRabbitmqMixin
# -----------------------------------------------------------------------------


class QueueRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.info(message_info(132, "QueueRabbitmqMixin"))

    def send_to_redo_queue(self, redo_record):
        logging.info(message_info(131, redo_record))

# -----------------------------------------------------------------------------
# Class: QueueRedoRecordsThread
# -----------------------------------------------------------------------------


class QueueRedoRecordsThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, redo_queue=None):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.redo_queue = redo_queue

    def redo_records(self):
        '''A generator for producing Senzing redo records.'''

        # Pull values from configuration.

        redo_sleep_time_in_seconds = self.config.get('redo_sleep_time_in_seconds')

        # Initialize variables.

        redo_record_bytearray = bytearray()
        return_code = 0

        # Read forever.

        while True:

            # Read a Senzing redo record.

            try:
                return_code = self.g2_engine.getRedoRecord(redo_record_bytearray)
            except G2Exception.G2ModuleNotInitialized as err:
                exit_error(702, err, redo_record_bytearray.decode())
            except Exception as err:
                exit_error(703, err)
            if return_code:
                exit_error(701, return_code)

            # If redo record was not received, sleep and try again.

            redo_record = redo_record_bytearray.decode()
            if not redo_record:
                time.sleep(redo_sleep_time_in_seconds)
                continue

            # Return generator value.

            yield redo_record

    def run(self):
        '''Get redo records from Senzing.  Put redo records in internal queue.'''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Transfer messages from Senzing to internal queue.

        for redo_record in self.redo_records():
            logging.debug(message_debug(902, threading.current_thread().name, redo_record))
            self.send_to_redo_queue(redo_record)
            self.config['counter_queued_records'] += 1

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: QueueRedoRecordsThread
# -----------------------------------------------------------------------------


class QueueRedoRecordsInternalThread(QueueRedoRecordsThread, QueueInternalMixin):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, g2_configuration_manager=None, redo_queue=None):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.g2_configuration_manager = g2_configuration_manager
        self.governor = Governor(g2_engine=g2_engine, hint="redoer.ProcessRedoQueueThread")
        self.info_filter = InfoFilter(g2_engine=g2_engine)
        self.redo_queue = redo_queue

    def filter_info_message(self, line=None):
        return self.info_filter.filter(line=line)

    def govern(self):
        return self.governor.govern()

    def is_g2_default_configuration_changed(self):

        # Get active Configuration ID being used by g2_engine.

        active_config_id = bytearray()
        self.g2_engine.getActiveConfigID(active_config_id)

        # Get most current Configuration ID from G2 database.

        default_config_id = bytearray()
        self.g2_configuration_manager.getDefaultConfigID(default_config_id)

        # Determine if configuration has changed.

        result = active_config_id != default_config_id
        if result:
            logging.info(message_info(292, active_config_id.decode(), default_config_id.decode()))

        return result

    def update_active_g2_configuration(self):

        # Get most current Configuration ID from G2 database.

        default_config_id = bytearray()
        self.g2_configuration_manager.getDefaultConfigID(default_config_id)

        # Apply new configuration to g2_engine.

        self.g2_engine.reinitV2(default_config_id)

    def run(self):
        ''' Process Senzing redo records. '''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Process redo records.

        return_code = 0
        for redo_record in self.redo_records():

            # Invoke Governor.

            self.govern()

            # Process record.

            logging.debug(message_debug(903, threading.current_thread().name, redo_record))
            self.process_redo_record(redo_record)
            self.config['counter_processed_records'] += 1

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: WriteToQueueThread
# -----------------------------------------------------------------------------


class WriteToQueueThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, g2_configuration_manager=None, redo_queue=None):
        threading.Thread.__init__(self)
        self.config = config
        self.governor = Governor(g2_engine=None, hint="redoer.WriteToQueueThread")
        self.redo_queue = redo_queue

    def govern(self):
        return self.governor.govern()

    def run(self):
        ''' Process Senzing redo records. '''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Process redo records.

        return_code = 0
        for redo_record in self.redo_records():

            # Invoke Governor.

            self.govern()

            # Process record.

            logging.debug(message_debug(903, threading.current_thread().name, redo_record))
            self.process_redo_record(redo_record)
            self.config['counter_processed_records'] += 1

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueWithInfoThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueInternalThread(ProcessRedoQueueThread, InputInternalMixin, ProcessMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueWithInfoInternalThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueWithInfoInternalThread(ProcessRedoQueueThread, InputInternalMixin, ProcessWithInfoMixin, OutputRabbitmqMixin):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueWithInfoKafkaThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueWithInfoKafkaThread(ProcessRedoQueueThread):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueWithInfoRabbitmqThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueWithInfoRabbitmqThread(ProcessRedoQueueThread):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueWithInfoInternalThread
# -----------------------------------------------------------------------------


# FIXME: MJD
class ProcessRedoQueueWriteRabbitmqThread(WriteToQueueThread, InputInternalMixin, ProcessWriteToRabbitmqMixin):

    def __init__(self, *args, **kwargs):
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def create_signal_handler_function(args):
    ''' Tricky code.  Uses currying technique. Create a function for signal handling.
        that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(298, args))
        sys.exit(0)

    return result_function


def delay(config):
    delay_in_seconds = config.get('delay_in_seconds')
    if delay_in_seconds > 0:
        logging.info(message_info(120, delay_in_seconds))
        time.sleep(delay_in_seconds)


def entry_template(config):
    ''' Format of entry message. '''
    debug = config.get("debug", False)
    config['start_time'] = time.time()
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(297, config_json)


def exit_template(config):
    ''' Format of exit message. '''
    debug = config.get("debug", False)
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(298, config_json)


def exit_error(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(698))
    sys.exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(0)

# -----------------------------------------------------------------------------
# Senzing configuration.
# -----------------------------------------------------------------------------


def get_g2_configuration_dictionary(config):
    ''' Construct a dictionary in the form of the old ini files. '''
    result = {
        "PIPELINE": {
            "CONFIGPATH": config.get("config_path"),
            "RESOURCEPATH": config.get("resource_path"),
            "SUPPORTPATH": config.get("support_path"),
        },
        "SQL": {
            "CONNECTION": config.get("g2_database_url_specific"),
        }
    }
    return result


def get_g2_configuration_json(config):
    result = ""
    if config.get('engine_configuration_json'):
        result = config.get('engine_configuration_json')
    else:
        result = json.dumps(get_g2_configuration_dictionary(config))
    return result

# -----------------------------------------------------------------------------
# Senzing services.
# -----------------------------------------------------------------------------


def get_g2_configuration_manager(config, g2_configuration_manager_name="loader-G2-configuration-manager"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2ConfigMgr()
        result.initV2(g2_configuration_manager_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(896, g2_configuration_json, err)
    return result


def get_g2_engine(config, g2_engine_name="loader-G2-engine"):
    '''Get the G2Engine resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Engine()
        result.initV2(g2_engine_name, g2_configuration_json, config.get('debug', False))
        config['last_configuration_check'] = time.time()
    except G2Exception.G2ModuleException as err:
        exit_error(898, g2_configuration_json, err)
    return result


def get_g2_product(config, g2_product_name="loader-G2-product"):
    '''Get the G2Product resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Product()
        result.initV2(g2_product_name, g2_configuration_json, config.get('debug'))
    except G2Exception.G2ModuleException as err:
        exit_error(892, config.get('g2project_ini'), err)
    return result

# -----------------------------------------------------------------------------
# Log information.
# -----------------------------------------------------------------------------


def log_license(config):
    '''Capture the license and version info in the log.'''

    g2_product = get_g2_product(config)
    license = json.loads(g2_product.license())
    version = json.loads(g2_product.version())

    logging.info(message_info(160, '-' * 20))
    if 'VERSION' in version:
        logging.info(message_info(161, version['VERSION'], version['BUILD_DATE']))
    if 'customer' in license:
        logging.info(message_info(162, license['customer']))
    if 'licenseType' in license:
        logging.info(message_info(163, license['licenseType']))
    if 'expireDate' in license:
        logging.info(message_info(164, license['expireDate']))

        # Calculate days remaining.

        expire_date = datetime.datetime.strptime(license['expireDate'], '%Y-%m-%d')
        today = datetime.datetime.today()
        remaining_time = expire_date - today
        if remaining_time.days > 0:
            logging.info(message_info(165, remaining_time.days))
            expiration_warning_in_days = config.get('expiration_warning_in_days')
            if remaining_time.days < expiration_warning_in_days:
                logging.warning(message_warning(203, remaining_time.days))
        else:
            logging.info(message_info(168, abs(remaining_time.days)))

        # Issue warning if license is about to expire.

    if 'recordLimit' in license:
        logging.info(message_info(166, license['recordLimit']))
    if 'contract' in license:
        logging.info(message_info(167, license['contract']))
    logging.info(message_info(299, '-' * 49))

    # Garbage collect g2_product.

    g2_product.destroy()

    # If license has expired, exit with error.

    if remaining_time.days < 0:
        exit_error(885)

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_docker_acceptance_test(args):
    ''' For use with Docker acceptance testing. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Epilog.

    logging.info(exit_template(config))


def do_redo(args):
    ''' Process Senzing's Redo queue. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # If requested, delay start.

    delay(config)

    # Write license information to log.

    log_license(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')
    queue_maxsize = config.get('queue_maxsize')

    # Create internal Queue.

    redo_queue = multiprocessing.Queue(queue_maxsize)

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create threads for master process.

    threads = []

    # Add a single thread for reading from Senzing Redo queue and placing on internal queue.

    thread = QueueRedoRecordsInternalThread(
        config=config,
        g2_engine=g2_engine,
        redo_queue=redo_queue
    )
    thread.name = "QueueRedoRecordsInternal-0-thread-1"
    threads.append(thread)

    # Add a number of threads for processing Redo records from internal queue.

    for i in range(0, threads_per_process):
        thread = ProcessRedoQueueInternalThread(
            config=config,
            g2_engine=g2_engine,
            g2_configuration_manager=g2_configuration_manager,
            redo_queue=redo_queue
        )
        thread.name = "ProcessRedoQueueInternal-0-thread-{0}".format(i)
        threads.append(thread)

    # Add a monitoring thread.

    adminThreads = []
    thread = MonitorThread(
        config=config,
        g2_engine=g2_engine,
        workers=threads
    )
    thread.name = "Monitor-0-thread-0"
    adminThreads.append(thread)

    # Start threads.

    for thread in threads:
        thread.start()

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_redo_with_info_rabbitmq(args):
    ''' Process Senzing's Redo queue. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # If configuration values not specified, use defaults.

    options_to_defaults_map = {
        "rabbitmq_failure_host": "rabbitmq_host",
        "rabbitmq_failure_password": "rabbitmq_password",
        "rabbitmq_failure_username": "rabbitmq_username",
        "rabbitmq_info_host": "rabbitmq_host",
        "rabbitmq_info_password": "rabbitmq_password",
        "rabbitmq_info_username": "rabbitmq_username",
    }

    for key, value in options_to_defaults_map.items():
        if not config.get(key):
            config[key] = config.get(value)

    # Prolog.

    logging.info(entry_template(config))

    # If requested, delay start.

    delay(config)

    # Write license information to log.

    log_license(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')
    queue_maxsize = config.get('queue_maxsize')

    # Create internal Queue.

    redo_queue = multiprocessing.Queue(queue_maxsize)

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create threads for master process.

    threads = []

    # Add a single thread for reading from Senzing Redo queue and placing on internal queue.

    thread = QueueRedoRecordsInternalThread(
        config=config,
        g2_engine=g2_engine,
        redo_queue=redo_queue
    )
    thread.name = "QueueRedoRecordsInternal-0-thread-1"
    threads.append(thread)

    # Add a number of threads for processing Redo records from internal queue.

    for i in range(0, threads_per_process):
        thread = ProcessRedoQueueWithInfoInternalThread(
            config=config,
            g2_engine=g2_engine,
            g2_configuration_manager=g2_configuration_manager,
            redo_queue=redo_queue
        )
        thread.name = "ProcessRedoQueueWithInfoInternal-0-thread-{0}".format(i)
        threads.append(thread)

    # Add a monitoring thread.

    adminThreads = []
    thread = MonitorThread(
        config=config,
        g2_engine=g2_engine,
        workers=threads
    )
    thread.name = "Monitor-0-thread-0"
    adminThreads.append(thread)

    # Start threads.

    for thread in threads:
        thread.start()

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_sleep(args):
    ''' Sleep.  Used for debugging. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')

    # Sleep

    if sleep_time_in_seconds > 0:
        logging.info(message_info(296, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    else:
        sleep_time_in_seconds = 3600
        while True:
            logging.info(message_info(295))
            time.sleep(sleep_time_in_seconds)

    # Epilog.

    logging.info(exit_template(config))


def do_write_to_rabbitmq(args):
    ''' Read from Senzing's Redo queue, write to RabbitMQ '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # If configuration values not specified, use defaults.

    options_to_defaults_map = {
        "rabbitmq_redo_host": "rabbitmq_host",
        "rabbitmq_redo_password": "rabbitmq_password",
        "rabbitmq_redo_username": "rabbitmq_username",
    }

    for key, value in options_to_defaults_map.items():
        if not config.get(key):
            config[key] = config.get(value)

    # Prolog.

    logging.info(entry_template(config))

    # If requested, delay start.

    delay(config)

    # Write license information to log.

    log_license(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')
    queue_maxsize = config.get('queue_maxsize')

    # Create internal Queue.

    redo_queue = multiprocessing.Queue(queue_maxsize)

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create threads for master process.

    threads = []

    # Add a single thread for reading from Senzing Redo queue and placing on internal queue.

    thread = QueueRedoRecordsInternalThread(
        config=config,
        g2_engine=g2_engine,
        redo_queue=redo_queue
    )
    thread.name = "QueueRedoRecordsInternal-0-thread-1"
    threads.append(thread)

    # Add a number of threads for processing Redo records from internal queue.

    for i in range(0, threads_per_process):
        thread = ProcessRedoQueueWriteRabbitmqThread(
            config=config,
            g2_engine=g2_engine,
            g2_configuration_manager=g2_configuration_manager,
            redo_queue=redo_queue
        )
        thread.name = "ProcessRedoQueueWriteRabbitmq-0-thread-{0}".format(i)
        threads.append(thread)

    # Add a monitoring thread.

    adminThreads = []
    thread = MonitorThread(
        config=config,
        g2_engine=g2_engine,
        workers=threads
    )
    thread.name = "Monitor-0-thread-0"
    adminThreads.append(thread)

    # Start threads.

    for thread in threads:
        thread.start()

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    ''' Log version information. '''

    logging.info(message_info(294, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)
    logging.debug(message_debug(998))

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        if len(os.getenv("SENZING_DOCKER_LAUNCHED", "")):
            subcommand = "sleep"
            args = argparse.Namespace(subcommand=subcommand)
            do_sleep(args)
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warning(message_warning(696, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
