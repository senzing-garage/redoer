#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# redoer.py
# -----------------------------------------------------------------------------

import argparse
from glob import glob
import json
import linecache
import logging
import multiprocessing
import os
import signal
import string
import sys
import threading
import time
from urllib.parse import urlparse, urlunparse

# Import Senzing libraries.
try:
    from G2ConfigMgr import G2ConfigMgr
    from G2Engine import G2Engine
    import G2Exception
except ImportError:
    pass

__all__ = []
__version__ = "1.0.0"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2020-01-15'
__updated__ = '2020-01-18'

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
    "engine_configuration_json": {
        "default": None,
        "env": "SENZING_ENGINE_CONFIGURATION_JSON",
        "cli": "engine-configuration-json"
    },
    "g2_database_url_generic": {
        "default": "sqlite3://na:na@/var/opt/senzing/sqlite/G2C.db",
        "env": "SENZING_DATABASE_URL",
        "cli": "database-url"
    },
    "password": {
        "default": None,
        "env": "SENZING_PASSWORD",
        "cli": "password"
    },
    "queue_maxsize": {
        "default": 10,
        "env": "SENZING_QUEUE_MAX_SIZE",
        "cli": "queue-max-size"
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
    "senzing_dir": {
        "default": "/opt/senzing",
        "env": "SENZING_DIR",
        "cli": "senzing-dir"
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
    "129": "{0} is running.",
    "130": "{0} has exited.",
    "292": "Configuration change detected.  Old: {0} New: {1}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
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
        'redo_sleep_time_in_seconds',
        'sleep_time_in_seconds'
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case:  Tailored database URL

    result['g2_database_url_specific'] = get_g2_database_url_specific(result.get("g2_database_url_generic"))

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

        if not config.get('senzing_dir'):
            user_error_messages.append(message_error(414))

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
# Class: ReadRedoQueueThread
# -----------------------------------------------------------------------------


class ReadRedoQueueThread(threading.Thread):

    def __init__(self, config, g2_engine, redo_queue):
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
            self.redo_queue.put(redo_record)

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueThread(threading.Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager, redo_queue):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.g2_configuration_manager = g2_configuration_manager
        self.redo_queue = redo_queue

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

    def redo_records(self):
        '''Generator that produces Senzing redo records.'''
        while True:
            yield self.redo_queue.get()

    def run(self):
        '''Process Senzing redo records.'''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Process redo records.

        return_code = 0
        for redo_record in self.redo_records():

            logging.debug(message_debug(903, threading.current_thread().name, redo_record))

            try:
                return_code = self.g2_engine.process(redo_record)
            except G2Exception.G2ModuleNotInitialized as err:
                exit_error(707, err, redo_record_bytearray.decode())
#           except G2Exception.G2ModuleGenericException as err:
#               exit_error(708, err, redo_record_bytearray.decode())
            except Exception as err:
                if self.is_g2_default_configuration_changed():
                    self.update_active_g2_configuration()
                    return_code = self.g2_engine.process(redo_record)
                else:
                    exit_error(709, err)
            if return_code:
                exit_error(706, return_code)

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

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

    thread = ReadRedoQueueThread(config, g2_engine, redo_queue)
    thread.name = "ReadRedoQueue-0-thread-1"
    threads.append(thread)

    # Add a number of threads for processing Redo records from internal queue.

    for i in range(0, threads_per_process):
        thread = ProcessRedoQueueThread(config, g2_engine, g2_configuration_manager, redo_queue)
        thread.name = "ProcessRedoQueue-0-thread-{0}".format(i)
        threads.append(thread)

    # Start threads.

    for thread in threads:
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
