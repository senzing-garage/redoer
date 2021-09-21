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

from urllib.parse import urlparse, urlunparse
import argparse
import boto3
import confluent_kafka
import datetime
import json
import linecache
import logging
import queue
import os
import pika
import re
import signal
import string
import subprocess
import sys
import threading
import time
import functools

# Import Senzing libraries.
try:
    from G2ConfigMgr import G2ConfigMgr
    from G2Engine import G2Engine
    from G2Product import G2Product
    import G2Exception
except ImportError:
    pass

__all__ = []
__version__ = "1.4.0"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2020-01-15'
__updated__ = '2021-09-21'

SENZING_PRODUCT_ID = "5010"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# Working with bytes.

KILOBYTES = 1024
MEGABYTES = 1024 * KILOBYTES
GIGABYTES = 1024 * MEGABYTES

# Lists from https://www.ietf.org/rfc/rfc1738.txt

safe_character_list = ['$', '-', '_', '.', '+', '!', '*', '(', ')', ',', '"'] + list(string.ascii_letters)
unsafe_character_list = ['"', '<', '>', '#', '%', '{', '}', '|', '\\', '^', '~', '[', ']', '`']
reserved_character_list = [';', ',', '/', '?', ':', '@', '=', '&']

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "azure_connection_string": {
        "default": None,
        "env": "SENZING_AZURE_CONNECTION_STRING",
        "cli": "azure-connection-string"
    },
    "azure_failure_connection_string": {
        "default": None,
        "env": "SENZING_AZURE_FAILURE_CONNECTION_STRING",
        "cli": "azure-failure-connection-string"
    },
    "azure_failure_queue_name": {
        "default": None,
        "env": "SENZING_AZURE_FAILURE_QUEUE_NAME",
        "cli": "azure-failure-queue-name"
    },
    "azure_info_connection_string": {
        "default": None,
        "env": "SENZING_AZURE_INFO_CONNECTION_STRING",
        "cli": "azure-info-connection-string"
    },
    "azure_info_queue_name": {
        "default": None,
        "env": "SENZING_AZURE_INFO_QUEUE_NAME",
        "cli": "azure-info-queue-name"
    },
    "azure_queue_name": {
        "default": None,
        "env": "SENZING_AZURE_QUEUE_NAME",
        "cli": "azure-queue-name"
    },
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
    "exit_on_thread_termination": {
        "default": False,
        "env": "SENZING_EXIT_ON_THREAD_TERMINATION",
        "cli": "exit-on-thread-termination"
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
    "kafka_bootstrap_server": {
        "default": "localhost:9092",
        "env": "SENZING_KAFKA_BOOTSTRAP_SERVER",
        "cli": "kafka-bootstrap-server",
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
        "cli": "kafka-info-topic"
    },
    "kafka_redo_bootstrap_server": {
        "default": None,
        "env": "SENZING_KAFKA_REDO_BOOTSTRAP_SERVER",
        "cli": "kafka-redo-bootstrap-server",
    },
    "kafka_redo_group": {
        "default": "senzing-kafka-redo-group",
        "env": "SENZING_KAFKA_REDO_GROUP",
        "cli": "kafka-redo-group"
    },
    "kafka_redo_topic": {
        "default": "senzing-kafka-redo-topic",
        "env": "SENZING_KAFKA_REDO_TOPIC",
        "cli": "kafka-redo-topic"
    },
    "log_level_parameter": {
        "default": "info",
        "env": "SENZING_LOG_LEVEL",
        "cli": "log-level-parameter"
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
    "pstack_pid": {
        "default": "1",
        "env": "SENZING_PSTACK_PID",
        "cli": "pstack-pid",
    },
    "queue_maxsize": {
        "default": 10,
        "env": "SENZING_QUEUE_MAX_SIZE",
        "cli": "queue-max-size"
    },
    "rabbitmq_delivery_mode": {
        "default": 1,
        "env": "SENZING_RABBITMQ_DELIVERY_MODE",
        "cli": "rabbitmq-delivery-mode",
    },
    "rabbitmq_exchange": {
        "default": "senzing-rabbitmq-exchange",
        "env": "SENZING_RABBITMQ_EXCHANGE",
        "cli": "rabbitmq-exchange",
    },
    "rabbitmq_failure_exchange": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_EXCHANGE",
        "cli": "rabbitmq-failure-exchange",
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
    "rabbitmq_failure_routing_key": {
        "default": "senzing.failure",
        "env": "SENZING_RABBITMQ_FAILURE_ROUTING_KEY",
        "cli": "rabbitmq-failure-routing-key",
    },
    "rabbitmq_failure_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_USERNAME",
        "cli": "rabbitmq-failure-username",
    },
    "rabbitmq_failure_virtual_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_VIRTUAL_HOST",
        "cli": "rabbitmq-failure-virtual-host",
    },
    "rabbitmq_heartbeat_in_seconds": {
        "default": 60,
        "env": "SENZING_RABBITMQ_HEARTBEAT_IN_SECONDS",
        "cli": "rabbitmq-heartbeat-in-seconds",
    },
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_info_exchange": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_EXCHANGE",
        "cli": "rabbitmq-info-exchange",
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
    "rabbitmq_info_routing_key": {
        "default": "senzing.info",
        "env": "SENZING_RABBITMQ_INFO_ROUTING_KEY",
        "cli": "rabbitmq-info-routing-key",
    },
    "rabbitmq_info_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_USERNAME",
        "cli": "rabbitmq-info-username",
    },
    "rabbitmq_info_virtual_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_VIRTUAL_HOST",
        "cli": "rabbitmq-info-virtual-host",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "rabbitmq_prefetch_count": {
        "default": 50,
        "env": "SENZING_RABBITMQ_PREFETCH_COUNT",
        "cli": "rabbitmq-prefetch-count",
    },
    "rabbitmq_redo_exchange": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_EXCHANGE",
        "cli": "rabbitmq-redo-exchange",
    },
    "rabbitmq_reconnect_delay_in_seconds": {
        "default": "60",
        "env": "SENZING_RABBITMQ_RECONNECT_DELAY_IN_SECONDS",
        "cli": "rabbitmq-reconnect-wait-time-in-seconds",
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
        "cli": "rabbitmq-redo-queue",
    },
    "rabbitmq_redo_routing_key": {
        "default": "senzing.redo",
        "env": "SENZING_RABBITMQ_REDO_ROUTING_KEY",
        "cli": "rabbitmq-redo-routing-key",
    },
    "rabbitmq_redo_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_USERNAME",
        "cli": "rabbitmq-redo-username",
    },
    "rabbitmq_redo_virtual_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_REDO_VIRTUAL_HOST",
        "cli": "rabbitmq-redo-virtual-host",
    },
    "rabbitmq_use_existing_entities": {
        "default": True,
        "env": "SENZING_RABBITMQ_USE_EXISTING_ENTITIES",
        "cli": "rabbitmq-use-existing-entities",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
    },
    "rabbitmq_virtual_host": {
        "default": pika.ConnectionParameters.DEFAULT_VIRTUAL_HOST,
        "env": "SENZING_RABBITMQ_VIRTUAL_HOST",
        "cli": "rabbitmq-virtual-host",
    },
    "redo_sleep_time_in_seconds": {
        "default": 10,
        "env": "SENZING_REDO_SLEEP_TIME_IN_SECONDS",
        "cli": "redo-sleep-time-in-seconds",
    },
    "redo_retry_sleep_time_in_seconds": {
        "default": 60,
        "env": "SENZING_REDO_RETRY_SLEEP_TIME_IN_SECONDS",
        "cli": "redo-retry-sleep-time-in-seconds",
    },
    "redo_retry_limit": {
        "default": 5,
        "env": "SENZING_REDO_RETRY_LIMIT",
        "cli": "redo-retry-limit",
    },
    "resource_path": {
        "default": "/opt/senzing/g2/resources",
        "env": "SENZING_RESOURCE_PATH",
        "cli": "resource-path"
    },
    "run_gdb": {
        "default": False,
        "env": "SENZING_RUN_GDB",
        "cli": "run-gdb",
    },
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "sqs_failure_queue_url": {
        "default": None,
        "env": "SENZING_SQS_FAILURE_QUEUE_URL",
        "cli": "sqs-failure-queue-url"
    },
    "sqs_info_queue_url": {
        "default": None,
        "env": "SENZING_SQS_INFO_QUEUE_URL",
        "cli": "sqs-info-queue-url"
    },
    "sqs_redo_queue_url": {
        "default": None,
        "env": "SENZING_SQS_REDO_QUEUE_URL",
        "cli": "sqs-redo-queue-url"
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
    "engine_configuration_json",
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
            "help": 'Read Senzing redo records from Senzing SDK and send to G2Engine.process()',
            "argument_aspects": ["engine"]
        },
        'redo-withinfo-kafka': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to Kafka.',
            "argument_aspects": ["engine", "threads", "monitoring", "kafka", "kafka-redo", "kafka-info", "kafka-failure"],
        },
        'redo-withinfo-rabbitmq': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to RabbitMQ.',
            "argument_aspects": ["engine", "threads", "monitoring", "rabbitmq", "rabbitmq-redo", "rabbitmq-info", "rabbitmq-failure"],
        },
        'redo-withinfo-sqs': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to AWS SQS.',
            "argument_aspects": ["engine", "threads", "monitoring", "sqs-redo", "sqs-info", "sqs-failure"],
        },
        'read-from-kafka': {
            "help": 'Read Senzing redo records from Kafka and send to G2Engine.process()',
            "argument_aspects": ["engine", "threads", "monitoring", "kafka", "kafka-redo"],
        },
        'read-from-kafka-withinfo': {
            "help": 'Read Senzing redo records from Kafka and send to G2Engine.processWithInfo()',
            "argument_aspects": ["engine", "threads", "monitoring", "kafka", "kafka-redo", "kafka-info", "kafka-failure"],
        },
        'read-from-rabbitmq': {
            "help": 'Read Senzing redo records from RabbitMQ and send to G2Engine.process()',
            "argument_aspects": ["engine", "threads", "monitoring", "rabbitmq", "rabbitmq-redo"],
        },
        'read-from-rabbitmq-withinfo': {
            "help": 'Read Senzing redo records from RabbitMQ and send to G2Engine.processWithInfo()',
            "argument_aspects": ["engine", "threads", "monitoring", "rabbitmq", "rabbitmq-redo", "rabbitmq-info", "rabbitmq-failure"],
        },
        'read-from-sqs': {
            "help": 'Read Senzing redo records from AWS SQS and send to G2Engine.process()',
            "argument_aspects": ["engine", "threads", "monitoring", "sqs-redo"],
        },
        'read-from-sqs-withinfo': {
            "help": 'Read Senzing redo records from AWS SQS and send to G2Engine.processWithInfo()',
            "argument_aspects": ["engine", "threads", "monitoring", "sqs-redo", "sqs-info", "sqs-failure"],
        },
        'redo-withinfo-kafka': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to Kafka.',
            "argument_aspects": ["engine", "threads", "kafka", "kafka-redo", "kafka-info", "kafka-failure"],
        },
        'redo-withinfo-rabbitmq': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to RabbitMQ.',
            "argument_aspects": ["engine", "threads", "monitoring", "rabbitmq", "rabbitmq-redo", "rabbitmq-info", "rabbitmq-failure"],
        },
        'redo-withinfo-sqs': {
            "help": 'Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to AWS SQS.',
            "argument_aspects": ["engine", "threads", "monitoring", "sqs-redo", "sqs-info", "sqs-failure"],
        },
        'write-to-azure-queue': {
            "help": 'Read Senzing redo records from Senzing SDK and send to Azure Message Bus queue.',
            "argument_aspects": ["engine", "threads", "monitoring", "azure-queue", "azure-queue-redo"],
        },
        'write-to-kafka': {
            "help": 'Read Senzing redo records from Senzing SDK and send to Kafka.',
            "argument_aspects": ["engine", "threads", "monitoring", "kafka", "kafka-redo"],
        },
        'write-to-rabbitmq': {
            "help": 'Read Senzing redo records from Senzing SDK and send to RabbitMQ.',
            "argument_aspects": ["engine", "threads", "monitoring", "rabbitmq", "rabbitmq-redo"],
        },
        'write-to-sqs': {
            "help": 'Read Senzing redo records from Senzing SDK and send to AWS SQS.',
            "argument_aspects": ["engine", "threads", "monitoring", "sqs-redo"],
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

    # Define argument_aspects.

    argument_aspects = {
        "azure-queue": {
            "--azure-connection-string": {
                "dest": "azure_connection_string",
                "metavar": "SENZING_AZURE_CONNECTION_STRING",
                "help": "Azure Queue connection string. Default: none"
            },
            "--azure-queue-name": {
                "dest": "azure_queue_name",
                "metavar": "SENZING_AZURE_QUEUE_NAME",
                "help": "Azure Queue name. Default: none"
            },
        },
        "azure-failure-queue": {
            "--azure-failure-connection-string": {
                "dest": "azure_failure_connection_string",
                "metavar": "SENZING_AZURE_FAILURE_CONNECTION_STRING",
                "help": "Azure Queue connection string for failures. Default: none"
            },
            "--azure-failure-queue-name": {
                "dest": "azure_failure_queue_name",
                "metavar": "SENZING_FAILURE_AZURE_QUEUE_NAME",
                "help": "Azure Queue name for failures. Default: none"
            },
        },
        "azure-info-queue": {
            "--azure-info-connection-string": {
                "dest": "azure_info_connection_string",
                "metavar": "SENZING_AZURE_INFO_CONNECTION_STRING",
                "help": "Azure Queue connection string for withInfo. Default: none"
            },
            "--azure-info-queue-name": {
                "dest": "azure_info_queue_name",
                "metavar": "SENZING_INFO_AZURE_QUEUE_NAME",
                "help": "Azure Queue name for withInfo. Default: none"
            },
        },
        "engine": {
            "--engine-configuration-json": {
                "dest": "engine_configuration_json",
                "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                "help": "Advanced Senzing engine configuration. Default: none"
            }
        },
        "kafka": {
            "--kafka-bootstrap-server": {
                "dest": "kafka_bootstrap_server",
                "metavar": "SENZING_KAFKA_BOOTSTRAP_SERVER",
                "help": "Kafka bootstrap server. Default: localhost:9092"
            },
        },
        "kafka-failure": {
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
        },
        "kafka-info": {
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
        },
        "kafka-redo": {
            "--kafka-redo-bootstrap-server": {
                "dest": "kafka_redo_bootstrap_server",
                "metavar": "SENZING_KAFKA_REDO_BOOTSTRAP_SERVER",
                "help": "Kafka bootstrap server. Default: SENZING_KAFKA_BOOTSTRAP_SERVER"
            },
            "--kafka-redo-group": {
                "dest": "kafka_redo_group",
                "metavar": "SENZING_KAFKA_REDO_GROUP",
                "help": "Kafka group. Default: senzing-kafka-redo-group"
            },
            "--kafka-redo-topic": {
                "dest": "kafka_redo_topic",
                "metavar": "SENZING_KAFKA_REDO_TOPIC",
                "help": "Kafka topic. Default: senzing-kafka-redo-topic"
            },
        },
        "monitoring": {
            "--monitoring-period-in-seconds": {
                "dest": "monitoring_period_in_seconds",
                "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                "help": "Period, in seconds, between monitoring reports. Default: 600"
            },
        },
        "rabbitmq": {
            "--rabbitmq-exchange": {
                "dest": "rabbitmq_exchange",
                "metavar": "SENZING_RABBITMQ_EXCHANGE",
                "help": "RabbitMQ exchange. Default: SENZING_RABBITMQ_EXCHANGE"
            },
            "--rabbitmq-heartbeat-in-seconds": {
                "dest": "rabbitmq_heartbeat_in_seconds",
                "metavar": "SENZING_RABBITMQ_HEARTBEAT_IN_SECONDS",
                "help": "RabbitMQ heartbeat. Default: 60"
            },
            "--rabbitmq-host": {
                "dest": "rabbitmq_host",
                "metavar": "SENZING_RABBITMQ_HOST",
                "help": "RabbitMQ host. Default: localhost:5672"
            },
            "--rabbitmq-password": {
                "dest": "rabbitmq_password",
                "metavar": "SENZING_RABBITMQ_PASSWORD",
                "help": "RabbitMQ password. Default: bitnami"
            },
            "--rabbitmq-reconnect-delay-in-seconds": {
                "dest": "rabbitmq_reconnect_delay_in_seconds",
                "metavar": "SENZING_RABBITMQ_RECONNECT_DELAY_IN_SECONDS",
                "help": "The time (in seconds) to wait between attempts to reconnect to the RabbitMQ broker. Default: 60"
            },
            "--rabbitmq-username": {
                "dest": "rabbitmq_username",
                "metavar": "SENZING_RABBITMQ_USERNAME",
                "help": "RabbitMQ username. Default: user"
            },
            "--rabbitmq-virtual-host": {
                "dest": "rabbitmq_virtual_host",
                "metavar": "SENZING_RABBITMQ_VIRTUAL_HOST",
                "help": "RabbitMQ virtual host. Default: The RabbitMQ defined default virtual host"
            },
            "--rabbitmq-use-existing-entities": {
                "dest": "rabbitmq_use_existing_entities",
                "metavar": "SENZING_RABBITMQ_USE_EXISTING_ENTITIES",
                "help": "Connect to an existing exchange and queue using their settings. An error is thrown if the exchange or queue does not exist. If False, it will create the exchange and queue if they do not exist. If they exist, then it will attempt to connect, checking the settings match. Default: True"
            },
        },
        "rabbitmq-failure": {
            "--rabbitmq-failure-exchange": {
                "dest": "rabbitmq_failure_exchange",
                "metavar": "SENZING_RABBITMQ_FAILURE_EXCHANGE",
                "help": "RabbitMQ exchange. Default: SENZING_RABBITMQ_EXCHANGE"
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
            "--rabbitmq-failure-routing-key": {
                "dest": "rabbitmq_failure_routing_key",
                "metavar": "SENZING_RABBITMQ_FAILURE_ROUTING_KEY",
                "help": "RabbitMQ routing key. Default: senzing.failure"
            },
            "--rabbitmq-failure-username": {
                "dest": "rabbitmq_failure_username",
                "metavar": "SENZING_RABBITMQ_FAILURE_USERNAME",
                "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
            },
            "--rabbitmq-failure-virtual-host": {
                "dest": "rabbitmq_failure_virtual_host",
                "metavar": "SENZING_RABBITMQ_FAILURE_VIRTUAL_HOST",
                "help": "RabbitMQ virtual host. Default: SENZING_RABBITMQ_VIRTUAL_HOST"
            },
        },
        "rabbitmq-info": {
            "--rabbitmq-info-exchange": {
                "dest": "rabbitmq_info_exchange",
                "metavar": "SENZING_RABBITMQ_INFO_EXCHANGE",
                "help": "RabbitMQ exchange. Default: SENZING_RABBITMQ_EXCHANGE"
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
            "--rabbitmq-info-routing-key": {
                "dest": "rabbitmq_info_routing_key",
                "metavar": "SENZING_RABBITMQ_INFO_ROUTING_KEY",
                "help": "RabbitMQ routing key. Default: senzing.info"
            },
            "--rabbitmq-info-username": {
                "dest": "rabbitmq_info_username",
                "metavar": "SENZING_RABBITMQ_INFO_USERNAME",
                "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
            },
            "--rabbitmq-info-virtual-host": {
                "dest": "rabbitmq_info_virtual_host",
                "metavar": "SENZING_RABBITMQ_INFO_VIRTUAL_HOST",
                "help": "RabbitMQ virtual host. Default: SENZING_RABBITMQ_VIRTUAL_HOST"
            },
        },
        "rabbitmq-redo": {
            "--rabbitmq-redo-exchange": {
                "dest": "rabbitmq_redo_exchange",
                "metavar": "SENZING_RABBITMQ_REDO_EXCHANGE",
                "help": "RabbitMQ exchange. Default: SENZING_RABBITMQ_EXCHANGE"
            },
            "--rabbitmq-redo-host": {
                "dest": "rabbitmq_redo_host",
                "metavar": "SENZING_RABBITMQ_REDO_HOST",
                "help": "RabbitMQ host. Default: SENZING_RABBITMQ_HOST"
            },
            "--rabbitmq-redo-password": {
                "dest": "rabbitmq_redo_password",
                "metavar": "SENZING_RABBITMQ_REDO_PASSWORD",
                "help": "RabbitMQ password. Default: SENZING_RABBITMQ_PASSWORD"
            },
            "--rabbitmq-redo-queue": {
                "dest": "rabbitmq_redo_queue",
                "metavar": "SENZING_RABBITMQ_REDO_QUEUE",
                "help": "RabbitMQ queue. Default: senzing-rabbitmq-redo-queue"
            },
            "--rabbitmq-redo-routing-key": {
                "dest": "rabbitmq_redo_routing_key",
                "metavar": "SENZING_RABBITMQ_REDO_ROUTING_KEY",
                "help": "RabbitMQ routing key. Default: senzing.redo"
            },
            "--rabbitmq-redo-username": {
                "dest": "rabbitmq_redo_username",
                "metavar": "SENZING_RABBITMQ_REDO_USERNAME",
                "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
            },
            "--rabbitmq-redo-virtual-host": {
                "dest": "rabbitmq_redo_virtual_host",
                "metavar": "SENZING_RABBITMQ_redo_VIRTUAL_HOST",
                "help": "RabbitMQ virtual host. Default: SENZING_RABBITMQ_VIRTUAL_HOST"
            },
        },
        "sqs-failure": {
            "--sqs-failure-queue-url": {
                "dest": "sqs_failure_queue_url",
                "metavar": "SENZING_SQS_FAILURE_QUEUE_URL",
                "help": "AWS SQS failure URL. Default: none"
            },
        },
        "sqs-info": {
            "--sqs-info-queue-url": {
                "dest": "sqs_info_queue_url",
                "metavar": "SENZING_SQS_INFO_QUEUE_URL",
                "help": "AWS SQS info URL. Default: none"
            },
        },
        "sqs-redo": {
            "--sqs-redo-queue-url": {
                "dest": "sqs_redo_queue_url",
                "metavar": "SENZING_SQS_REDO_QUEUE_URL",
                "help": "AWS SQS redo URL. Default: none"
            },
        },
        "threads": {
            "--threads-per-process": {
                "dest": "threads_per_process",
                "metavar": "SENZING_THREADS_PER_PROCESS",
                "help": "Number of threads per process. Default: 4"
            }
        },
    }

    # Augment "subcommands" variable with arguments specified by aspects.

    for subcommand, subcommand_value in subcommands.items():
        if 'argument_aspects' in subcommand_value:
            for aspect in subcommand_value['argument_aspects']:
                if 'arguments' not in subcommands[subcommand]:
                    subcommands[subcommand]['arguments'] = {}
                arguments = argument_aspects.get(aspect, {})
                for argument, argument_value in arguments.items():
                    subcommands[subcommand]['arguments'][argument] = argument_value

    # Parse command line arguments.

    parser = argparse.ArgumentParser(prog="redoer.py", description="Process Senzing redo records. For more information, see https://github.com/Senzing/redoer")
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
    "103": "Thread: {0} Kafka topic: {1}; message: {2}; error: {3}; error: {4}",
    "120": "Sleeping for requested delay of {0} seconds.",
    "121": "Thread: {0} Adding JSON to failure queue: {1}",
    "125": "G2 engine statistics: {0}",
    "127": "Monitor: {0}",
    "128": "Thread: {0} Adding JSON to info queue: {1}",
    "129": "{0} is running.",
    "130": "{0} has exited.",
    "131": "Adding redo record to redo queue: {0}",
    "132": "Sleeping {0} seconds before attempting to reconnect to RabbitMQ",
    "133": "RabbitMQ connection is not open. Did opening the connection succeed? Thread {0}",
    "134": "RabbitMQ connection closed by the broker. Thread {0}. Error: {1}",
    "135": "Could not ACK a RabbitMQ message. Thread {0}. Error: {1}",
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
    "181": "Monitoring halted. No active workers.",
    "190": "AWS SQS Long-polling: No messages from {0}",
    "203": "          WARNING: License will expire soon. Only {0} days left.",
    "292": "Configuration change detected.  Old: {0} New: {1}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "404": "Thread: {0} Kafka topic: {1} BufferError: {2} Message: {3}",
    "405": "Thread: {0} Kafka topic: {1} KafkaException: {2} Message: {3}",
    "406": "Thread: {0} Kafka topic: {1} NotImplemented: {2} Message: {3}",
    "407": "Thread: {0} Kafka topic: {1} Unknown error: {2} Message: {3}",
    "408": "Thread: {0} Kafka topic: {1}; message: {2}; error: {3}; error: {4}",
    "410": "Thread: {0} RabbitMQ queue: {1} Unknown RabbitMQ error when connecting: {2}.",
    "411": "Thread: {0} RabbitMQ queue: {1} Unknown RabbitMQ error: {2} Message: {3}",
    "412": "Thread: {0} RabbitMQ queue: {1} AMQPConnectionError: {2} Could not connect to RabbitMQ host at {3}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details.",
    "413": "Thread: {0} RabbitMQ queue: {1} Unknown RabbitMQ error: {2}",
    "414": "Thread: {0} RabbitMQ queue: {1} Error when trying to send message to RabbitMQ: {2}",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "561": "Thread: {0} Unknown RabbitMQ error when connecting: {1}",
    "562": "Thread: {0} Could not connect to RabbitMQ host at {2}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details. Error: {1}",
    "563": "Thread: {0} The exchange {1} and/or the queue {2} do not exist. Create them, or set rabbitmq-use-existing-entities to False to have stream-producer create them.",
    "564": "Thread: {0} The exchange {1} and/or the queue {2} exist but are configured with unexpected parameters. Set rabbitmq-use-existing-entities to True to connect to the preconfigured exchange and queue, or delete the existing exchange and queue and try again.",
    "695": "Unknown database scheme '{0}' in database url '{1}'",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "701": "G2Engine.getRedoRecord() bad return code: {0}",
    "702": "G2Engine.getRedoRecord() G2ModuleNotInitialized: {0} XML: {1}",
    "703": "G2Engine.getRedoRecord() err: {0}",
    "704": "G2Engine.getRedoRecord() err: {0}; Attempting reprocessing in {1} seconds",
    "706": "G2Engine.process() bad return code: {0}",
    "707": "Thread: {0} G2Engine.process() G2ModuleNotInitialized: {0} XML: {1}",
    "708": "G2Engine.process() G2ModuleGenericException: {0} XML: {1}",
    "709": "Thread: {0} G2Engine.process() err: {1}",
    "710": "Thread: {0} G2Engine.process() Database connection error: {1}; Retrying execution",
    "721": "Running low on workers.  May need to restart",
    "722": "Thread: {0} Kafka commit failed for {1}",
    "723": "Detected inactive thread. Total threads: {0}  Active threads: {1}",
    "730": "There are not enough safe characters to do the translation. Unsafe Characters: {0}; Safe Characters: {1}",
    "880": "Unspecific error when {1}. Error: {0}",
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
    "901": "Thread: {0} g2_engine.getRedoRecord()",
    "902": "Thread: {0} g2_engine.getRedoRecord() returned nothing. Sleeping {1} seconds before retry.",
    "903": "Thread: {0} g2_engine.getRedoRecord() -> {1}",
    "904": "Thread: {0} Added message to internal queue: {1}",
    "906": "Thread: {0} re-processing redo record: {1}",
    "908": "Thread: {0} g2_engine.reinitV2({1})",
    "910": "Thread: {0} g2_engine.process() redo_record: {1}",
    "911": "Thread: {0} g2_engine.process() -> redo_record: {1}",
    "913": "Thread: {0} g2_engine.processWithInfo() redo_record: {1}",
    "914": "Thread: {0} g2_engine.processWithInfo() -> redo_record: {1} withInfo: {2}",
    "915": "RabbitmqSubscribeThread: {0} Host: {1} Queue-name: {2} Username: {3}  Prefetch-count: {4}",
    "916": "Thread: {0} Queue: {1} Publish Message: {2}",
    "917": "Thread: {0} Queue: {1} Subscribe Message: {2}",
    "918": "Thread: {0} pulled message from {1} queue: {2}",
    "919": "Thread: {0} processing redo record: {1}",
    "920": "gdb STDOUT: {0}",
    "921": "gdb STDERR: {0}",
    "922": "Thread: {0} Marker: {1} Redo record: {2}",
    "995": "Thread: {0} Using Class: {1}",
    "996": "Thread: {0} Using Mixin: {1}",
    "997": "Thread: {0} Using Thread: {1}",
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

    # Add program information.

    result['program_version'] = __version__
    result['program_updated'] = __updated__

    # Add "run_as" information.

    result['run_as_uid'] = os.getuid()
    result['run_as_gid'] = os.getgid()

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = [
        'debug',
        'exit_on_thread_termination',
        'rabbitmq_use_existing_entities',
        'run_gdb',
    ]
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
        'delay_in_seconds',
        'expiration_warning_in_days',
        'log_license_period_in_seconds',
        'monitoring_period_in_seconds',
        'queue_maxsize',
        'rabbitmq_heartbeat_in_seconds',
        'rabbitmq_reconnect_delay_in_seconds',
        'redo_sleep_time_in_seconds',
        'redo_retry_sleep_time_in_seconds',
        'redo_retry_limit',
        'sleep_time_in_seconds',
        'threads_per_process'
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case:  Tailored database URL

    result['g2_database_url_specific'] = get_g2_database_url_specific(result.get("g2_database_url_generic"))

    # Initialize counters.

    counters = [
        'processed_redo_records',
        'sent_to_failure_queue',
        'sent_to_info_queue',
        'sent_to_redo_queue',
        'received_from_redo_queue',
        'redo_records_from_senzing_engine'
    ]
    for counter in counters:
        result[counter] = 0

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

    def close(self):
        return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

# -----------------------------------------------------------------------------
# Class: InfoFilter
# -----------------------------------------------------------------------------


class InfoFilter:

    def __init__(self, g2_engine=None, *args, **kwargs):
        self.g2_engine = g2_engine

    def filter(self, message=None, *args, **kwargs):
        return message

# -----------------------------------------------------------------------------
# RabbitMQ publish and subscribe threads
# -----------------------------------------------------------------------------


class Rabbitmq:
    '''
    https://github.com/pika/pika/issues/1104
    '''

    def __init__(
        self,
        username,
        password,
        host,
        queue_name,
        exchange,
        virtual_host,
        heartbeat,
        routing_key,
        passive,
        delivery_mode=2,
        prefetch_count=1,
        reconnect_delay_in_seconds=60
    ):

        logging.debug(message_debug(995, threading.current_thread().name, "Rabbitmq"))

        # Check input parameter data types.

        assert isinstance(delivery_mode, int)
        assert isinstance(exchange, str)
        assert isinstance(heartbeat, int)
        assert isinstance(host, str)
        assert isinstance(password, str)
        assert isinstance(prefetch_count, int)
        assert isinstance(queue_name, str)
        assert isinstance(username, str)
        assert isinstance(virtual_host, str)

        # Instance variables.

        self.delivery_mode = delivery_mode
        self.exchange = exchange
        self.queue_name = queue_name
        self.passive = passive
        self.routing_key = routing_key
        self.prefetch_count = prefetch_count
        self.reconnect_delay_in_seconds = reconnect_delay_in_seconds

        # Create a RabbitMQ connection and channel.

        self.credentials = pika.PlainCredentials(
            username=username,
            password=password,
        )
        self.connection_parameters = pika.ConnectionParameters(
            credentials=self.credentials,
            host=host,
            heartbeat=heartbeat,
            virtual_host=virtual_host
        )

        self.connection, self.channel = self.connect()

    def connect(self, exit_on_exception=True):

        connection = None
        channel = None
        try:
            connection = pika.BlockingConnection(self.connection_parameters)
            channel = connection.channel()
            channel.basic_qos(
                prefetch_count=self.prefetch_count,
            )

            channel.exchange_declare(
                exchange=self.exchange,
                passive=self.passive
            )

            message_queue = channel.queue_declare(
                queue=self.queue_name,
                passive=self.passive
            )

            # if we are actively declaring, then we need to bind. If passive declare, we assume it is already set up

            if not self.passive:
                channel.queue_bind(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    queue=message_queue.method.queue
                )

        except pika.exceptions.AMQPConnectionError as err:
            if exit_on_exception:
                exit_error(562, threading.current_thread().name, err, self.connection_parameters.host)
            else:
                logging.info(message_info(562, threading.current_thread().name, err, self.connection_parameters.host))
        except BaseException as err:
            if exit_on_exception:
                exit_error(561, threading.current_thread().name, err)
            else:
                logging.info(message_info(561, threading.current_thread().name, err))

        return connection, channel

    def receive(self, callback=None):

        def on_message_callback(channel, method, properties, body):
            logging.debug(message_debug(917, threading.current_thread().name, self.queue_name, body))

            if callback is not None:
                my_thread = threading.Thread(target=callback, args=(body, method))
                my_thread.daemon = True
                my_thread.start()

                while my_thread.is_alive():
                    self.connection.process_data_events()
                    self.connection.sleep(5)

        while True:
            try:
                if self.channel is not None and self.channel.is_open:
                    self.channel.basic_consume(
                        queue=self.queue_name,
                        on_message_callback=on_message_callback,
                    )

                    self.channel.start_consuming()
                else:
                    logging.info(message_info(133, threading.current_thread().name))
            except pika.exceptions.ChannelClosed:
                logging.info(message_info(130, threading.current_thread().name))
            except BaseException as err:
                logging.warning(message_warning(413, threading.current_thread().name, self.queue_name, err))

            logging.info(message_info(132, self.reconnect_delay_in_seconds))
            time.sleep(self.reconnect_delay_in_seconds)

            # Reconnect to RabbitMQ queue.

            self.connection, self.channel = self.connect(
                exit_on_exception=False
            )

    def ack_message(self, delivery_tag):
        try:
            cb = functools.partial(self.ack_message_callback, delivery_tag)
            self.connection.add_callback_threadsafe(cb)
        except pika.exceptions.ConnectionClosed as err:
            logging.info(message_info(134, threading.current_thread().name, err))
        except Exception as err:
            logging.info(message_info(880, err, "connection.add_callback_threadsafe()"))

    def ack_message_callback(self, delivery_tag):
        try:
            self.channel.basic_ack(delivery_tag)
        except Exception as err:
            logging.info(message_info(135, threading.current_thread().name, err))

    def send(self, message):
        logging.debug(message_debug(916, threading.current_thread().name, self.queue_name, message))
        assert type(message) == str
        message_bytes = message.encode()
        while True:
            try:
                if self.channel is not None and self.channel.is_open:
                    self.channel.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.routing_key,
                        body=message_bytes,
                        properties=pika.BasicProperties(
                            delivery_mode=self.delivery_mode,
                        ),
                    )
                    break
            except pika.exceptions.StreamLostError as err:
                logging.warning(message_warning(414, threading.current_thread().name, self.queue_name, err))
            except Exception as err:
                logging.warning(message_warning(411, threading.current_thread().name, self.queue_name, err, message))

            logging.info(message_info(132, self.reconnect_delay_in_seconds))
            time.sleep(self.reconnect_delay_in_seconds)

            self.connection, self.channel = self.connect(
                exit_on_exception=False
            )

    def close(self):
        self.connection.close()


class RabbitmqSubscribeThread(threading.Thread):
    '''
    Wrap RabbitMQ behind a Python Queue.
    '''

    def __init__(self, internal_queue, host, exchange, virtual_host, heartbeat, queue_name, routing_key, username, password, passive, prefetch_count, reconnect_delay_in_seconds):
        threading.Thread.__init__(self)
        logging.debug(message_debug(997, threading.current_thread().name, "RabbitmqSubscribeThread"))

        logging.debug(message_debug(
            915,
            threading.current_thread().name,
            host,
            queue_name,
            username,
            prefetch_count))

        self.internal_queue = internal_queue
        self.queue_name = queue_name

        # Connect to RabbitMQ.

        self.input_rabbitmq_mixin_rabbitmq = Rabbitmq(
            username=username,
            password=password,
            host=host,
            queue_name=queue_name,
            exchange=exchange,
            virtual_host=virtual_host,
            heartbeat=heartbeat,
            routing_key=routing_key,
            passive=passive,
            prefetch_count=prefetch_count,
            reconnect_delay_in_seconds=reconnect_delay_in_seconds
        )

    def callback(self, message, method):
        '''
        Put message into internal queue.
        '''
        logging.debug(message_debug(917, threading.current_thread().name, self.queue_name, message))
        if type(message) == bytes:
            message = message.decode()
        assert isinstance(message, str)
        self.internal_queue.put((message, method.delivery_tag))

    def ack_message(self, delivery_tag):
        self.input_rabbitmq_mixin_rabbitmq.ack_message(delivery_tag)

    def run(self):
        self.input_rabbitmq_mixin_rabbitmq.receive(self.callback)

# -----------------------------------------------------------------------------
# Class: MonitorThread
# -----------------------------------------------------------------------------


class MonitorThread(threading.Thread):
    '''
    Periodically log operational metrics.
    '''

    def __init__(self, config=None, g2_engine=None, workers=None):
        threading.Thread.__init__(self)
        self.config = config
        self.digits_regex_pattern = re.compile(':\d+$')
        self.exit_on_thread_termination = self.config.get("exit_on_thread_termination")
        self.g2_engine = g2_engine
        self.in_regex_pattern = re.compile('\sin\s')
        self.log_level_parameter = config.get("log_level_parameter")
        self.log_license_period_in_seconds = self.config.get("log_license_period_in_seconds")
        self.pstack_pid = config.get("pstack_pid")
        self.run_gdb = self.config.get('run_gdb')
        self.sleep_time_in_seconds = self.config.get('monitoring_period_in_seconds')
        self.workers = workers

    def run(self):
        '''Periodically monitor what is happening.'''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Initialize variables.

        last = {
            "processed_redo_records": 0,
            "sent_to_failure_queue": 0,
            "sent_to_info_queue": 0,
            "sent_to_redo_queue": 0,
            "received_from_redo_queue": 0,
            "redo_records_from_senzing_engine": 0,
        }

        last_log_license = time.time()

        # Sleep-monitor loop.

        active_workers = len(self.workers)
        for worker in self.workers:
            if not worker.is_alive():
                active_workers -= 1

        while active_workers > 0:

            time.sleep(self.sleep_time_in_seconds)

            # Calculate active Threads.

            active_workers = len(self.workers)
            for worker in self.workers:
                if not worker.is_alive():
                    active_workers -= 1

            # Determine if we're running out of workers.

            if (active_workers / float(len(self.workers))) < 0.5:
                logging.warning(message_warning(721))

            # If requested, terminate all threads if any thread is not active.

            if self.exit_on_thread_termination:
                if len(self.workers) != active_workers:
                    exit_error_program(723, active_workers, len(self.workers))

            # Calculate times.

            now = time.time()
            uptime = now - self.config.get('start_time', now)
            elapsed_log_license = now - last_log_license

            # Log license periodically to show days left in license.

            if elapsed_log_license > self.log_license_period_in_seconds:
                log_license(self.config)
                last_log_license = now

            # Construct and log monitor statistics.

            stats = {
                "uptime": int(uptime),
                "workers_total": len(self.workers),
                "workers_active": active_workers,
            }

            # Tricky code.  Avoid modifying dictionary in the loop.
            # i.e. "for key, value in last.items():" would loop infinitely
            # because of "last[key] = total".

            keys = last.keys()
            for key in keys:
                value = last.get(key)
                total = self.config.get(key)
                interval = total - value
                stats["{0}_total".format(key)] = total
                stats["{0}_interval".format(key)] = interval
                last[key] = total

            logging.info(message_info(127, json.dumps(stats, sort_keys=True)))

            # Log engine statistics with sorted JSON keys.

            g2_engine_stats_response = bytearray()
            self.g2_engine.stats(g2_engine_stats_response)
            g2_engine_stats_dictionary = json.loads(g2_engine_stats_response.decode())
            logging.info(message_info(125, json.dumps(g2_engine_stats_dictionary, sort_keys=True)))

            # If requested, debug stacks.

            if self.run_gdb:
                completed_process = None
                try:

                    # Run gdb to get stacks.

                    completed_process = subprocess.run(
                        ["gdb", "-q", "-p", self.pstack_pid, "-batch", "-ex", "thread apply all bt"],
                        capture_output=True)

                except Exception as err:
                    logging.warning(message_warning(999, err))

                if completed_process is not None:

                    # Process gdb output.

                    counter = 0
                    stdout_dict = {}
                    stdout_lines = str(completed_process.stdout).split('\\n')
                    for stdout_line in stdout_lines:

                        # Filter lines.

                        if self.digits_regex_pattern.search(stdout_line) is not None and self.in_regex_pattern.search(stdout_line) is not None:

                            # Format lines.

                            counter += 1
                            line_parts = stdout_line.split()
                            output_line = "{0:<3} {1} {2}".format(line_parts[0], line_parts[3], line_parts[-1].rsplit('/', 1)[-1])
                            stdout_dict[str(counter).zfill(4)] = output_line

                    # Log STDOUT.

                    stdout_json = json.dumps(stdout_dict)
                    logging.debug(message_debug(920, stdout_json))

                    # Log STDERR.

                    counter = 0
                    stderr_dict = {}
                    stderr_lines = str(completed_process.stderr).split('\\n')
                    for stderr_line in stderr_lines:
                        counter += 1
                        stderr_dict[str(counter).zfill(4)] = stderr_line
                    stderr_json = json.dumps(stderr_dict)
                    logging.debug(message_debug(921, stderr_json))

        logging.info(message_info(181))

# =============================================================================
# Mixins: Input*
#   Methods:
#   - redo_records() -> generated list of strings
#   Classes:
#   - InputAzureQueueMixin - Gets redo records from Azure Queue
#   - InputInternalMixin - Gets redo records from internal queue
#   - InputKafkaMixin - Gets redo records from Kafka
#   - InputRabbitmqMixin - Gets redo records from RabbitMQ
#   - InputSqsMixin - Gets redo records from AWS SQS
# =============================================================================


# -----------------------------------------------------------------------------
# Class: InputAzureMixin
# -----------------------------------------------------------------------------

class InputAzureMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "InputAzureMixin"))
        connection_string = config.get("azure_connection_string")
        queue_name = config.get("azure_queue_name")

        # Create objects.

        self.servicebus_client = ServiceBusClient.from_connection_string(connection_string)
        self.receiver = self.servicebus_client.get_queue_receiver(queue_name=queue_name)

    def redo_records(self):
        '''
        Generator that produces Senzing redo records
        retrieved from a Azure Queue.
        '''

        # In a loop, get messages from AWS SQS.

        while True:

            for queue_message in self.receiver:
                message_body = str(queue_message)

                # As a generator, give the value to the co-routine.

                logging.debug(message_debug(918, threading.current_thread().name, "Azure Queue", message_body))
                assert isinstance(message_body, str)
                yield message_body, queue_message

    def acknowledge_read_message(self, queue_message):
        '''
        Tell Azure Queue we're done with message.
        '''

        self.receiver.complete_message(queue_message)

# -----------------------------------------------------------------------------
# Class: InputInternalMixin
# -----------------------------------------------------------------------------


class InputInternalMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "InputInternalMixin"))

    def redo_records(self):
        '''
        Generator that produces Senzing redo records
        retrieved from the "internal queue".
        '''

        while True:
            message = self.redo_queue.get()
            logging.debug(message_debug(918, threading.current_thread().name, "internal", message))
            assert isinstance(message, tuple)
            self.config['received_from_redo_queue'] += 1
            yield message

# -----------------------------------------------------------------------------
# Class: InputKafkaMixin
# -----------------------------------------------------------------------------


class InputKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "InputKafkaMixin"))

        # Create Kafka client.

        consumer_configuration = {
            'bootstrap.servers': self.config.get('kafka_redo_bootstrap_server'),
            'group.id': self.config.get("kafka_redo_group"),
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = confluent_kafka.Consumer(consumer_configuration)
        self.consumer.subscribe([self.config.get("kafka_redo_topic")])

    def redo_records(self):
        '''
        Generator that produces Senzing redo records
        retrieved from a Kafka topic.
        '''

        while True:

            # Get message from Kafka queue.
            # Timeout quickly to allow other co-routines to process.

            kafka_message = self.consumer.poll(1.0)

            # Handle non-standard Kafka output.

            if kafka_message is None:
                continue
            if kafka_message.error():
                if kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(message_error(722, threading.current_thread().name, kafka_message.error()))
                    continue

            # Construct and verify Kafka message.

            message = str(kafka_message.value().decode()).strip()
            if not message:
                continue
            self.config['received_from_redo_queue'] += 1

            # As a generator, give the value to the co-routine.

            logging.debug(message_debug(918, threading.current_thread().name, "Kafka", message))
            assert isinstance(message, str)
            yield message, None

            # After successful import into Senzing, tell Kafka we're done with message.

            self.consumer.commit()

        # Being outside of "while True", the following won't be executed.
        # But it is good form to close resources.

        self.consumer.close()

# -----------------------------------------------------------------------------
# Class: InputRabbitmqMixin
# -----------------------------------------------------------------------------


class InputRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "InputRabbitmqMixin"))

        # Create qn internal queue for this mixin.

        self.input_rabbitmq_mixin_queue = queue.Queue()

        threads = []

        # Create thread for redo queue.

        self.redo_thread = RabbitmqSubscribeThread(
            self.input_rabbitmq_mixin_queue,
            self.config.get("rabbitmq_redo_host"),
            self.config.get("rabbitmq_redo_exchange"),
            self.config.get("rabbitmq_redo_virtual_host"),
            self.config.get("rabbitmq_heartbeat_in_seconds"),
            self.config.get("rabbitmq_redo_queue"),
            self.config.get("rabbitmq_redo_routing_key"),
            self.config.get("rabbitmq_redo_username"),
            self.config.get("rabbitmq_redo_password"),
            self.config.get("rabbitmq_use_existing_entities"),
            self.config.get("rabbitmq_prefetch_count"),
            self.config.get("rabbitmq_reconnect_delay_in_seconds")
        )
        self.redo_thread.name = "{0}-{1}".format(threading.current_thread().name, "redo")
        threads.append(self.redo_thread)

        # Start threads.

        for thread in threads:
            thread.start()

    def redo_records(self):
        '''
        Generator that produces Senzing redo records.
        This method reads from an internal queue
        populated by the RabbitMQ callback() method.
        '''
        while True:
            message, delivery_tag = self.input_rabbitmq_mixin_queue.get()
            logging.debug(message_debug(918, threading.current_thread().name, "RabbitMQ", message))
            assert isinstance(message, str)
            self.config['received_from_redo_queue'] += 1
            yield message, delivery_tag

    def acknowledge_read_message(self, delivery_tag):
        self.redo_thread.ack_message(delivery_tag)

# -----------------------------------------------------------------------------
# Class: InputSqsMixin
# -----------------------------------------------------------------------------


class InputSqsMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "InputSqsMixin"))
        self.queue_url = self.config.get("sqs_redo_queue_url")

        # Create sqs object.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

        regular_expression = "^([^/]+://[^/]+)/"
        regex = re.compile(regular_expression)
        match = regex.match(self.queue_url)
        if not match:
            exit_error(750, self.queue_url)
        endpoint_url = match.group(1)
        self.sqs = boto3.client("sqs", endpoint_url=endpoint_url)

    def redo_records(self):
        '''
        Generator that produces Senzing redo records
        retrieved from a Kafka topic.
        '''

        while True:

            # Get message from AWS SQS queue.

            sqs_response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=[],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[],
                VisibilityTimeout=30,
                WaitTimeSeconds=20
            )

            # If non-standard SQS output or empty messages, just loop.

            if sqs_response is None:
                continue
            sqs_messages = sqs_response.get("Messages", [])
            if not sqs_messages:
                logging.info(message_info(190, self.queue_url))
                continue

            # Construct and verify SQS message.

            sqs_message = sqs_messages[0]
            sqs_message_body = sqs_message.get("Body")
            sqs_message_receipt_handle = sqs_message.get("ReceiptHandle")
            self.config['received_from_redo_queue'] += 1

            # As a generator, give the value to the co-routine.

            logging.debug(message_debug(918, threading.current_thread().name, "SQS", sqs_message_body))
            assert isinstance(sqs_message_body, str)
            yield sqs_message_body, sqs_message_receipt_handle

    def acknowledge_read_message(self, delivery_tag):
        '''
        Tell AWS SQS we're done with message.
        '''

        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=delivery_tag
        )

# =============================================================================
# Mixins: Execute*
#   Methods:
#   - process_redo_record(redo_record)
#   Classes:
#   - ExecuteMixin - calls g2_engine.process(...)
#   - ExecuteWithInfoMixin - g2_engine.processWithInfo(...)
#   - ExecuteWriteToRabbitmqMixin - Sends redo record to RabbitMQ
#   - ExecuteWriteToKafkaMixin - Sends redo record to Kafka
# =============================================================================

# -----------------------------------------------------------------------------
# Class: ExecuteMixin
# -----------------------------------------------------------------------------


class ExecuteMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "ExecuteMixin"))

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        This method uses G2Engine.process()
        The method can be sub-classed to call other G2Engine methods.
        '''

        logging.debug(message_debug(919, threading.current_thread().name, redo_record))
        assert isinstance(redo_record, str)

        # Call g2_engine.process() and handle "edge" cases.

        try:
            logging.debug(message_debug(910, threading.current_thread().name, redo_record))
            self.g2_engine.process(redo_record)
            logging.debug(message_debug(911, threading.current_thread().name, redo_record))

            self.config['processed_redo_records'] += 1

        except G2Exception.G2ModuleNotInitialized as err:
            exit_error(707, threading.current_thread().name, err, redo_record)
        except Exception as err:
            # OT-TODO: replace this error handling in the future when G2 throws dedicated
            # failed connection exception.
            if is_db_connection_error(err.args[0]):
                logging.warning(message_warning(710, threading.current_thread().name, err))
                return False
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                logging.debug(message_debug(906, threading.current_thread().name, redo_record))
                self.g2_engine.process(redo_record)
                logging.debug(message_debug(911, threading.current_thread().name, redo_record))
                self.config['processed_redo_records'] += 1
            else:
                exit_error(709, threading.current_thread().name, err)

        return True

# -----------------------------------------------------------------------------
# Class: ExecuteWithInfoMixin
# -----------------------------------------------------------------------------


class ExecuteWithInfoMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "ExecuteWithInfoMixin"))
        self.g2_engine_flags = 0

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        This method uses G2Engine.processWithInfo()
        '''

        logging.debug(message_debug(919, threading.current_thread().name, redo_record))
        assert isinstance(redo_record, str)

        # Additional parameters for processWithInfo().

        info_bytearray = bytearray()

        # Call g2_engine.processWithInfo() and handle "edge" cases.

        try:
            logging.debug(message_debug(913, threading.current_thread().name, redo_record))
            self.g2_engine.processWithInfo(redo_record, info_bytearray, self.g2_engine_flags)
            logging.debug(message_debug(914, threading.current_thread().name, redo_record, info_bytearray))

            self.config['processed_redo_records'] += 1

        except G2Exception.G2ModuleNotInitialized as err:
            self.send_to_failure_queue(redo_record)
            exit_error(707, threading.current_thread().name, err, info_bytearray.decode())
        except Exception as err:
            # OT-TODO: replace this error handling in the future when G2 throws dedicated
            # failed connection exception.
            if is_db_connection_error(err.args[0]):
                logging.warning(message_warning(710, threading.current_thread().name, err))
                return False
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                logging.debug(message_debug(906, threading.current_thread().name, redo_record))
                self.g2_engine.processWithInfo(redo_record, info_bytearray, self.g2_engine_flags)
                logging.debug(message_debug(914, threading.current_thread().name, redo_record, info_bytearray))
                self.config['processed_redo_records'] += 1
            else:
                self.send_to_failure_queue(redo_record)
                exit_error(709, threading.current_thread().name, err)

        info_json = info_bytearray.decode()

        # Allow user to manipulate the message.

        filtered_info_json = self.filter_info_message(message=info_json)

        # Put "info" on info queue.

        if filtered_info_json:
            self.send_to_info_queue(filtered_info_json)

        return True

# -----------------------------------------------------------------------------
# Class: ExecuteWriteToKafkaMixin
# -----------------------------------------------------------------------------


class ExecuteWriteToKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "ExecuteWriteToKafkaMixin"))

        kafka_redo_bootstrap_server = self.config.get('kafka_redo_bootstrap_server')
        self.kafka_redo_topic = self.config.get('kafka_redo_topic')

        # Kafka configuration.

        kafka_producer_configuration = {
            'bootstrap.servers': kafka_redo_bootstrap_server,
        }
        self.kafka_producer = confluent_kafka.Producer(kafka_producer_configuration)

    def on_kafka_delivery(self, error, message):
        message_topic = message.topic()
        message_value = message.value()
        message_error = message.error()
        logging.debug(message_debug(103, threading.current_thread().name, message_topic, message_value, message_error, error))
        if error is not None:
            logging.warning(message_warning(408, threading.current_thread().name, message_topic, message_value, message_error, error))

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        Simply send to Kafka.
        '''

        logging.debug(message_debug(916, threading.current_thread().name, self.kafka_redo_topic, redo_record))
        assert isinstance(redo_record, str)

        load_succeeded = True

        try:
            self.kafka_producer.produce(self.kafka_redo_topic, redo_record, on_delivery=self.on_kafka_delivery)
            self.config['sent_to_redo_queue'] += 1
        except BufferError as err:
            logging.warning(message_warning(404, threading.current_thread().name, self.kafka_redo_topic, err, redo_record))
            load_succeeded = False
        except confluent_kafka.KafkaException as err:
            logging.warning(message_warning(405, threading.current_thread().name, self.kafka_redo_topic, err, redo_record))
            load_succeeded = False
        except NotImplementedError as err:
            logging.warning(message_warning(406, threading.current_thread().name, self.kafka_redo_topic, err, redo_record))
            load_succeeded = False
        except Exception as err:
            logging.warning(message_warning(407, threading.current_thread().name, self.kafka_redo_topic, err, redo_record))
            load_succeeded = False

        return load_succeeded

# -----------------------------------------------------------------------------
# Class: ExecuteWriteToRabbitmqMixin
# -----------------------------------------------------------------------------


class ExecuteWriteToRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "ExecuteWriteToRabbitmqMixin"))

        self.execute_write_to_rabbitmq_mixin_rabbitmq = Rabbitmq(
            username=self.config.get("rabbitmq_redo_username"),
            password=self.config.get("rabbitmq_redo_password"),
            host=self.config.get("rabbitmq_redo_host"),
            queue_name=self.config.get("rabbitmq_redo_queue"),
            exchange=self.config.get("rabbitmq_redo_exchange"),
            virtual_host=self.config.get("rabbitmq_redo_virtual_host"),
            heartbeat=self.config.get("rabbitmq_heartbeat_in_seconds"),
            routing_key=self.config.get("rabbitmq_redo_routing_key"),
            passive=self.config.get("rabbitmq_use_existing_entities"),
        )

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        Simply send to RabbitMQ.
        '''

        logging.debug(message_debug(919, threading.current_thread().name, redo_record))
        assert isinstance(redo_record, str)

        self.execute_write_to_rabbitmq_mixin_rabbitmq.send(redo_record)
        self.config['sent_to_redo_queue'] += 1

        return True

# -----------------------------------------------------------------------------
# Class: ExecuteWriteToSqsMixin
# -----------------------------------------------------------------------------


class ExecuteWriteToSqsMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "ExecuteWriteToSqsMixin"))
        self.queue_url = self.config.get("sqs_redo_queue_url")

        # Create sqs object.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

        regular_expression = "^([^/]+://[^/]+)/"
        regex = re.compile(regular_expression)
        match = regex.match(self.queue_url)
        if not match:
            exit_error(750, self.queue_url)
        endpoint_url = match.group(1)
        self.sqs = boto3.client("sqs", endpoint_url=endpoint_url)

    def process_redo_record(self, redo_record=None):
        '''
        Process a single Senzing redo record.
        Simply send to AWS SQS.
        '''

        logging.debug(message_debug(919, threading.current_thread().name, redo_record))
        assert isinstance(redo_record, str)

        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            DelaySeconds=10,
            MessageAttributes={},
            MessageBody=(redo_record),
        )
        self.config['sent_to_redo_queue'] += 1

        return True

# =============================================================================
# Mixins: Output*
#   Methods:
#   - send_to_failure_queue(message)
#   - send_to_info_queue(message)
#   Classes:
#   - OutputAzureQueueMixin - Send to Azure queue
#   - OutputInternalMixin - Send to log
#   - OutputKafkaMixin - Send to Kafka
#   - OutputRabbitmqMixin - Send to RabbitMQ
#   - OutputSqsMixin - Send to AWS SQS
# =============================================================================

# -----------------------------------------------------------------------------
# Class: OutputAzureQueueMixin
# -----------------------------------------------------------------------------


class OutputAzureQueueMixin():
    ''' This is a "null object". '''

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "OutputInternalMixin"))
        failure_connection_string = config.get("azure_failure_connection_string")
        failure_queue_name = config.get("azure_failure_queue_name")
        info_connection_string = config.get("azure_info_connection_string")
        info_queue_name = config.get("azure_info_queue_name")

        failure_servicebus_client = ServiceBusClient.from_connection_string(failure_connection_string)
        self.failure_sender = failure_servicebus_client.get_queue_sender(queue_name=failure_queue_name)
        info_servicebus_client = ServiceBusClient.from_connection_string(info_connection_string)
        self.failure_sender = info_servicebus_client.get_queue_sender(queue_name=info_queue_name)

    def send_to_failure_queue(self, message):
        assert isinstance(message, str)
        service_bus_message = ServiceBusMessage(message)
        self.failure_sender.send_messages(service_bus_message)
        self.config['sent_to_failure_queue'] += 1

    def send_to_info_queue(self, message):
        assert isinstance(message, str)
        service_bus_message = ServiceBusMessage(message)
        self.finfo_sender.send_messages(service_bus_message)
        self.config['sent_to_info_queue'] += 1


# -----------------------------------------------------------------------------
# Class: OutputInternalMixin
# -----------------------------------------------------------------------------


class OutputInternalMixin():
    ''' This is a "null object". '''

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "OutputInternalMixin"))

    def send_to_failure_queue(self, message):
        logging.info(message_info(121, threading.current_thread().name, message))
        assert isinstance(message, str)
        self.config['sent_to_failure_queue'] += 1

    def send_to_info_queue(self, message):
        logging.info(message_info(128, threading.current_thread().name, message))
        assert isinstance(message, str)
        self.config['sent_to_info_queue'] += 1

# -----------------------------------------------------------------------------
# Class: OutputKafkaMixin
# -----------------------------------------------------------------------------


class OutputKafkaMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "OutputKafkaMixin"))

        # Kafka configuration for failure queuing.

        self.kafka_failure_topic = self.config.get('kafka_failure_topic')
        kafka_producer_configuration = {
            'bootstrap.servers': self.config.get('kafka_failure_bootstrap_server'),
        }
        self.kafka_failure_producer = confluent_kafka.Producer(kafka_producer_configuration)

        # Kafka configuration for info queuing.

        self.kafka_info_topic = self.config.get('kafka_info_topic')
        kafka_producer_configuration = {
            'bootstrap.servers': self.config.get('kafka_info_bootstrap_server'),
        }
        self.kafka_info_producer = confluent_kafka.Producer(kafka_producer_configuration)

    def on_kafka_delivery(self, error, message):
        message_topic = message.topic()
        message_value = message.value()
        message_error = message.error()
        logging.debug(message_debug(103, threading.current_thread().name, message_topic, message_value, message_error, error))
        if error is not None:
            logging.warning(message_warning(408, threading.current_thread().name, message_topic, message_value, message_error, error))

    def send_to_failure_queue(self, message):
        logging.debug(message_debug(916, threading.current_thread().name, self.kafka_failure_topic, message))
        assert isinstance(message, str)
        try:
            self.kafka_failure_producer.produce(self.kafka_failure_topic, message, on_delivery=self.on_kafka_delivery)
            self.config['sent_to_failure_queue'] += 1
        except BufferError as err:
            logging.warning(message_warning(404, threading.current_thread().name, self.kafka_failure_topic, err, message))
        except confluent_kafka.KafkaException as err:
            logging.warning(message_warning(405, threading.current_thread().name, self.kafka_failure_topic, err, message))
        except NotImplementedError as err:
            logging.warning(message_warning(406, threading.current_thread().name, self.kafka_failure_topic, err, message))
        except Exception as err:
            logging.warning(message_warning(407, threading.current_thread().name, self.kafka_failure_topic, err, message))

    def send_to_info_queue(self, message):
        logging.debug(message_debug(916, threading.current_thread().name, self.kafka_info_topic, message))
        assert type(message) == str
        try:
            self.kafka_info_producer.produce(self.kafka_info_topic, message, on_delivery=self.on_kafka_delivery)
            self.config['sent_to_info_queue'] += 1
        except BufferError as err:
            logging.warning(message_warning(404, threading.current_thread().name, self.kafka_info_topic, err, message))
        except confluent_kafka.KafkaException as err:
            logging.warning(message_warning(405, threading.current_thread().name, self.kafka_info_topic, err, message))
        except NotImplementedError as err:
            logging.warning(message_warning(406, threading.current_thread().name, self.kafka_info_topic, err, message))
        except Exception as err:
            logging.warning(message_warning(407, threading.current_thread().name, self.kafka_info_topic, err, message))

# -----------------------------------------------------------------------------
# Class: OutputRabbitmqMixin
# -----------------------------------------------------------------------------


class OutputRabbitmqMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "OutputRabbitmqMixin"))

        # Connect to RabbitMQ for "info".

        self.output_rabbitmq_mixin_info_rabbitmq = Rabbitmq(
            username=self.config.get("rabbitmq_info_username"),
            password=self.config.get("rabbitmq_info_password"),
            host=self.config.get("rabbitmq_info_host"),
            queue_name=self.config.get("rabbitmq_info_queue"),
            exchange=self.config.get("rabbitmq_info_exchange"),
            virtual_host=self.config.get("rabbitmq_info_virtual_host"),
            heartbeat=self.config.get("rabbitmq_heartbeat_in_seconds"),
            routing_key=self.config.get("rabbitmq_info_routing_key"),
            passive=self.config.get("rabbitmq_use_existing_entities"),
        )

        # Connect to RabbitMQ for "failure".

        self.output_rabbitmq_mixin_failure_rabbitmq = Rabbitmq(
            username=self.config.get("rabbitmq_failure_username"),
            password=self.config.get("rabbitmq_failure_password"),
            host=self.config.get("rabbitmq_failure_host"),
            queue_name=self.config.get("rabbitmq_failure_queue"),
            exchange=self.config.get("rabbitmq_failure_exchange"),
            virtual_host=self.config.get("rabbitmq_failure_virtual_host"),
            heartbeat=self.config.get("rabbitmq_heartbeat_in_seconds"),
            routing_key=self.config.get("rabbitmq_failure_routing_key"),
            passive=self.config.get("rabbitmq_use_existing_entities"),
        )

    def send_to_failure_queue(self, message):
        assert isinstance(message, str)
        self.output_rabbitmq_mixin_failure_rabbitmq.send(message)
        self.config['sent_to_failure_queue'] += 1

    def send_to_info_queue(self, message):
        assert isinstance(message, str)
        self.output_rabbitmq_mixin_info_rabbitmq.send(message)
        self.config['sent_to_info_queue'] += 1

# -----------------------------------------------------------------------------
# Class: OutputSqsMixin
# -----------------------------------------------------------------------------


class OutputSqsMixin():
    ''' This is a "null object". '''

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "OutputInternalMixin"))
        self.info_queue_url = self.config.get("sqs_info_queue_url")
        self.failure_queue_url = self.config.get("sqs_failure_queue_url")

        # Create sqs object.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

        regular_expression = "^([^/]+://[^/]+)/"
        regex = re.compile(regular_expression)
        match = regex.match(self.info_queue_url)
        if not match:
            exit_error(750, self.info_queue_url)
        endpoint_url = match.group(1)
        self.sqs = boto3.client("sqs", endpoint_url=endpoint_url)

    def send_to_failure_queue(self, message):
        assert isinstance(message, str)
        response = self.sqs.send_message(
            QueueUrl=self.failure_queue_url,
            DelaySeconds=10,
            MessageAttributes={},
            MessageBody=(message),
        )
        self.config['sent_to_failure_queue'] += 1

    def send_to_info_queue(self, message):
        assert isinstance(message, str)
        response = self.sqs.send_message(
            QueueUrl=self.info_queue_url,
            DelaySeconds=10,
            MessageAttributes={},
            MessageBody=(message),
        )
        self.config['sent_to_info_queue'] += 1

# =============================================================================
# Mixins: Queue*
#   Methods:
#   - send_to_redo_queue(redo_record)
#   Classes:
#   - QueueInternalMixin - Send to internal queue
# =============================================================================

# -----------------------------------------------------------------------------
# Class: QueueInternalMixin
# -----------------------------------------------------------------------------


class QueueInternalMixin():

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(996, threading.current_thread().name, "QueueInternalMixin"))

    def send_to_redo_queue(self, redo_record):
        assert type(redo_record) == tuple
        self.redo_queue.put(redo_record)

# =============================================================================
# Threads: Process*, Queue*
#   Methods:
#   - run
#   Classes:
#   - ProcessRedoQueueThread - Call process_redo_record(redo_record)
#   - QueueRedoRecordsThread - Call send_to_redo_queue(redo_record)
# =============================================================================

# -----------------------------------------------------------------------------
# Class: ProcessRedoQueueThread
# -----------------------------------------------------------------------------


class ProcessRedoQueueThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, g2_configuration_manager=None, redo_queue=None, governor=None):
        threading.Thread.__init__(self)
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoQueueThread"))
        self.config = config
        self.g2_configuration_manager = g2_configuration_manager
        self.g2_engine = g2_engine
        self.governor = governor
        self.info_filter = InfoFilter(g2_engine=g2_engine)
        self.redo_queue = redo_queue

    def filter_info_message(self, message=None):
        assert isinstance(message, str)
        return self.info_filter.filter(message=message)

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

        logging.debug(message_debug(908, threading.current_thread().name, default_config_id))
        self.g2_engine.reinitV2(default_config_id)

    def run(self):
        ''' Process Senzing redo records. '''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Process redo records.

        return_code = 0
        for redo_record, acknowledge_tag in self.redo_records():
            logging.debug(message_debug(922, threading.current_thread().name, "After generator", redo_record))

            # Invoke Governor.

            self.govern()
            logging.debug(message_debug(922, threading.current_thread().name, "After govern()", redo_record))

            # Process record based on the Mixin's process_redo_record() method.

            process_succeeded = self.process_redo_record(redo_record)
            logging.debug(message_debug(922, threading.current_thread().name, "After process_redo_record()", redo_record))

            # Acnkowledge reading the message, if available.
            if process_succeeded:
                try:
                    self.acknowledge_read_message(acknowledge_tag)
                except AttributeError as err:
                    pass

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: QueueRedoRecordsThread
# -----------------------------------------------------------------------------


class QueueRedoRecordsThread(threading.Thread):

    def __init__(self, config=None, g2_engine=None, redo_queue=None):
        threading.Thread.__init__(self)
        logging.debug(message_debug(997, threading.current_thread().name, "QueueRedoRecordsThread"))
        self.config = config
        self.g2_engine = g2_engine
        self.redo_queue = redo_queue

    def redo_records(self):
        '''A generator for producing Senzing redo records.'''

        # Pull values from configuration.

        redo_sleep_time_in_seconds = self.config.get('redo_sleep_time_in_seconds')
        redo_retry_sleep_time_in_seconds = self.config.get('redo_retry_sleep_time_in_seconds')
        redo_retry_limit = self.config.get('redo_retry_limit')

        # Initialize variables.

        redo_record_bytearray = bytearray()
        return_code = 0
        retry_count = 0

        # Read forever.

        while True:

            # Read a Senzing redo record.

            try:
                logging.debug(message_debug(901, threading.current_thread().name))
                return_code = self.g2_engine.getRedoRecord(redo_record_bytearray)
            except G2Exception.G2ModuleNotInitialized as err:
                exit_error(702, err, redo_record_bytearray.decode())
            except Exception as err:
                # OT-TODO: replace this error handling in the future when G2 throws dedicated
                # failed connection exception.
                if is_db_connection_error(err.args[0]) and retry_count < redo_retry_limit:
                    retry_count += 1
                    logging.warning(message_warning(704, err, redo_retry_sleep_time_in_seconds))
                    time.sleep(redo_retry_sleep_time_in_seconds)
                    continue
                exit_error(703, err)
            if return_code:
                exit_error(701, return_code)

            # Reset the retry count
            retry_count = 0

            # If redo record was not received, sleep and try again.

            redo_record = redo_record_bytearray.decode()
            if not redo_record:
                logging.debug(message_debug(902, threading.current_thread().name, redo_sleep_time_in_seconds))
                time.sleep(redo_sleep_time_in_seconds)
                continue

            # Return generator value.

            logging.debug(message_debug(903, threading.current_thread().name, redo_record))
            assert isinstance(redo_record, str)
            self.config['redo_records_from_senzing_engine'] += 1
            yield redo_record, None

    def run(self):
        '''Get redo records from Senzing.  Put redo records in internal queue.'''

        # Show that thread is starting in the log.

        logging.info(message_info(129, threading.current_thread().name))

        # Transfer messages from Senzing to internal queue.

        for redo_record in self.redo_records():
            logging.debug(message_debug(904, threading.current_thread().name, redo_record))
            self.send_to_redo_queue(redo_record)

        # Log message for thread exiting.

        logging.info(message_info(130, threading.current_thread().name))

# =============================================================================
# Classes created with mixins
# =============================================================================

# ---- No external queue ------------------------------------------------------


class ProcessRedoQueueInternalWithInfoThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWithInfoMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoQueueInternalWithInfoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessRedoThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class QueueRedoRecordsInternalThread(QueueRedoRecordsThread, QueueInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "QueueRedoRecordsInternalThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# ---- Azure Queue related ----------------------------------------------------


class ProcessReadFromAzureQueueThread(ProcessRedoQueueThread, InputAzureQueueMixin, ExecuteMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(
            997, threading.current_thread().name, "ProcessReadFromAzureQueueThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessReadFromAzureQueueWithinfoThread(ProcessRedoQueueThread, InputAzureQueueMixin, ExecuteWithInfoMixin, OutputAzureQueueMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread(
        ).name, "ProcessReadFromAzureQueueWithinfoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessRedoWithinfoAzureQueueThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWithInfoMixin, OutputAzureQueueMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread(
        ).name, "ProcessRedoWithinfoAzureQueueThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class QueueRedoRecordsAzureQueueThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWriteToAzureQueueMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(
            997, threading.current_thread().name, "QueueRedoRecordsAzureQueueThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# ---- Kafka related ----------------------------------------------------------


class ProcessReadFromKafkaThread(ProcessRedoQueueThread, InputKafkaMixin, ExecuteMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromKafkaThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessReadFromKafkaWithinfoThread(ProcessRedoQueueThread, InputKafkaMixin, ExecuteWithInfoMixin, OutputKafkaMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromKafkaWithinfoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessRedoWithinfoKafkaThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWithInfoMixin, OutputKafkaMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoWithinfoKafkaThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class QueueRedoRecordsKafkaThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWriteToKafkaMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "QueueRedoRecordsKafkaThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# ---- RabbitMQ related -------------------------------------------------------


class ProcessReadFromRabbitmqThread(ProcessRedoQueueThread, InputRabbitmqMixin, ExecuteMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromRabbitmqThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessReadFromRabbitmqWithinfoThread(ProcessRedoQueueThread, InputRabbitmqMixin, ExecuteWithInfoMixin, OutputRabbitmqMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromRabbitmqWithinfoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessRedoWithinfoRabbitmqThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWithInfoMixin, OutputRabbitmqMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoWithinfoRabbitmqThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class QueueRedoRecordsRabbitmqThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWriteToRabbitmqMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "QueueRedoRecordsRabbitmqThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)

# ---- AWS SQS related --------------------------------------------------------


class ProcessReadFromSqsThread(ProcessRedoQueueThread, InputSqsMixin, ExecuteMixin, OutputInternalMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromSqsThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessReadFromSqsWithinfoThread(ProcessRedoQueueThread, InputSqsMixin, ExecuteWithInfoMixin, OutputSqsMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessReadFromSqsWithinfoThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class ProcessRedoWithinfoSqsThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWithInfoMixin, OutputSqsMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "ProcessRedoWithinfoSqsThread"))
        for base in type(self).__bases__:
            base.__init__(self, *args, **kwargs)


class QueueRedoRecordsSqsThread(ProcessRedoQueueThread, InputInternalMixin, ExecuteWriteToSqsMixin):

    def __init__(self, *args, **kwargs):
        logging.debug(message_debug(997, threading.current_thread().name, "QueueRedoRecordsSqsThread"))
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


def exit_error_program(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(698))
    os._exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(0)


def is_db_connection_error(errorText):
    return 'Database Connection Failure' in errorText or 'Database Connection Lost' in errorText

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
# redo templates
# -----------------------------------------------------------------------------


def redo_processor(
    args=None,
    options_to_defaults_map={},
    read_thread=None,
    process_thread=None,
    monitor_thread=None
):

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # If configuration values not specified, use defaults.

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

    redo_queue = queue.Queue()

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)
    governor = Governor(g2_engine=g2_engine, hint="redoer")

    # Create threads for master process.

    threads = []

    # Add a single thread for reading from Senzing Redo queue and placing on internal queue.

    if read_thread:
        thread = read_thread(
            config=config,
            g2_engine=g2_engine,
            redo_queue=redo_queue
        )
        thread.name = "Process-0-{0}-0".format(thread.__class__.__name__)
        threads.append(thread)

    # Add a number of threads for processing Redo records from internal queue.

    if process_thread:
        for i in range(0, threads_per_process):
            thread = process_thread(
                config=config,
                g2_engine=g2_engine,
                g2_configuration_manager=g2_configuration_manager,
                redo_queue=redo_queue,
                governor=governor
            )
            thread.name = "Process-0-{0}-{1}".format(thread.__class__.__name__, i)
            threads.append(thread)

    # Add a monitoring thread.

    adminThreads = []

    if monitor_thread:
        thread = monitor_thread(
            config=config,
            g2_engine=g2_engine,
            workers=threads
        )
        thread.name = "Process-0-{0}-0".format(thread.__class__.__name__)
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


def do_read_from_azure_queue(args):
    '''
    Read Senzing redo records from Azure Queue and send to G2Engine.process().
    "withinfo" is not returned.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromAzureQueueThread,
        monitor_thread=MonitorThread
    )


def do_read_from_kafka(args):
    '''
    Read Senzing redo records from Kafka and send to G2Engine.process().
    "withinfo" is not returned.
    '''

    options_to_defaults_map = {
        "kafka_redo_bootstrap_server": "kafka_bootstrap_server",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromKafkaThread,
        monitor_thread=MonitorThread
    )


def do_read_from_rabbitmq(args):
    '''
    Read Senzing redo records from RabbitMQ and send to G2Engine.process().
    "withinfo" is not returned.
    '''

    options_to_defaults_map = {
        "rabbitmq_redo_exchange": "rabbitmq_exchange",
        "rabbitmq_redo_host": "rabbitmq_host",
        "rabbitmq_redo_password": "rabbitmq_password",
        "rabbitmq_redo_username": "rabbitmq_username",
        "rabbitmq_redo_virtual_host": "rabbitmq_virtual_host",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromRabbitmqThread,
        monitor_thread=MonitorThread
    )


def do_read_from_sqs(args):
    '''
    Read Senzing redo records from RabbitMQ and send to G2Engine.process().
    "withinfo" is not returned.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromSqsThread,
        monitor_thread=MonitorThread
    )


def do_read_from_azure_queue_withinfo(args):
    '''
    Read Senzing redo records from Azure queue and send to G2Engine.processWithInfo().
    "withinfo" returned is sent to Azure queue.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromAzureQueueWithinfoThread,
        monitor_thread=MonitorThread
    )


def do_read_from_kafka_withinfo(args):
    '''
    Read Senzing redo records from Kafka and send to G2Engine.processWithInfo().
    "withinfo" returned is sent to Kafka.
    '''

    options_to_defaults_map = {
        "kafka_failure_bootstrap_server": "kafka_bootstrap_server",
        "kafka_info_bootstrap_server": "kafka_bootstrap_server",
        "kafka_redo_bootstrap_server": "kafka_bootstrap_server",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromKafkaWithinfoThread,
        monitor_thread=MonitorThread
    )


def do_read_from_rabbitmq_withinfo(args):
    '''
    Read Senzing redo records from RabbitMQ and send to G2Engine.processWithInfo().
    "withinfo" returned is sent to RabbitMQ.
    '''

    options_to_defaults_map = {
        "rabbitmq_failure_exchange": "rabbitmq_exchange",
        "rabbitmq_failure_host": "rabbitmq_host",
        "rabbitmq_failure_password": "rabbitmq_password",
        "rabbitmq_failure_username": "rabbitmq_username",
        "rabbitmq_failure_virtual_host": "rabbitmq_virtual_host",
        "rabbitmq_info_exchange": "rabbitmq_exchange",
        "rabbitmq_info_host": "rabbitmq_host",
        "rabbitmq_info_password": "rabbitmq_password",
        "rabbitmq_info_username": "rabbitmq_username",
        "rabbitmq_info_virtual_host": "rabbitmq_virtual_host",
        "rabbitmq_redo_exchange": "rabbitmq_exchange",
        "rabbitmq_redo_host": "rabbitmq_host",
        "rabbitmq_redo_password": "rabbitmq_password",
        "rabbitmq_redo_username": "rabbitmq_username",
        "rabbitmq_redo_virtual_host": "rabbitmq_virtual_host",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromRabbitmqWithinfoThread,
        monitor_thread=MonitorThread
    )


def do_read_from_sqs_withinfo(args):
    '''
    Read Senzing redo records from AWS SQS and send to G2Engine.processWithInfo().
    "withinfo" returned is sent to AWS SQS.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        process_thread=ProcessReadFromSqsWithinfoThread,
        monitor_thread=MonitorThread
    )


def do_redo(args):
    '''
    Read Senzing redo records from Senzing SDK and send to G2Engine.process().
    No external queues are used.  "withinfo" is not returned.
    '''

    redo_processor(
        args=args,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=ProcessRedoThread,
        monitor_thread=MonitorThread
    )


def do_redo_withinfo_azure_queue(args):
    '''
    Read Senzing redo records from Senzing SDK and send to G2Engine.processWithInfo().
    No external queues are used.  "withinfo" returned is sent to Azure queue.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=ProcessRedoWithinfoAzureQueueThread,
        monitor_thread=MonitorThread
    )


def do_redo_withinfo_kafka(args):
    '''
    Read Senzing redo records from Senzing SDK and send to G2Engine.processWithInfo().
    No external queues are used.  "withinfo" returned is sent to kafka.
    '''

    options_to_defaults_map = {
        "kafka_failure_bootstrap_server": "kafka_bootstrap_server",
        "kafka_info_bootstrap_server": "kafka_bootstrap_server",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=ProcessRedoWithinfoKafkaThread,
        monitor_thread=MonitorThread
    )


def do_redo_withinfo_rabbitmq(args):
    '''
    Read Senzing redo records from Senzing SDK and send to G2Engine.processWithInfo().
    No external queues are used.  "withinfo" returned is sent to RabbitMQ.
    '''

    options_to_defaults_map = {
        "rabbitmq_failure_exchange": "rabbitmq_exchange",
        "rabbitmq_failure_host": "rabbitmq_host",
        "rabbitmq_failure_password": "rabbitmq_password",
        "rabbitmq_failure_username": "rabbitmq_username",
        "rabbitmq_failure_virtual_host": "rabbitmq_virtual_host",
        "rabbitmq_info_exchange": "rabbitmq_exchange",
        "rabbitmq_info_host": "rabbitmq_host",
        "rabbitmq_info_password": "rabbitmq_password",
        "rabbitmq_info_username": "rabbitmq_username",
        "rabbitmq_info_virtual_host": "rabbitmq_virtual_host",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=ProcessRedoWithinfoRabbitmqThread,
        monitor_thread=MonitorThread
    )


def do_redo_withinfo_sqs(args):
    '''
    Read Senzing redo records from Senzing SDK and send to G2Engine.processWithInfo().
    No external queues are used.  "withinfo" returned is sent to RabbitMQ.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=ProcessRedoWithinfoSqsThread,
        monitor_thread=MonitorThread
    )


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


def do_write_to_azure_queue(args):
    '''
    Read Senzing redo records from Senzing SDK and send to Azure Queue.
    No g2_engine processing is done.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=QueueRedoRecordsAzureQueueThread,
        monitor_thread=MonitorThread
    )


def do_write_to_kafka(args):
    '''
    Read Senzing redo records from Senzing SDK and send to Kafka.
    No g2_engine processing is done.
    '''

    options_to_defaults_map = {
        "kafka_redo_bootstrap_server": "kafka_bootstrap_server",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=QueueRedoRecordsKafkaThread,
        monitor_thread=MonitorThread
    )


def do_write_to_rabbitmq(args):
    '''
    Read Senzing redo records from Senzing SDK and send to RabbitMQ.
    No g2_engine processing is done.
    '''

    options_to_defaults_map = {
        "rabbitmq_redo_exchange": "rabbitmq_exchange",
        "rabbitmq_redo_host": "rabbitmq_host",
        "rabbitmq_redo_password": "rabbitmq_password",
        "rabbitmq_redo_username": "rabbitmq_username",
        "rabbitmq_redo_virtual_host": "rabbitmq_virtual_host",
    }

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=QueueRedoRecordsRabbitmqThread,
        monitor_thread=MonitorThread
    )


def do_write_to_sqs(args):
    '''
    Read Senzing redo records from Senzing SDK and send to AWS SQS.
    No g2_engine processing is done.
    '''

    options_to_defaults_map = {}

    redo_processor(
        args=args,
        options_to_defaults_map=options_to_defaults_map,
        read_thread=QueueRedoRecordsInternalThread,
        process_thread=QueueRedoRecordsSqsThread,
        monitor_thread=MonitorThread
    )


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

    # Import plugins

    try:
        import senzing_governor
        from senzing_governor import Governor
        logging.info(message_info(180, senzing_governor.__file__))
    except ImportError:
        pass

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
