# redoer

If you are beginning your journey with [Senzing],
please start with [Senzing Quick Start guides].

You are in the [Senzing Garage] where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## Synopsis

Process Senzing redo records.

## Overview

The [redoer.py] python script processes Senzing "redo" records.
The `senzing/redoer` docker image is a wrapper for use in docker formations (e.g. docker-compose, kubernetes).

To see all of the subcommands, run:

```console
$ ./redoer.py

usage: redoer.py [-h]
                 {redo,redo-withinfo-kafka,redo-withinfo-rabbitmq,redo-withinfo-sqs,read-from-kafka,read-from-kafka-withinfo,read-from-rabbitmq,read-from-rabbitmq-withinfo,read-from-sqs,read-from-sqs-withinfo,write-to-kafka,write-to-rabbitmq,write-to-sqs,sleep,version,docker-acceptance-test}
                 ...

Process Senzing redo records. For more information, see https://github.com/senzing-garage/redoer

positional arguments:
  {redo,redo-withinfo-kafka,redo-withinfo-rabbitmq,redo-withinfo-sqs,read-from-kafka,read-from-kafka-withinfo,read-from-rabbitmq,read-from-rabbitmq-withinfo,read-from-sqs,read-from-sqs-withinfo,write-to-kafka,write-to-rabbitmq,write-to-sqs,sleep,version,docker-acceptance-test}

                                 Subcommands (SENZING_SUBCOMMAND):
    redo                         Read Senzing redo records from Senzing SDK and send to G2Engine.process()
    redo-withinfo-kafka          Read Senzing redo records from Senzing SDK, send toG2Engine.processWithInfo(), results sent to Kafka.
    redo-withinfo-rabbitmq       Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to RabbitMQ.
    redo-withinfo-sqs            Read Senzing redo records from Senzing SDK, send to G2Engine.processWithInfo(), results sent to AWS SQS.
    read-from-kafka              Read Senzing redo records from Kafka and send to G2Engine.process()
    read-from-kafka-withinfo     Read Senzing redo records from Kafka and send to G2Engine.processWithInfo()
    read-from-rabbitmq           Read Senzing redo records from RabbitMQ and send to G2Engine.process()
    read-from-rabbitmq-withinfo  Read Senzing redo records from RabbitMQ and send to G2Engine.processWithInfo()
    read-from-sqs                Read Senzing redo records from AWS SQS and send to G2Engine.process()
    read-from-sqs-withinfo       Read Senzing redo records from AWS SQS and send to G2Engine.processWithInfo()
    write-to-kafka               Read Senzing redo records from Senzing SDK and send to Kafka.
    write-to-rabbitmq            Read Senzing redo records from Senzing SDK and send to RabbitMQ.
    write-to-sqs                 Read Senzing redo records from Senzing SDK and send to AWS SQS.
    sleep                        Do nothing but sleep. For Docker testing.
    version                      Print version of program.
    docker-acceptance-test       For Docker acceptance testing.

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./redoer redo --help
```

### Contents

1. [Preamble]
   1. [Legend]
1. [Expectations]
1. [Demonstrate using Command Line Interface]
   1. [Prerequisites for CLI]
   1. [Download]
   1. [Environment variables for CLI]
   1. [Run command]
1. [Demonstrate using Docker]
   1. [Prerequisites for Docker]
   1. [Database support]
   1. [External database]
   1. [Run Docker container]
1. [Configuration]
1. [References]
1. [License]

## Preamble

At [Senzing], we strive to create GitHub documentation in a
"[don't make me think]" style. For the most part, instructions are copy and paste.
Whenever thinking is needed, it's marked with a "thinking" icon :thinking:.
Whenever customization is needed, it's marked with a "pencil" icon :pencil2:.
If the instructions are not clear, please let us know by opening a new
[Documentation issue] describing where we can improve. Now on with the show...

### Legend

1. :thinking: - A "thinker" icon means that a little extra thinking may be required.
   Perhaps there are some choices to be made.
   Perhaps it's an optional step.
1. :pencil2: - A "pencil" icon means that the instructions may need modification before performing.
1. :warning: - A "warning" icon means that something tricky is happening, so pay attention.

## Expectations

- **Space:** This repository and demonstration require 6 GB free disk space.
- **Time:** Budget 40 minutes to get the demonstration up-and-running, depending on CPU and network speeds.
- **Background knowledge:** This repository assumes a working knowledge of:
  - [Docker]

## Demonstrate using Command Line Interface

### Prerequisites for CLI

:thinking: The following tasks need to be complete before proceeding.
These are "one-time tasks" which may already have been completed.

1. Install system dependencies:
   1. Use `apt` based installation for [Debian, Ubuntu, and others]
      1. See [apt-packages.txt] for list
   1. Use `yum` based installation for [Red Hat, CentOS, openSuse and others].
      1. See [yum-packages.txt] for list
1. Install Python dependencies:
   1. See [requirements.txt] for list
      1. [Installation hints]
1. :thinking: **Optional:** Some databases need additional support.
   For other databases, this step may be skipped.
   1. **Db2:** See
      [Support Db2].
   1. **MS SQL:** See
      [Support MS SQL].
1. [Configure Senzing database]

### Download

1. Get a local copy of [template-python.py].
   Example:

   1. :pencil2: Specify where to download file.
      Example:

      ```console
      export SENZING_DOWNLOAD_FILE=~/redoer.py
      ```

   1. Download file.
      Example:

      ```console
      curl -X GET \
        --output ${SENZING_DOWNLOAD_FILE} \
        https://raw.githubusercontent.com/Senzing/redoer/main/redoer.py
      ```

   1. Make file executable.
      Example:

      ```console
      chmod +x ${SENZING_DOWNLOAD_FILE}
      ```

1. :thinking: **Alternative:** The entire git repository can be downloaded by following instructions at
   [Clone repository]

### Environment variables for CLI

1. :pencil2: Identify the Senzing `g2` directory.
   Example:

   ```console
   export SENZING_G2_DIR=/opt/senzing/g2
   ```

   1. Here's a simple test to see if `SENZING_G2_DIR` is correct.
      The following command should return file contents.
      Example:

      ```console
      cat ${SENZING_G2_DIR}/g2BuildVersion.json
      ```

1. Set common environment variables
   Example:

   ```console
   export PYTHONPATH=${SENZING_G2_DIR}/sdk/python
   ```

1. :thinking: Set operating system specific environment variables.
   Choose one of the options.

   1. **Option #1:** For [Debian, Ubuntu, and others].
      Example:

      ```console
      export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:${SENZING_G2_DIR}/lib/debian:$LD_LIBRARY_PATH
      ```

   1. **Option #2** For [Red Hat, CentOS, openSuse and others].
      Example:

      ```console
      export LD_LIBRARY_PATH=${SENZING_G2_DIR}/lib:$LD_LIBRARY_PATH
      ```

### Run command

1. Run the command.
   Example:

   ```console
   ${SENZING_DOWNLOAD_FILE} --help
   ```

## Demonstrate using Docker

### Prerequisites for Docker

:thinking: The following tasks need to be complete before proceeding.
These are "one-time tasks" which may already have been completed.

1. The following software programs need to be installed:
   1. [docker]

### Database support

:thinking: **Optional:** Some databases need additional support.
For other databases, these steps may be skipped.

1. **Db2:** See [Support Db2] instructions to set `SENZING_OPT_IBM_DIR_PARAMETER`.
1. **MS SQL:** See [Support MS SQL] instructions to set `SENZING_OPT_MICROSOFT_DIR_PARAMETER`.

### External database

:thinking: **Optional:** Use if storing data in an external database.
If not specified, the internal SQLite database will be used.

1. :pencil2: Specify database.
   Example:

   ```console
   export DATABASE_PROTOCOL=postgresql
   export DATABASE_USERNAME=postgres
   export DATABASE_PASSWORD=postgres
   export DATABASE_HOST=senzing-postgresql
   export DATABASE_PORT=5432
   export DATABASE_DATABASE=G2
   ```

1. Construct Database URL.
   Example:

   ```console
   export SENZING_DATABASE_URL="${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}"
   ```

1. Construct parameter for `docker run`.
   Example:

   ```console
   export SENZING_DATABASE_URL_PARAMETER="--env SENZING_DATABASE_URL=${SENZING_DATABASE_URL}"
   ```

### Run Docker container

Although the `Docker run` command looks complex,
it accounts for all of the optional variations described above.
Unset `*_PARAMETER` environment variables have no effect on the
`docker run` command and may be removed or remain.

1. :pencil2: Set environment variables.
   Example:

   ```console
   export SENZING_SUBCOMMAND=redo
   ```

1. Run Docker container.
   Example:

   ```console
   sudo docker run \
     --env SENZING_SUBCOMMAND \
     --interactive \
     --rm \
     --tty \
     ${SENZING_DATABASE_URL_PARAMETER} \
     ${SENZING_OPT_IBM_DIR_PARAMETER} \
     ${SENZING_OPT_MICROSOFT_DIR_PARAMETER} \
     senzing/redoer
   ```

## Configuration

Configuration values specified by environment variable or command line parameter.

- **[SENZING_CONFIG_PATH]**
- **[SENZING_DATABASE_URL]**
- **[SENZING_DATA_SOURCE]**
- **[SENZING_DEBUG]**
- **[SENZING_DELAY_IN_SECONDS]**
- **[SENZING_ENGINE_CONFIGURATION_JSON]**
- **[SENZING_EXIT_ON_THREAD_TERMINATION]**
- **[SENZING_EXPIRATION_WARNING_IN_DAYS]**
- **[SENZING_INPUT_URL]**
- **[SENZING_KAFKA_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_FAILURE_TOPIC]**
- **[SENZING_KAFKA_INFO_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_INFO_TOPIC]**
- **[SENZING_KAFKA_REDO_BOOTSTRAP_SERVER]**
- **[SENZING_KAFKA_REDO_GROUP]**
- **[SENZING_KAFKA_REDO_TOPIC]**
- **[SENZING_LOG_LEVEL]**
- **[SENZING_LOG_LICENSE_PERIOD_IN_SECONDS]**
- **[SENZING_MONITORING_PERIOD_IN_SECONDS]**
- **[SENZING_NETWORK]**
- **[SENZING_QUEUE_MAX_SIZE]**
- **[SENZING_RABBITMQ_DELIVERY_MODE]**
- **[SENZING_RABBITMQ_EXCHANGE]**
- **[SENZING_RABBITMQ_FAILURE_EXCHANGE]**
- **[SENZING_RABBITMQ_FAILURE_HOST]**
- **[SENZING_RABBITMQ_FAILURE_PASSWORD]**
- **[SENZING_RABBITMQ_FAILURE_QUEUE]**
- **[SENZING_RABBITMQ_FAILURE_ROUTING_KEY]**
- **[SENZING_RABBITMQ_FAILURE_USERNAME]**
- **[SENZING_RABBITMQ_HOST]**
- **[SENZING_RABBITMQ_INFO_EXCHANGE]**
- **[SENZING_RABBITMQ_INFO_HOST]**
- **[SENZING_RABBITMQ_INFO_PASSWORD]**
- **[SENZING_RABBITMQ_INFO_QUEUE]**
- **[SENZING_RABBITMQ_INFO_ROUTING_KEY]**
- **[SENZING_RABBITMQ_INFO_USERNAME]**
- **[SENZING_RABBITMQ_PASSWORD]**
- **[SENZING_RABBITMQ_PREFETCH_COUNT]**
- **[SENZING_RABBITMQ_REDO_HOST]**
- **[SENZING_RABBITMQ_REDO_PASSWORD]**
- **[SENZING_RABBITMQ_REDO_QUEUE]**
- **[SENZING_RABBITMQ_REDO_ROUTING_KEY]**
- **[SENZING_RABBITMQ_REDO_USERNAME]**
- **[SENZING_RABBITMQ_USERNAME]**
- **[SENZING_RABBITMQ_USE_EXISTING_ENTITIES]**
- **[SENZING_REDO_RETRY_LIMIT]**
- **[SENZING_REDO_RETRY_SLEEP_TIME_IN_SECONDS]**
- **[SENZING_REDO_SLEEP_TIME_IN_SECONDS]**
- **[SENZING_RESOURCE_PATH]**
- **[SENZING_RUNAS_USER]**
- **[SENZING_SLEEP_TIME_IN_SECONDS]**
- **[SENZING_SQS_FAILURE_QUEUE_URL]**
- **[SENZING_SQS_INFO_QUEUE_URL]**
- **[SENZING_SQS_REDO_QUEUE_URL]**
- **[SENZING_SUBCOMMAND]**
- **[SENZING_SUPPORT_PATH]**
- **[SENZING_THREADS_PER_PROCESS]**

## License

View [license information] for the software container in this Docker image.
Note that this license does not permit further distribution.

This Docker image may also contain software from the
[Senzing GitHub community] under the [Apache License 2.0].

Further, as with all Docker images,
this likely also contains other software which may be under other licenses
(such as Bash, etc. from the base distribution,
along with any direct or indirect dependencies of the primary software being contained).

As for any pre-built image usage,
it is the image user's responsibility to ensure that any use of this image complies
with any relevant licenses for all software contained within.

## References

1. [Development]
1. [Errors]
1. [Examples]
1. Related artifacts:
   1. [DockerHub]
   1. [Helm Chart]

[Apache License 2.0]: https://www.apache.org/licenses/LICENSE-2.0
[apt-packages.txt]: src/apt-packages.txt
[Clone repository]: docs/development.md#clone-repository
[Configuration]: #configuration
[Configure Senzing database]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/configure-senzing-database.md
[Database support]: #database-support
[Debian, Ubuntu, and others]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#Debian-based
[Demonstrate using Command Line Interface]: #demonstrate-using-command-line-interface
[Demonstrate using Docker]: #demonstrate-using-docker
[Development]: docs/development.md
[Docker]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/docker.md
[DockerHub]: https://hub.docker.com/r/senzing/redoer
[Documentation issue]: https://github.com/senzing-garage/template-python/issues/new?template=documentation_request.md
[don't make me think]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/dont-make-me-think.md
[Download]: #download
[Environment variables for CLI]: #environment-variables-for-cli
[Errors]: docs/errors.md
[Examples]: docs/examples.md
[Expectations]: #expectations
[External database]: #external-database
[Helm Chart]: https://github.com/senzing-garage/charts/tree/main/charts/senzing-redoer
[Installation hints]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/install-python-dependencies.md
[Legend]: #legend
[license information]: https://senzing.com/end-user-license-agreement/
[License]: #license
[Preamble]: #preamble
[Prerequisites for CLI]: #prerequisites-for-cli
[Prerequisites for Docker]: #prerequisites-for-docker
[Red Hat, CentOS, openSuse and others]: https://en.wikipedia.org/wiki/List_of_Linux_distributions#RPM-based
[redoer.py]: redoer.py
[References]: #references
[requirements.txt]: requirements.txt
[Run command]: #run-command
[Run Docker container]: #run-docker-container
[Senzing Garage]: https://github.com/senzing-garage
[Senzing GitHub community]: https://github.com/senzing-garage/
[Senzing Quick Start guides]: https://docs.senzing.com/quickstart/
[SENZING_CONFIG_PATH]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_config_path
[SENZING_DATA_SOURCE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_data_source
[SENZING_DATABASE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_database_url
[SENZING_DEBUG]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_debug
[SENZING_DELAY_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_delay_in_seconds
[SENZING_ENGINE_CONFIGURATION_JSON]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_engine_configuration_json
[SENZING_EXIT_ON_THREAD_TERMINATION]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_exit_on_thread_termination
[SENZING_EXPIRATION_WARNING_IN_DAYS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_expiration_warning_in_days
[SENZING_INPUT_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_input_url
[SENZING_KAFKA_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_bootstrap_server
[SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_failure_bootstrap_server
[SENZING_KAFKA_FAILURE_TOPIC]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_failure_topic
[SENZING_KAFKA_INFO_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_info_bootstrap_server
[SENZING_KAFKA_INFO_TOPIC]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_info_topic
[SENZING_KAFKA_REDO_BOOTSTRAP_SERVER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_redo_bootstrap_server
[SENZING_KAFKA_REDO_GROUP]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_redo_group
[SENZING_KAFKA_REDO_TOPIC]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_kafka_redo_topic
[SENZING_LOG_LEVEL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_log_level
[SENZING_LOG_LICENSE_PERIOD_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_log_license_period_in_seconds
[SENZING_MONITORING_PERIOD_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_monitoring_period_in_seconds
[SENZING_NETWORK]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_network
[SENZING_QUEUE_MAX_SIZE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_queue_max_size
[SENZING_RABBITMQ_DELIVERY_MODE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_delivery_mode
[SENZING_RABBITMQ_EXCHANGE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_exchange
[SENZING_RABBITMQ_FAILURE_EXCHANGE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_exchange
[SENZING_RABBITMQ_FAILURE_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_host
[SENZING_RABBITMQ_FAILURE_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_password
[SENZING_RABBITMQ_FAILURE_QUEUE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_queue
[SENZING_RABBITMQ_FAILURE_ROUTING_KEY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_routing_key
[SENZING_RABBITMQ_FAILURE_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_failure_username
[SENZING_RABBITMQ_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_host
[SENZING_RABBITMQ_INFO_EXCHANGE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_exchange
[SENZING_RABBITMQ_INFO_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_host
[SENZING_RABBITMQ_INFO_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_password
[SENZING_RABBITMQ_INFO_QUEUE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_queue
[SENZING_RABBITMQ_INFO_ROUTING_KEY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_routing_key
[SENZING_RABBITMQ_INFO_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_info_username
[SENZING_RABBITMQ_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_password
[SENZING_RABBITMQ_PREFETCH_COUNT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_prefetch_count
[SENZING_RABBITMQ_REDO_HOST]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_redo_host
[SENZING_RABBITMQ_REDO_PASSWORD]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_redo_password
[SENZING_RABBITMQ_REDO_QUEUE]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_redo_queue
[SENZING_RABBITMQ_REDO_ROUTING_KEY]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_redo_routing_key
[SENZING_RABBITMQ_REDO_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_redo_username
[SENZING_RABBITMQ_USE_EXISTING_ENTITIES]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_use_existing_entities
[SENZING_RABBITMQ_USERNAME]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_rabbitmq_username
[SENZING_REDO_RETRY_LIMIT]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_redo_retry_limit
[SENZING_REDO_RETRY_SLEEP_TIME_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_redo_retry_sleep_time_in_seconds
[SENZING_REDO_SLEEP_TIME_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_redo_sleep_tinme_in_seconds
[SENZING_RESOURCE_PATH]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_resource_path
[SENZING_RUNAS_USER]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_runas_user
[SENZING_SLEEP_TIME_IN_SECONDS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sleep_time_in_seconds
[SENZING_SQS_FAILURE_QUEUE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sqs_failure_queue_url
[SENZING_SQS_INFO_QUEUE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sqs_info_queue_url
[SENZING_SQS_REDO_QUEUE_URL]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_sqs_redo_queue_url
[SENZING_SUBCOMMAND]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_subcommand
[SENZING_SUPPORT_PATH]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_support_path
[SENZING_THREADS_PER_PROCESS]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_threads_per_process
[Senzing]: https://senzing.com/
[Support Db2]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/support-db2.md
[Support MS SQL]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/support-mssql.md
[template-python.py]: template-python.py
[yum-packages.txt]: src/yum-packages.txt
