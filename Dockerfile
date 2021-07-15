ARG BASE_IMAGE=senzing/senzing-base:1.6.1
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2021-07-15

LABEL Name="senzing/redoer" \
      Maintainer="support@senzing.com" \
      Version="1.3.9"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

COPY requirements.txt ./
RUN pip3 install --upgrade pip \
 && pip3 install -r requirements.txt

# Copy files from repository.

COPY ./rootfs /
COPY ./redoer.py /app/

# Make non-root container.

USER 1001

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/redoer.py"]
