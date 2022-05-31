ARG BASE_IMAGE=debian:11.3-slim@sha256:06a93cbdd49a265795ef7b24fe374fee670148a7973190fb798e43b3cf7c5d0f

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${BASE_IMAGE} AS builder

ENV REFRESHED_AT=2022-05-16

LABEL Name="senzing/redoer" \
      Maintainer="support@senzing.com" \
      Version="1.5.3"

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      libaio1 \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment.

RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Install packages via PIP.

COPY requirements.txt .
RUN pip3 install --upgrade pip \
 && pip3 install -r requirements.txt \
 && rm /requirements.txt

# -----------------------------------------------------------------------------
# Stage: Final
# -----------------------------------------------------------------------------

# Create the runtime image.

FROM ${BASE_IMAGE} AS runner

ENV REFRESHED_AT=2022-05-16

LABEL Name="senzing/redoer" \
      Maintainer="support@senzing.com" \
      Version="1.5.3"

# Define health check.

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
      libaio1 \
      libodbc1 \
      librdkafka-dev \
      libssl1.1 \
      libxml2 \
      postgresql-client \
      python3 \
      python3-venv \
      unixodbc \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copy files from repository.

COPY ./rootfs /
COPY ./redoer.py /app/

# Copy python virtual environment from the builder image.

COPY --from=builder /app/venv /app/venv

# Make non-root container.

USER 1001

# Activate virtual environment.

ENV VIRTUAL_ENV=/app/venv
ENV PATH="/app/venv/bin:${PATH}"

# Runtime environment variables.

ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib:/opt/senzing/g2/lib/debian:/opt/IBM/db2/clidriver/lib
ENV PATH=${PATH}:/opt/senzing/g2/python:/opt/IBM/db2/clidriver/adm:/opt/IBM/db2/clidriver/bin
ENV PYTHONPATH=/opt/senzing/g2/python
ENV PYTHONUNBUFFERED=1
ENV SENZING_DOCKER_LAUNCHED=true
ENV SENZING_ETC_PATH=/etc/opt/senzing

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["/app/redoer.py"]
