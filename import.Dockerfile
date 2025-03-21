ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-build-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=pelican

# create gen3 user
# Create a group 'gen3' with GID 1000 and a user 'gen3' with UID 1000
RUN groupadd -g 1000 gen3 && \
    useradd -m -s /bin/bash -u 1000 -g gen3 gen3

# Install pipx
RUN python3 -m pip install pipx && \
    python3 -m pipx ensurepath

USER gen3
# Install Poetry via pipx
RUN pipx install poetry
ENV PATH="/home/gen3/.local/bin:${PATH}"
USER root

WORKDIR /${appname}

RUN dnf update && dnf install -y \
    wget \
    tar \
    java-11-amazon-corretto \
    gnutls \
    && rm -rf /var/cache/yum

ENV HADOOP_VERSION="3.2.1"
ENV HADOOP_HOME="/hadoop" \
    HADOOP_INSTALLATION_URL="http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"

RUN wget ${HADOOP_INSTALLATION_URL} \
    && mkdir -p $HADOOP_HOME \
    && tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1 \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && rm -rf $HADOOP_HOME/share/doc \
    && chown -R gen3:gen3 $HADOOP_HOME

ENV SQOOP_VERSION="1.4.7"
ENV SQOOP_HOME="/sqoop" \
    SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz" \
    SQOOP_MD5_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz.md5"

RUN wget -q ${SQOOP_INSTALLATION_URL} \
    && wget -qO- ${SQOOP_MD5_URL} | md5sum -c - \
    && mkdir -p $SQOOP_HOME \
    && tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1 \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && rm -rf $SQOOP_HOME/docs \
    && chown -R gen3:gen3 $SQOOP_HOME

ENV POSTGRES_JAR_VERSION="42.2.9"
ENV POSTGRES_JAR_URL="https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JAR_VERSION}.jar" \
    POSTGRES_JAR_PATH=$SQOOP_HOME/lib/postgresql-${POSTGRES_JAR_VERSION}.jar \
    JAVA_HOME="/usr/lib/jvm/java-11-amazon-corretto"

RUN wget ${POSTGRES_JAR_URL} -O ${POSTGRES_JAR_PATH}

ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop" \
    HADOOP_MAPRED_HOME="${HADOOP_HOME}" \
    HADOOP_COMMON_HOME="${HADOOP_HOME}" \
    HADOOP_HDFS_HOME="${HADOOP_HOME}" \
    YARN_HOME="${HADOOP_HOME}" \
    ACCUMULO_HOME="/accumulo" \
    HIVE_HOME="/hive" \
    HBASE_HOME="/hbase" \
    HCAT_HOME="/hcatalog" \
    ZOOKEEPER_HOME="/zookeeper" \
    HADOOP_COMMON_LIB_NATIVE_DIR="${HADOOP_HOME}/lib/native" \
    LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}"

RUN mkdir -p $ACCUMULO_HOME $HIVE_HOME $HBASE_HOME $HCAT_HOME $ZOOKEEPER_HOME

RUN chown -R gen3:gen3 $ACCUMULO_HOME $HIVE_HOME $HBASE_HOME $HCAT_HOME $ZOOKEEPER_HOME $JAVA_HOME $POSTGRES_JAR_PATH

ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}

# Builder stage
FROM base AS builder

RUN dnf update && dnf install -y \
    python3-devel \
    gcc \
    postgresql-devel

COPY . /${appname}

# cache so that poetry install will run if these files change
COPY poetry.lock pyproject.toml /${appname}/

RUN poetry install -vv --no-interaction --without dev

# Final stage
FROM base

COPY --from=builder /venv /venv
COPY --from=builder /${appname} /${appname}

# Switch to non-root user 'gen3' for the serving process
USER gen3

ENV PYTHONUNBUFFERED=1

ENTRYPOINT poetry run python job_import.py
