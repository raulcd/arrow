# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM debian:bookworm

ENV DEBIAN_FRONTEND noninteractive

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    apt-utils \
    createrepo-c \
    devscripts \
    gpg \
    locales \
    openssh-server \
    rake \
    rpm \
    ruby \
    sudo && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*

RUN gem install apt-dists-merge -v ">= 1.0.2"

RUN locale-gen en_US.UTF-8

RUN mkdir -p /run/sshd
RUN echo "StreamLocalBindUnlink yes" >> /etc/ssh/sshd_config

ENV ARROW_USER arrow
ENV ARROW_UID 10000

RUN \
  groupadd --gid ${ARROW_UID} ${ARROW_USER} && \
  useradd --uid ${ARROW_UID} --gid ${ARROW_UID} --create-home ${ARROW_USER} && \
  mkdir -p /home/arrow/.gnupg /home/arrow/.ssh && \
  chown -R arrow: /home/arrow/.gnupg /home/arrow/.ssh && \
  chmod -R og-rwx /home/arrow/.gnupg /home/arrow/.ssh && \
  echo "${ARROW_USER} ALL=(ALL:ALL) NOPASSWD:ALL" | \
    EDITOR=tee visudo -f /etc/sudoers.d/arrow

COPY id_rsa.pub /home/arrow/.ssh/authorized_keys
RUN \
  chown -R arrow: /home/arrow/.ssh && \
  chmod -R og-rwx /home/arrow/.ssh

COPY runner.sh /home/arrow/runner.sh
RUN \
  chown -R arrow: /home/arrow/runner.sh && \
  chmod +x /home/arrow/runner.sh

EXPOSE 22
