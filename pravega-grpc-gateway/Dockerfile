#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Building Container

FROM gradle:4.10-jdk8 as GradleBuilder

USER 0

RUN apt-get update \
    && apt-get install -y \
        maven \
    && rm -rf /var/lib/apt/lists/*

USER gradle

ENV APP_NAME=pravega-grpc-gateway

COPY --chown=gradle:gradle build.gradle /home/gradle/src/build.gradle
COPY --chown=gradle:gradle gradle.properties /home/gradle/src/gradle.properties
COPY --chown=gradle:gradle settings.gradle /home/gradle/src/settings.gradle
COPY --chown=gradle:gradle ${APP_NAME} /home/gradle/src/${APP_NAME}

WORKDIR /home/gradle/src

ENV GRADLE_USER_HOME=/home/gradle

RUN gradle :${APP_NAME}:installDist \
--no-daemon --info --stacktrace

# Runtime Container

FROM openjdk:8-jre

ENV APP_NAME=pravega-grpc-gateway
ENV LISTEN_PORT=80

EXPOSE 80

COPY --from=GradleBuilder /home/gradle/src/${APP_NAME}/build/install/${APP_NAME} /opt/${APP_NAME}

ENTRYPOINT /opt/${APP_NAME}/bin/${APP_NAME}
