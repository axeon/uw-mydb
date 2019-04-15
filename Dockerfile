FROM frolvlad/alpine-java:jdk8-slim

ENV TIMEZONE="Asia/Shanghai"
# Set Timezone
RUN set -ex \
    && sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories \
    && apk add --no-cache --virtual .build-deps tzdata \
    && cp /usr/share/zoneinfo/${TIMEZONE} /etc/localtime \
    && echo "${TIMEZONE}" > /etc/timezone \
    && apk del .build-deps

ARG APP_VERSION="1.0.0"
ENV APP_NAME=uw-mydb-${APP_VERSION}
ADD target/$APP_NAME.jar /

ENV JAVA_OPTS=""
ENV SPRING_BOOT_OPTS=""

EXPOSE 8080
ENTRYPOINT exec java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /$APP_NAME.jar $SPRING_BOOT_OPTS