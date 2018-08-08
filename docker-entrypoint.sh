#!/bin/sh
set -e

export UW_AUTH_CLIENT_HOST_ID=$HOSTNAME

exec /usr/bin/java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /$APP_NAME.jar $SPRING_BOOT_OPTS
