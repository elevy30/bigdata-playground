# AlpineLinux with a glibc-2.21 and Oracle Java 8
FROM elevy30/alpine-ojre8:slim

MAINTAINER Eyal Levy, https://github.com/elevy30

VOLUME /tmp
ADD spring-boot-web1-1.0.jar app.jar
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS=""
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar" ]