#!/usr/bin/env bash
mvn package && java -jar target/spring-boot-web1-1.0-SNAPSHOT.jar
mvn spring-boot:run