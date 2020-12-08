#!/bin/sh

java -Dlogback.configurationFile=test/conf/logback-dtest.xml -jar harry-integration-external/target/harry-integration-external-0.0.1-SNAPSHOT.jar conf/external.yaml
