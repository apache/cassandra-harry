#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

CASSANDRA_SHA = 39ded1844f
DOCKER_REPO   = org.apache.cassandra/harry/harry-runner

cassandra:
	git clone https://github.com/apache/cassandra.git

cassandra-jar: cassandra
	cp bin/build-shaded-dtest-for-harry.sh ./cassandra/
	cp bin/relocate-dependencies-harry.pom ./cassandra/
	cd cassandra; \
		git checkout ${CASSANDRA_SHA}; \
		./build-shaded-dtest-for-harry.sh

package: cassandra-jar
	rm -fr shared/*
	mvn clean && mvn package -DskipTests

img: package
	docker build -t ${DOCKER_REPO}:latest-local ./ -f docker/Dockerfile.local

run: img
	docker run -v `pwd`/shared:/shared -it ${DOCKER_REPO}:latest-local

run-last:
	docker run -v `pwd`/shared:/shared -it ${DOCKER_REPO}:latest-local

standalone:
	rm -fr shared/*
	mvn clean && mvn package -DskipTests -P standalone

stress: package
	@java -cp $(shell pwd)/harry-core/target/lib/*:$(shell pwd)/harry-integration/target/lib/*:$(shell pwd)/harry-integration-external/target/lib/*:$(shell pwd)/harry-core/target/*:$(shell pwd)/harry-integration/target/*:$(shell pwd)/harry-integration-external/target/* -ea harry.runner.external.MiniStress $(ARGS) 
