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
CLONE_REPO=$(if $(CASSANDRA_REPO),$(CASSANDRA_REPO),'git@github.com:apache/cassandra.git')

cassandra-submodule:
	git clone -b patched-trunk ${CLONE_REPO} cassandra

update-conf:
	cp cassandra/conf/cassandra.yaml harry-integration/test/conf/cassandra.yaml
	cp cassandra/conf/cassandra.yaml test/conf/cassandra.yaml

package: cassandra-submodule
	# Build Cassandra
	./bin/build-cassandra-submodule.sh
	# Build Harry
	mvn clean package -DskipTests

run-cassandra: package
	./cassandra/bin/cassandra -f

stress: package
	@java \
		--add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
		--add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
		--add-exports java.base/sun.nio.ch=ALL-UNNAMED \
		--add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
		--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED \
		--add-exports java.rmi/sun.rmi.server=ALL-UNNAMED \
		--add-exports java.sql/java.sql=ALL-UNNAMED \
		--add-opens java.base/java.lang.module=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.loader=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.reflect=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.math=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.module=ALL-UNNAMED \
		--add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED \
		--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
		-cp $(shell pwd)/harry-core/target/lib/*:$(shell pwd)/harry-integration/target/lib/*:$(shell pwd)/harry-integration-external/target/lib/*:$(shell pwd)/harry-core/target/*:$(shell pwd)/harry-integration/target/*:$(shell pwd)/harry-integration-external/target/* \
		-ea harry.runner.external.MiniStress $(ARGS)
