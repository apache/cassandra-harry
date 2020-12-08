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

cd /opt/harry/

HARRY_DIR=/cassandra/harry/
ls -1 /mnt/harry-failures/* | xargs rm -fr
local_run=false
if [ $1 = "local_run" ]; then
   local_run=true
fi

if [ "$local_run" = true ] ; then
    echo "Running locally..."
fi

while true; do 
  java -ea \
       -Xms4g \
       -Xmx4g \
       -XX:MaxRAM=4g \
       -XX:MaxMetaspaceSize=384M \
       -XX:MetaspaceSize=128M \
       -XX:SoftRefLRUPolicyMSPerMB=0 \
       -XX:MaxDirectMemorySize=2g \
       -Dcassandra.memtable_row_overhead_computation_step=100 \
       -Djdk.attach.allowAttachSelf=true \
       -XX:+HeapDumpOnOutOfMemoryError \
       -XX:-UseBiasedLocking \
       -XX:+UseTLAB \
       -XX:+ResizeTLAB \
       -XX:+UseNUMA \
       -XX:+PerfDisableSharedMem \
       -XX:+UseConcMarkSweepGC \
       -XX:+CMSParallelRemarkEnabled \
       -XX:SurvivorRatio=8 \
       -XX:MaxTenuringThreshold=1 \
       -XX:CMSInitiatingOccupancyFraction=75 \
       -XX:+UseCMSInitiatingOccupancyOnly \
       -XX:CMSWaitDuration=10000 \
       -XX:+CMSParallelInitialMarkEnabled \
       -XX:+CMSEdenChunksRecordAlways \
       -XX:+CMSClassUnloadingEnabled \
       -XX:+UseCondCardMark \
       -XX:OnOutOfMemoryError=kill \
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
       -Dorg.apache.cassandra.test.logback.configurationFile=file:///opt/harry/test/conf/logback-dtest.xml \
       -cp /opt/harry/lib/*:/opt/harry/harry-integration-0.0.1-SNAPSHOT.jar \
       -Dharry.root=${HARRY_DIR} \
       harry.runner.HarryRunnerJvm \
       /opt/harry/example.yaml

   if [ $? -ne 0 ]; then
      if [ -e "failure.dump" ]; then
	echo "Creating failure dump..."
        FAILURES_DIR="/opt/harry/dump/"
	RUN="run-$(date +%Y%m%d%H%M%S)-${RANDOM}"
        mkdir ${FAILURES_DIR}
        mkdir ${FAILURES_DIR}cluster-state
        mv ${HARRY_DIR}* ${FAILURES_DIR}/cluster-state
        mv operation.log ${FAILURES_DIR}/
        mv failure.dump ${FAILURES_DIR}/
        mv run.yaml ${FAILURES_DIR}/

        if [ "$local_run" = true ] ; then
	    mv ${FAILURES_DIR}/* /shared/
	else
	    echo "TODO"
	fi
      fi
  fi

 if [ "$local_run" = true ] ; then
    exit 0
  else
    rm -fr ${HARRY_DIR}*
    sleep 1
  fi
done
