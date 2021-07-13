#!/bin/sh

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
       -Dorg.apache.cassandra.test.logback.configurationFile=file://test/conf/logback-dtest.xml \
       -cp harry-integration/target/harry-integration-0.0.1-SNAPSHOT.jar:$(find harry-integration/target/dependency/*.jar | tr -s '\n' ':'). \
       harry.runner.HarryRunnerJvm \
       conf/default.yaml
