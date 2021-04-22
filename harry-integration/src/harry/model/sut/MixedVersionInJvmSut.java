/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package harry.model.sut;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Versions;

/**
 * Make sure to set -Dcassandra.test.dtest_jar_path when using this class
 */
public class MixedVersionInJvmSut extends InJvmSutBase<IUpgradeableInstance, UpgradeableCluster>
{
    public static void init()
    {
        Configuration.registerSubtypes(MixedInJvmSutConfiguration.class);
    }

    private static final Logger logger = LoggerFactory.getLogger(MixedVersionInJvmSut.class);
    private final Versions.Version initialVersion;
    private final List<Versions.Version> versions;

    public MixedVersionInJvmSut(UpgradeableCluster cluster, Versions.Version initialVersion, List<Versions.Version> versions)
    {
        super(cluster, 10);
        this.initialVersion = initialVersion;
        this.versions = versions;
    }

    @JsonTypeName("mixed_in_jvm")
    public static class MixedInJvmSutConfiguration extends InJvmSutBaseConfiguration<IUpgradeableInstance, UpgradeableCluster>
    {
        public final String initial_version;
        public final List<String> versions;

        private final Versions.Version initialVersion;
        private final List<Versions.Version> upgradeVersions;

        @JsonCreator
        public MixedInJvmSutConfiguration(@JsonProperty(value = "nodes", defaultValue = "3") int nodes,
                                          @JsonProperty(value = "worker_threads", defaultValue = "10") int worker_threads,
                                          @JsonProperty(value = "initial_version") String initial_version,
                                          @JsonProperty(value = "versions") List<String> versions,
                                          @JsonProperty("root") String root)
        {
            super(nodes, worker_threads, root);

            this.initial_version = initial_version;
            this.versions = versions;
            Versions allVersions = Versions.find();

            this.initialVersion = allVersions.get(initial_version);
            this.upgradeVersions = new ArrayList<>();
            for (String version : versions)
                upgradeVersions.add(allVersions.get(version));
        }

        protected UpgradeableCluster cluster(Consumer<IInstanceConfig> cfg, int nodes, File root)
        {
            try
            {
                return UpgradeableCluster.build()
                                         .withConfig(cfg)
                                         .withNodes(nodes)
                                         .withRoot(root)
                                         .withVersion(initialVersion)
                                         .createWithoutStarting();
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        protected InJvmSutBase<IUpgradeableInstance, UpgradeableCluster> sut(UpgradeableCluster cluster)
        {
            return new MixedVersionInJvmSut(cluster, initialVersion, upgradeVersions);
        }
    }

    @Override
    public void afterSchemaInit()
    {
        for (int i = 1; i <= cluster.size(); i++)
        {
            Versions.Version v = versions.get(i - 1);
            if (!v.equals(initialVersion))
            {
                logger.info("Upgrading {} node from {} to {}", i, initialVersion, v);
                IUpgradeableInstance instance = cluster.get(i);
                try
                {
                    instance.shutdown().get();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
                instance.setVersion(v);
                instance.startup();
            }
            else
            {
                logger.info("Skipping {} node upgrade, since it is already at the required version ({})", i, initialVersion);
            }
        }
    }
}