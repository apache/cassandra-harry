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

package harry.model.sut.injvm;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Versions;

/**
 * Make sure to set -Dcassandra.test.dtest_jar_path when using this class
 */
public class MixedVersionInJvmSut extends InJvmSutBase<IUpgradeableInstance, ICluster<IUpgradeableInstance>>
{
    private static final Logger logger = LoggerFactory.getLogger(MixedVersionInJvmSut.class);
    private final Versions.Version initialVersion;
    private final List<Versions.Version> versions;

    public MixedVersionInJvmSut(ICluster<IUpgradeableInstance> cluster, Versions.Version initialVersion, List<Versions.Version> versions)
    {
        super(cluster, 10);
        this.initialVersion = initialVersion;
        this.versions = versions;
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