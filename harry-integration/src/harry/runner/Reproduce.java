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

package harry.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.InJvmSut;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class Reproduce extends TestBaseImpl
{

    private static final Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    public void runWithInJvmDtest() throws Throwable
    {
        InJvmSut.init();

        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("relocated.shaded.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");

        Configuration configuration = Configuration.fromFile("shared/run.yaml");

        Runner runner = configuration.createRunner();
        Run run = runner.getRun();

        try
        {
//            run.validator.validatePartition(0L);
        }
        catch(Throwable t)
        {
            logger.error(t.getMessage(), t);
        }
    }

    public static void main(String[] args) throws Throwable {
        try
        {
            new Reproduce().runWithInJvmDtest();
        }
        catch (Throwable t)
        {
            logger.error("Error: ", t);
        }
    }
}

