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

import java.io.File;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.PrintlnSut;
import harry.operations.Query;
import harry.reconciler.Reconciler;

public class Reproduce
{

    public static void main(String[] args) throws Throwable
    {
        File configFile = HarryRunner.loadConfig(args);
        Configuration configuration = Configuration.fromFile(configFile);
        System.out.println(Configuration.toYamlString(configuration));
        configuration = configuration.unbuild().setSUT(PrintlnSut::new).build();

        Run run = configuration.createRun();

        Reconciler reconciler = new Reconciler(run);
        long pd = 8135884698435133227L;
        System.out.println(reconciler.inflatePartitionState(pd,
                                                            5908L,
                                                            Query.selectPartition(run.schemaSpec,
                                                                                  pd,
                                                                                  false))
                                     .toString(run.schemaSpec));

        // Try out everything you might want to try with a given run
    }
}
