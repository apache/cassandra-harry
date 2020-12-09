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

package harry.runner.external;

import harry.core.Configuration;
import harry.model.sut.external.ExternalClusterSut;
import harry.runner.HarryRunner;

import java.io.File;

public class HarryRunnerExternal implements HarryRunner {

    public static void main(String[] args) throws Throwable {
        ExternalClusterSut.registerSubtypes();

        HarryRunner runner = new HarryRunnerExternal();
        File configFile = runner.loadConfig(args);

        Configuration configuration = Configuration.fromFile(configFile);
        runner.run(configuration);
    }
}
