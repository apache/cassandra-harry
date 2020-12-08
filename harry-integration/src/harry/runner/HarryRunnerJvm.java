package harry.runner;

import harry.core.Configuration;
import harry.model.sut.InJvmSut;

import java.io.File;

public class HarryRunnerJvm extends org.apache.cassandra.distributed.test.TestBaseImpl implements HarryRunner {

    public static void main(String[] args) throws Throwable {
        InJvmSut.registerSubtypes();

        HarryRunnerJvm runner = new HarryRunnerJvm();
        File configFile = runner.loadConfig(args);

        Configuration configuration = Configuration.fromFile(configFile);
        runner.run(configuration);
    }


}
