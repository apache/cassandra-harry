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
