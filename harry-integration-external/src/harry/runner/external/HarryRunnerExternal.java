package harry.runner.external;

import harry.core.Configuration;
import harry.model.sut.external.ExternalClusterSut;
import harry.runner.HarryRunner;

import java.io.File;
import java.io.FileNotFoundException;

public class HarryRunnerExternal implements HarryRunner {

    public static void main(String[] args) throws Throwable {

        ExternalClusterSut.registerSubtype();

        if (args.length == 0) {
            throw new RuntimeException("Harry config YAML not provided.");
        }

        File configFile =  new File(args[0]);
        if (!configFile.exists()) {
            throw new FileNotFoundException(configFile.getAbsolutePath());
        }
        if (!configFile.canRead()) {
            throw new RuntimeException("Cannot read config file, check your permissions on " + configFile.getAbsolutePath());
        }

        Configuration config = Configuration.fromFile(configFile);

        HarryRunner runner = new HarryRunnerExternal();
        runner.run(config.system_under_test);
    }
}
