package harry.runner;

import harry.model.sut.InJvmSut;

public class HarryRunnerJvm extends org.apache.cassandra.distributed.test.TestBaseImpl implements HarryRunner {

    public static void main(String[] args) throws Throwable {
        HarryRunnerJvm runner = new HarryRunnerJvm();

        InJvmSut.InJvmSutConfiguration config = new InJvmSut.InJvmSutConfiguration(3,
                10,
                System.getProperty("harry.root", "/tmp/cassandra/harry/") + System.currentTimeMillis());

        runner.run(config);
    }


}
