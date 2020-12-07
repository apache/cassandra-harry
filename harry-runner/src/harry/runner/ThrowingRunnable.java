package harry.runner;

public interface ThrowingRunnable {
    void run() throws Throwable;

    static Runnable toRunnable(ThrowingRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable var2) {
                throw new RuntimeException(var2);
            }
        };
    }
}