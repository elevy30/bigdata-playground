package poc.hadoop.common;

public enum FileAlreadyExistsPolicy {
    OVERRIDE_IF_EXISTS(true),
    FAIL_IF_EXISTS(false),
    IGNORE_IF_EXISTS(true),
    SILENTLY_ABORT(false),
    APPEND_IF_EXISTS(true);

    private final boolean shouldContinue;

    FileAlreadyExistsPolicy(boolean shouldContinue) {
        this.shouldContinue = shouldContinue;
    }

    public boolean shouldContinueIfFileExists() {
        return shouldContinue;
    }
}
