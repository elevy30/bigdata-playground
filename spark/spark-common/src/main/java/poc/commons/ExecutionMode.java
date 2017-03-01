package poc.commons;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ExecutionMode {

	Remote("remote"),
	Local("local"),
	Mesos("mesos"),
    Yarn("yarn");

    private String execMode;
    private static final Map<String, ExecutionMode> LOOKUP = new HashMap<>();

    static {
        for (ExecutionMode executionMode : EnumSet.allOf(ExecutionMode.class)) {
            LOOKUP.put(executionMode.getExecMode(), executionMode);
        }
    }

    ExecutionMode(String execMode) {
        this.execMode = execMode;
    }

    public String getExecMode() {
        return execMode;
    }

    public static ExecutionMode getExecutionMode(String mode) {
        return LOOKUP.get(mode);
    }

}
