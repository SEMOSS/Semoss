package prerna.reactor.interceptor;

/**
 * A utility class to hold the constant keys for the keyValue map
 * passed to pipeline reactors.
 */
public final class PipelineReactorUtils {

    public static final String ENGINE = "engine";
    public static final String METHOD_NAME = "methodName";
    public static final String ARGUMENTS = "arguments";
    public static final String RESULT = "result";
    public static final String INTERIM_RESULT = "interim_result";
    public static final String CONFIG = "config";
    public static final String TARGET_PARAM = "target_param";
    public static final String INTERCEPTOR = "interceptor";
    public static final String PASS = "pass";
    
    
    private PipelineReactorUtils() {
        // private constructor to prevent instantiation
    }
}
