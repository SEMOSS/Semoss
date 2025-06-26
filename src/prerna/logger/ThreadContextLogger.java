package prerna.logger;

import org.apache.logging.log4j.ThreadContext;

public class ThreadContextLogger {

    public static void setContext(ContextKey contextKey, String contextValue) {
        ThreadContext.put(contextKey.getContextKey(), contextValue);
    }

    public static String getContextValue(ContextKey key) {
        return ThreadContext.get(key.getContextKey());
    }

}
