package prerna.logging;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.ThreadContext;

public class MDCUtils {

	/**
	 * Wrap a Runnable to propagate MDC
	 * 
	 * @param task
	 * @param contextMap
	 * @return
	 */
	public static Runnable wrapRunnableWithMDC(Runnable task, Map<String, String> contextMap) {
		return () -> {
			ThreadContext.putAll(contextMap);
			try {
				task.run();
			} finally {
				ThreadContext.clearAll();
			}
		};
	}

	/**
	 * Wrap a Callable to propagate MDC
	 * 
	 * @param <V>
	 * @param task
	 * @param contextMap
	 * @return
	 */
	public static <V> Callable<V> wrapCallableWithMDC(Callable<V> task, Map<String, String> contextMap) {
		return () -> {
			ThreadContext.putAll(contextMap);
			try {
				return task.call();
			} finally {
				ThreadContext.clearAll();
			}
		};
	}

	/**
	 * Helper to submit a Runnable to an executor with MDC propagation
	 * 
	 * @param executor
	 * @param task
	 * @param contextMap
	 * @return
	 */
	public static Future<?> submitWithMDC(ExecutorService executor, Runnable task, Map<String, String> contextMap) {
		return executor.submit(wrapRunnableWithMDC(task, contextMap));
	}
}
