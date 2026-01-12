/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
