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
package prerna.ds.node;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.ThreadContext;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;

/**
 * Thin transport for the agent node worker, modeled on
 * {@link prerna.ds.py.PyTranslator}. Sends JavaScript source to the worker
 * over the shared gaas socket protocol ({@code OPERATION.NODE}) and returns
 * the worker's response payload: a map of {@code result} (the value of the
 * last expression, REPL semantics) and {@code stdout} (captured console
 * output).
 */
public class NodeTranslator {

	// runtime var carrying the per-execution timeout to the worker
	public static final String NODE_TIMEOUT_MS = "NODE_TIMEOUT_MS";

	private SocketClient sc;
	private Insight globalStoreInsight;

	/**
	 * @param sc                 the socket client connected to the node worker
	 * @param globalStoreInsight the insight whose id keys the worker-side
	 *                           execution context (state persists per insight)
	 */
	public NodeTranslator(SocketClient sc, Insight globalStoreInsight) {
		this.sc = sc;
		this.globalStoreInsight = globalStoreInsight;
	}

	public SocketClient getSocketClient() {
		return this.sc;
	}

	public Insight getGlobalStoreInsight() {
		return this.globalStoreInsight;
	}

	/**
	 * Execute JavaScript in the node worker and return the response payload.
	 * Pushes the standard ROOT / APP_ROOT / USER_ROOT runtime vars so agent
	 * code can locate its working folders, and scrubs those host paths back
	 * out of the string outputs.
	 *
	 * @param executionInsight the security-context insight; may be null
	 * @param script           the JavaScript source to execute
	 * @param timeoutMs        per-execution timeout in ms; the worker enforces
	 *                         its own cap on top of this
	 * @return the worker response - a map with {@code result} and
	 *         {@code stdout} keys
	 */
	public Object runScript(Insight executionInsight, String script, long timeoutMs) {
		final String ROOT = this.globalStoreInsight.getInsightFolder().replace('\\', '/');
		final String APP_ROOT = this.globalStoreInsight.getContextProjectId() != null ? EngineUtility
				.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
						this.globalStoreInsight.getContextProjectId(), this.globalStoreInsight.getContextProjectName())
				.replace('\\', '/') : null;
		String userRootTmp = null;
		try {
			if (this.globalStoreInsight.getUser() != null) {
				userRootTmp = AssetUtility
						.getRootFolderPath(this.globalStoreInsight, AssetUtility.USER_SPACE_KEY, false)
						.replace('\\', '/');
			}
		} catch (Exception e) {
			// best effort; keep null
		}
		final String USER_ROOT = userRootTmp;

		Map<String, Object> runtimeVars = new HashMap<>();
		runtimeVars.put("ROOT", ROOT);
		if (APP_ROOT != null) {
			runtimeVars.put("APP_ROOT", APP_ROOT);
		}
		if (USER_ROOT != null) {
			runtimeVars.put("USER_ROOT", USER_ROOT);
		}
		if (timeoutMs > 0) {
			runtimeVars.put(NODE_TIMEOUT_MS, timeoutMs);
		}

		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.NODE;
		ps.methodName = "runScript";
		ps.payload = new Object[] { script };
		ps.payloadClasses = new Class[] { String.class };
		ps.longRunning = true;
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.runtime_vars = runtimeVars;
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (executionInsight != null) {
			ps.executionInsightId = executionInsight.getInsightId();
		}

		if (!sc.isConnected()) {
			throw new SemossPixelException(
					"The node execution engine is no longer available. Please retry - a new worker will be started.");
		}
		ps = (PayloadStruct) sc.executeCommand(ps);
		if (ps == null) {
			throw new SemossPixelException("Received a null response from the node worker");
		}
		if (ps.ex != null) {
			throw new SemossPixelException(scrubPaths(ps.ex, ROOT, APP_ROOT, USER_ROOT));
		}
		Object output = ps.payload != null && ps.payload.length > 0 ? ps.payload[0] : null;
		return scrubOutput(output, ROOT, APP_ROOT, USER_ROOT);
	}

	/**
	 * Replace the host folder paths with the same placeholder tokens the python
	 * translator uses, so absolute server paths never reach the model or the
	 * browser.
	 */
	private Object scrubOutput(Object output, String root, String appRoot, String userRoot) {
		if (output instanceof String) {
			return scrubPaths((String) output, root, appRoot, userRoot);
		}
		if (output instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) output;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				if (entry.getValue() instanceof String) {
					entry.setValue(scrubPaths((String) entry.getValue(), root, appRoot, userRoot));
				}
			}
			return map;
		}
		return output;
	}

	private String scrubPaths(String value, String root, String appRoot, String userRoot) {
		String scrubbed = value;
		if (root != null && scrubbed.contains(root)) {
			scrubbed = scrubbed.replace(root, "$IF");
		}
		if (appRoot != null && scrubbed.contains(appRoot)) {
			scrubbed = scrubbed.replace(appRoot, "$APP_IF");
		}
		if (userRoot != null && scrubbed.contains(userRoot)) {
			scrubbed = scrubbed.replace(userRoot, "$USER_IF");
		}
		return scrubbed;
	}
}
