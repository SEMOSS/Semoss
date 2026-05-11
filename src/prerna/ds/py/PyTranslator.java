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
package prerna.ds.py;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.ThreadContext;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;

public class PyTranslator {

	static Map<String, SemossDataType> pyS = new HashMap<String, SemossDataType>();
	static {
		pyS.put("object", SemossDataType.STRING);
		pyS.put("category", SemossDataType.STRING);
		pyS.put("int64", SemossDataType.INT);
		pyS.put("float64", SemossDataType.DOUBLE);
		pyS.put("datetime64", SemossDataType.DATE);
		pyS.put("datetime64[ns]", SemossDataType.TIMESTAMP);
	}

	public static String curEncoding = null;

	private SocketClient sc = null;
	private Insight globalStoreInsight = null;

	/**
	 * @param sc                 the socket client connected to the Python process
	 * @param globalStoreInsight the insight whose globals dict is used as the
	 *                           Python execution namespace
	 */
	public PyTranslator(SocketClient sc, Insight globalStoreInsight) {
		this.sc = sc;
		this.globalStoreInsight = globalStoreInsight;
	}

	public SocketClient getSocketClient() {
		return this.sc;
	}

	public Insight getGlobalStoreInsight() {
		return this.globalStoreInsight;
	}

	public void setSocketClient(SocketClient sc) {
		this.sc = sc;
	}

	public SemossDataType convertDataType(String pDataType) {
		return pyS.get(pDataType);
	}

	/**
	 * 
	 * @return
	 */
	public String getCurEncoding() {
		if (curEncoding == null) {
			curEncoding = (String) transportScript(null, "sys.stdout.encoding", false);
		}
		return curEncoding;
	}

	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(String script) {
		return (List<Object>) transportScript(null, script, false);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(String script) {
		List<String> val = (List<String>) transportScript(null, script, false);
		return val;
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public String[] getStringArray(String script) {
		List<String> val = getStringList(script);
		String[] retString = new String[val.size()];
		val.toArray(retString);
		return retString;
	}

	/**
	 * Get boolean from py script
	 * 
	 * @param script
	 * @return
	 */
	public boolean getBoolean(String script) {
		Boolean x = (Boolean) transportScript(null, script, false);
		return x.booleanValue();
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(String script) {
		Number x = (Number) transportScript(null, script, false);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(String script) {
		Number x = (Number) transportScript(null, script, false);
		return x.longValue();
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(String script) {
		Number x = (Number) transportScript(null, script, false);
		return x.doubleValue();
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(String script) {
		return (String) transportScript(null, script, false);
	}

	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "list(" + frameName + ".columns)";
		List<String> colNames = (List<String>) transportScript(null, script, false);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param script
	 */
	public void runEmptyPy(String... script) {
		this.transportScript(null, convertArrayToString(script), false);
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param script
	 */
	public Object runDirectPy(String... script) {
		return this.transportScript(null, convertArrayToString(script), false);
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param executionInsight If we have a User invoking an engine python process
	 *                         The engine python process has its own unique insight
	 *                         for variable encapsulation However, we need to know
	 *                         from what insight is the user invoking this request
	 *                         So that if the engine is making a call back/reactor
	 *                         request It knows which User invoked for security
	 *                         permissions
	 * @param script
	 * @return
	 */
	public Object runDirectPy(Insight executionInsight, String... script) {
		return this.transportScript(executionInsight, convertArrayToString(script), false);
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @param script
	 * @return
	 */
	public Object runScript(String... script) {
		return this.transportScript(null, convertArrayToString(script), true);
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @param executionInsight If we have a User invoking an engine python process
	 *                         The engine python process has its own unique insight
	 *                         for variable encapsulation However, we need to know
	 *                         from what insight is the user invoking this request
	 *                         So that if the engine is making a call back/reactor
	 *                         request It knows which User invoked for security
	 *                         permissions
	 * @param script
	 * @return
	 */
	public Object runScript(Insight executionInsight, String... script) {
		return this.transportScript(executionInsight, convertArrayToString(script), false);
	}

	/**
	 * Executes a Python script with explicitly supplied asset paths, bypassing the
	 * shared Insight context fields ({@code contextProjectId} /
	 * {@code contextProjectName}).
	 * <p>
	 * Use this instead of {@link #runScript} whenever the calling code already
	 * knows the target engine's assets folder and must not race with other threads
	 * that may concurrently call {@code insight.setContext()}. The supplied
	 * {@code assetsDir} is used to set {@code APP_ROOT} for the execution; all
	 * paths are forwarded to the Python process via
	 * {@code PayloadStruct.asset_paths} so that the per-project module isolation in
	 * {@code _asset_aware_import} picks them up correctly.
	 *
	 * @param executionInsight     the insight whose security context governs this
	 *                             call; may differ from the translator's
	 *                             {@code globalStoreInsight} when an engine invokes
	 *                             Python on behalf of a user
	 * @param script               the Python code to execute
	 * @param assetsDir            the engine assets root folder (becomes
	 *                             {@code APP_ROOT})
	 * @param additionalAssetsDirs extra paths appended to {@code asset_paths} (e.g.
	 *                             the {@code /py} sub-folder); may be null
	 * @return the value of the last expression evaluated, or an empty string if the
	 *         script produces no evaluable expression
	 */
	public Object runScriptWithExplicitAssetPaths(Insight executionInsight, String script, String assetsDir,
			String[] additionalAssetsDirs) {
		final String ROOT = executionInsight.getInsightFolder().replace('\\', '/');
		final String APP_ROOT = assetsDir.replace('\\', '/');
		String userRootTmp = null;
		try {
			userRootTmp = AssetUtility.getRootFolderPath(executionInsight, AssetUtility.USER_SPACE_KEY, false)
					.replace('\\', '/');
		} catch (Exception e) {
			// best effort; keep null
		}
		final String USER_ROOT = userRootTmp;

		// get runtime vars
		Map<String, Object> runtimeVars = new HashMap<>();
		runtimeVars.put("ROOT", ROOT);
		runtimeVars.put("APP_ROOT", APP_ROOT);
		if (USER_ROOT != null) {
			runtimeVars.put("USER_ROOT", USER_ROOT);
		}

		// get paths
		int numAssetsDir = 1 + (additionalAssetsDirs == null ? 0 : additionalAssetsDirs.length);
		String[] finalAssetsDir = new String[numAssetsDir];
		finalAssetsDir[0] = assetsDir.replace('\\', '/');
		if (additionalAssetsDirs != null) {
			for (int i = 0; i < additionalAssetsDirs.length; i++) {
				finalAssetsDir[i + 1] = additionalAssetsDirs[i].replace('\\', '/');
			}
		}

		// execute
		Object output = transportScriptWithExplicitPaths(executionInsight, script, finalAssetsDir, runtimeVars, false);
		// try to perform some cleanup
		if (output instanceof String) {
			String strOutput = (String) output;
			if (ROOT != null && strOutput.contains(ROOT)) {
				strOutput = strOutput.replace(ROOT, "$IF");
			}
			if (APP_ROOT != null && strOutput.contains(APP_ROOT)) {
				strOutput = strOutput.replace(APP_ROOT, "$APP_IF");
			}
			if (USER_ROOT != null && strOutput.contains(USER_ROOT)) {
				strOutput = strOutput.replace(USER_ROOT, "$USER_IF");
			}
			return strOutput;
		}
		return output;
	}

	/**
	 * Sends a Python script to the socket process and returns the result. Infers
	 * {@code asset_paths} from the global-store insight's current context project
	 * when one is set.
	 *
	 * @param executionInsight  the security-context insight; may be null
	 * @param script            the Python code to execute
	 * @param supportLegacyVars if we should support legacy usage to access ROOT,
	 *                          APP_ROOT, USER_ROOT varaibles instead of new
	 *                          <p>
	 *                          from smssutil import smss_get_runtime_var
	 *                          smss_get_runtime_var("ROOT")
	 *                          </p>
	 *                          ;
	 * @return the deserialized result from the Python process
	 */
	private Object transportScript(Insight executionInsight, String script, boolean supportLegacyVars) {
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

		// get runtime vars
		Map<String, Object> runtimeVars = new HashMap<>();
		runtimeVars.put("ROOT", ROOT);
		runtimeVars.put("APP_ROOT", APP_ROOT);
		if (USER_ROOT != null) {
			runtimeVars.put("USER_ROOT", USER_ROOT);
		}

		String[] asset_paths = null;
		if (this.globalStoreInsight.getContextProjectId() != null) {
			String assetsDir = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
					this.globalStoreInsight.getContextProjectId(), this.globalStoreInsight.getContextProjectName());
			String assetsPyDir = assetsDir + "/py";
			asset_paths = new String[] { assetsDir, assetsPyDir };
		}

		Object output = transportScriptWithExplicitPaths(executionInsight, script, asset_paths, runtimeVars,
				supportLegacyVars);
		// try to perform some cleanup
		if (output instanceof String) {
			String strOutput = (String) output;
			if (ROOT != null && strOutput.contains(ROOT)) {
				strOutput = strOutput.replace(ROOT, "$IF");
			}
			if (APP_ROOT != null && strOutput.contains(APP_ROOT)) {
				strOutput = strOutput.replace(APP_ROOT, "$APP_IF");
			}
			if (USER_ROOT != null && strOutput.contains(USER_ROOT)) {
				strOutput = strOutput.replace(USER_ROOT, "$USER_IF");
			}
			return strOutput;
		}
		return output;
	}

	/**
	 * Low-level transport that accepts an explicit {@code asset_paths} array
	 * instead of inferring it from the shared Insight context. Unlike
	 * {@link #transportScript}, this method never reads {@code contextProjectId} or
	 * {@code contextProjectName}, making it safe to call from concurrent threads
	 * targeting different engines on the same Insight.
	 *
	 * @param executionInsight   the security-context insight; may be null
	 * @param script             the Python code (already prefixed with path
	 *                           variable assignments)
	 * @param explicitAssetPaths paths forwarded to the Python process as
	 *                           {@code asset_paths}
	 * @return the deserialized result from the Python process
	 */
	private Object transportScriptWithExplicitPaths(Insight executionInsight, String script,
			String[] explicitAssetPaths, Map<String, Object> runtimeVars, boolean supportLegacyVars) {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();
		ps.payload = new Object[] { script };
		ps.payloadClasses = new Class[] { String.class };
		ps.longRunning = true;
		ps.insightId = this.globalStoreInsight.getInsightId();
		if (explicitAssetPaths != null) {
			ps.asset_paths = explicitAssetPaths;
		}
		if (runtimeVars != null) {
			ps.runtime_vars = runtimeVars;
		}
		if (supportLegacyVars) {
			ps.append_vars = runtimeVars;
		}
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (executionInsight != null) {
			ps.executionInsightId = executionInsight.getInsightId();
		}
		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
			return ps.payload[0];
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * Sends a {@code CLEAR_NON_MODULE_GLOBALS} command to the Python process,
	 * removing all user-defined variables from this insight's globals dict while
	 * preserving imported modules and framework entries.
	 */
	public void clearInsightGlobals() {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.INSIGHT;
		ps.payload = new Object[] { "CLEAR_NON_MODULE_GLOBALS" };
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * Sends a {@code REMOVE_INSIGHT_GLOBALS} command to the Python process,
	 * completely dropping the globals dict for this insight and requesting garbage
	 * collection to free all Python objects held by it.
	 */
	public void removeInsightGlobals() {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.INSIGHT;
		ps.payload = new Object[] { "REMOVE_INSIGHT_GLOBALS" };
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * 
	 * @param script
	 * @return
	 */
	private String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder();
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++) {
			if (script[lineIndex] != null) {
				retString.append(script[lineIndex]).append("\n");
			}
		}
		return retString.toString();
	}

	/**
	 * 
	 * @param executionInsight
	 * @param fileLocation
	 * @param projectId
	 * @return
	 */
	public String loadPythonModuleFromFile(Insight executionInsight, String fileLocation, String projectId) {
		return loadPythonModuleFromFile(executionInsight, fileLocation, projectId, null);
	}

	/**
	 * 
	 * @param executionInsight
	 * @param fileLocation
	 * @param space
	 * @param alias
	 * @return
	 */
	public String loadPythonModuleFromFile(Insight executionInsight, String fileLocation, String space, String alias) {
		String appFolder = null;
		if (space != null) {
			appFolder = AssetUtility.getProjectAssetsFolder(space) + "/py/";
			appFolder = appFolder.replace("\\", "/");
		}

		if (alias == null || alias.trim().isEmpty()) {
			alias = "pyModule_" + UUID.randomUUID().toString().replace("-", "");
		}

		String filePath = appFolder + fileLocation;
		try {
			if (appFolder != null) {
				String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='"
						+ filePath + "', search='" + appFolder + "')";
				runScript(executionInsight, script);
			} else {
				String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='"
						+ filePath + "', search=None)";
				runScript(executionInsight, script);
			}
		} catch (Exception e) {
			throw new SemossPixelException("Unable to load python file as module");
		}

		return alias;
	}

	/**
	 * 
	 * @param executionInsight
	 * @param moduleAlias
	 * @param functionName
	 * @param argsList
	 * @return
	 */
	public Object runFunctionFromLoadedModule(Insight executionInsight, String moduleAlias, String functionName,
			List<Object> argsList) {
		StringBuilder args = new StringBuilder();
		boolean add_comma = false;
		for (int i = 0; i < argsList.size(); i++) {
			if (add_comma) {
				args.append(", ");
			}

			args.append(PyUtils.determineStringType(argsList.get(i)));
			add_comma = true;
		}

		Object pyResponse = null;
		try {
			String commands = moduleAlias + "." + functionName + "(" + args.toString() + ")\n";
			pyResponse = runDirectPy(executionInsight, commands);
		} catch (Exception e) {
			throw new SemossPixelException("Unable to run function from module " + moduleAlias);
		}

		return pyResponse;
	}

	// DEPRECATED METHODS

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runDirectPy(String...)}
	 *             instead.
	 * @param script
	 * @param this.globalStoreInsight
	 * @return
	 */
	@Deprecated
	public Object runSmssWrapperEval(String script) {
		return this.transportScript(null, script, false);
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runScript(String...)}
	 *             instead.
	 * @param script
	 * @return
	 */
	@Deprecated
	public String runPyAndReturnOutput(String... script) {
		return this.transportScript(null, convertArrayToString(script), true) + "";
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runScript(String...)}
	 *             instead.
	 * @param script
	 * @return
	 */
	@Deprecated
	public String runSingle(String... script) {
		return this.transportScript(null, convertArrayToString(script), true) + "";
	}

}
