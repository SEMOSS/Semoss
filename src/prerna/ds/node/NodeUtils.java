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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * Configuration helpers for the agent-only Node.js execution environment.
 *
 * <p>Unlike python (user-facing, gated by {@code USE_PYTHON}), the node worker
 * is reachable only through the agent harness platform tools. It is OFF by
 * default and requires both {@code AGENT_DEFAULT_TOOLS_ENABLE_NODE=true} and a
 * resolvable {@code NODE_HOME} in RDF_Map.prop. There is deliberately no Pixel
 * reactor and no REST endpoint for it.
 */
public class NodeUtils {

	private static final Logger classLogger = LogManager.getLogger(NodeUtils.class);

	public static final String AGENT_DEFAULT_TOOLS_ENABLE_NODE = "AGENT_DEFAULT_TOOLS_ENABLE_NODE";

	// cached package summary for the tool description
	private static String cachedPackageSummary = null;
	private static long cachedPackageJsonModified = -1;

	private NodeUtils() {
	}

	/**
	 * Whether the agent node code-execution tool should be exposed. Requires the
	 * explicit opt-in property, respects the blanket {@code DISABLE_TERMINAL}
	 * kill switch, and verifies the node executable actually resolves.
	 *
	 * @return true when the ExecuteNodeCode agent tool should be registered
	 */
	public static boolean isNodeToolEnabled() {
		String disableTerminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disableTerminal != null && Boolean.parseBoolean(disableTerminal.trim())) {
			return false;
		}
		String enabled = Utility.getDIHelperProperty(AGENT_DEFAULT_TOOLS_ENABLE_NODE);
		if (enabled == null || !Boolean.parseBoolean(enabled.trim())) {
			return false;
		}
		String nodeExe = getNodeExecutableOrNull();
		if (nodeExe == null || !new File(nodeExe).exists()) {
			classLogger.warn("{} is true but NODE_HOME does not resolve to a node executable",
					AGENT_DEFAULT_TOOLS_ENABLE_NODE);
			return false;
		}
		return true;
	}

	/**
	 * Resolve the node executable path, checking the {@code NODE_HOME} env var
	 * then {@code NODE_HOME} in the RDF_Map. The OS-specific binary
	 * ({@code /bin/node} or {@code node.exe}) is appended, mirroring
	 * {@code PYTHONHOME} resolution.
	 *
	 * @return the absolute path to the node executable
	 * @throws NullPointerException if no node home is configured
	 */
	public static String getNodeExecutable() {
		String node = getNodeExecutableOrNull();
		if (node == null) {
			throw new NullPointerException("Must define node home");
		}
		return node;
	}

	/**
	 * Same as {@link #getNodeExecutable()} but returns null when unconfigured.
	 *
	 * @return the absolute path to the node executable, or null
	 */
	public static String getNodeExecutableOrNull() {
		String node = System.getenv(Settings.NODE_HOME);
		if (node == null) {
			node = Utility.getDIHelperProperty(Settings.NODE_HOME);
		}
		if (node == null || node.trim().isEmpty()) {
			return null;
		}
		node = node.trim();
		if (SystemUtils.IS_OS_WINDOWS) {
			node = node + "/node.exe";
		} else {
			node = node + "/bin/node";
		}
		return node.replace("\\", "/");
	}

	/**
	 * @return the folder holding the node worker code
	 *         ({@code <BaseFolder>/js})
	 */
	public static String getJsBaseFolder() {
		return Utility.getBaseFolder().replace("\\", "/") + "/" + Constants.JS_BASE_FOLDER;
	}

	/**
	 * Resolve the curated node package environment folder: the
	 * {@code NODE_ENV_DIR} property when set, otherwise
	 * {@code <BaseFolder>/js/node_env}.
	 *
	 * @return the node_env folder path
	 */
	public static String getNodeEnvDir() {
		String dir = Utility.getDIHelperProperty(Settings.NODE_ENV_DIR);
		if (dir != null && !dir.trim().isEmpty()) {
			return dir.trim().replace("\\", "/");
		}
		return getJsBaseFolder() + "/node_env";
	}

	/**
	 * Human-readable summary of the curated packages ("lodash@^4.17.21, ...")
	 * read from the node_env package.json, for embedding in the agent tool
	 * description so the model knows exactly what it may require(). Cached by
	 * file modification time.
	 *
	 * @return the package summary, or a fallback note when unreadable
	 */
	public static synchronized String describeCuratedPackages() {
		File packageJson = new File(Utility.normalizePath(getNodeEnvDir() + "/package.json"));
		if (!packageJson.isFile()) {
			return "no curated packages found (node_env/package.json missing)";
		}
		long modified = packageJson.lastModified();
		if (cachedPackageSummary != null && modified == cachedPackageJsonModified) {
			return cachedPackageSummary;
		}
		try {
			String content = new String(Files.readAllBytes(packageJson.toPath()), StandardCharsets.UTF_8);
			JSONObject json = new JSONObject(content);
			JSONObject deps = json.optJSONObject("dependencies");
			if (deps == null || deps.isEmpty()) {
				cachedPackageSummary = "no curated packages configured";
			} else {
				List<String> entries = new ArrayList<>();
				for (String name : deps.keySet()) {
					entries.add(name + "@" + deps.optString(name));
				}
				entries.sort(String::compareTo);
				cachedPackageSummary = String.join(", ", entries);
			}
			cachedPackageJsonModified = modified;
			return cachedPackageSummary;
		} catch (Exception e) {
			classLogger.warn("Unable to read node_env package.json for the tool description", e);
			return "curated package list unavailable";
		}
	}
}
