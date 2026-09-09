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
import java.util.concurrent.TimeUnit;

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

	// ------------------------------------------------------------------
	// node_env install self-heal

	private static final Object NODE_ENV_INSTALL_LOCK = new Object();
	private static final long NPM_CI_TIMEOUT_MINUTES = 10;
	private static final int NPM_OUTPUT_TAIL_CHARS = 4000;

	/**
	 * Whether every dependency declared in the node_env package.json is
	 * actually installed (has a package.json under node_modules).
	 * node_modules is gitignored, so a branch switch or an external cleanup
	 * can gut the installed tree while the manifest still advertises the
	 * packages to the ExecuteNodeCode tool description.
	 *
	 * @return true when nothing is declared or every declared package resolves
	 */
	public static boolean isNodeEnvInstalled() {
		File envDir = new File(Utility.normalizePath(getNodeEnvDir()));
		File packageJson = new File(envDir, "package.json");
		if (!packageJson.isFile()) {
			// no curated environment configured - nothing to verify
			return true;
		}
		JSONObject deps;
		try {
			String content = new String(Files.readAllBytes(packageJson.toPath()), StandardCharsets.UTF_8);
			deps = new JSONObject(content).optJSONObject("dependencies");
		} catch (Exception e) {
			classLogger.warn("Unable to read node_env package.json while verifying the install", e);
			return false;
		}
		if (deps == null || deps.isEmpty()) {
			return true;
		}
		for (String name : deps.keySet()) {
			if (!new File(envDir, "node_modules/" + name + "/package.json").isFile()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Ensure the curated node_env packages are installed, running
	 * {@code npm ci --omit=dev} when the installed tree is missing or
	 * incomplete. Safe to call on every boot: a healthy environment returns
	 * immediately without touching npm. Never throws - a failed install just
	 * leaves ExecuteNodeCode degraded (require() of curated packages fails)
	 * until fixed manually.
	 */
	public static void ensureNodeEnvInstalled() {
		if (!isNodeToolEnabled()) {
			return;
		}
		synchronized (NODE_ENV_INSTALL_LOCK) {
			if (isNodeEnvInstalled()) {
				classLogger.debug("node_env packages verified installed");
				return;
			}
			File envDir = new File(Utility.normalizePath(getNodeEnvDir()));
			if (!new File(envDir, "package-lock.json").isFile()) {
				classLogger.error("node_env at {} is missing package-lock.json so npm ci cannot run - "
						+ "restore the lockfile (it is git-tracked) and restart", envDir);
				return;
			}
			String npm = getNpmExecutableOrNull();
			if (npm == null || !new File(npm).exists()) {
				classLogger.error("node_env at {} needs an install but no npm executable was found under NODE_HOME",
						envDir);
				return;
			}
			long start = System.currentTimeMillis();
			try {
				File logFile = File.createTempFile("node-env-npm-ci", ".log");
				classLogger.warn("node_env at {} is missing installed packages - running npm ci --omit=dev "
						+ "(output: {})", envDir, logFile);
				ProcessBuilder pb = new ProcessBuilder(npm, "ci", "--omit=dev", "--no-audit", "--no-fund");
				pb.directory(envDir);
				pb.redirectErrorStream(true);
				pb.redirectOutput(ProcessBuilder.Redirect.to(logFile));
				// npm resolves node through its env shebang - make sure our NODE_HOME wins the PATH
				File nodeExe = new File(getNodeExecutable());
				String path = pb.environment().get("PATH");
				pb.environment().put("PATH",
						nodeExe.getParent() + File.pathSeparator + (path == null ? "" : path));
				Process p = pb.start();
				if (!p.waitFor(NPM_CI_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
					p.destroyForcibly();
					classLogger.error("npm ci in {} timed out after {} minutes - see {}", envDir,
							NPM_CI_TIMEOUT_MINUTES, logFile);
					return;
				}
				long elapsed = System.currentTimeMillis() - start;
				if (p.exitValue() != 0) {
					classLogger.error("npm ci in {} failed with exit code {} after {} ms - output tail:\n{}",
							envDir, p.exitValue(), elapsed, readTail(logFile));
					return;
				}
				if (isNodeEnvInstalled()) {
					classLogger.info("node_env restored via npm ci in {} ms", elapsed);
					logFile.delete();
				} else {
					classLogger.warn("npm ci in {} completed but the install is still incomplete - output tail:\n{}",
							envDir, readTail(logFile));
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				classLogger.error("Interrupted while running npm ci in {}", envDir, e);
			} catch (Exception e) {
				classLogger.error("Failed to run npm ci in {}", envDir, e);
			}
		}
	}

	/**
	 * Resolve the npm executable that ships alongside NODE_HOME, mirroring
	 * {@link #getNodeExecutableOrNull()}.
	 *
	 * @return the absolute path to npm, or null when NODE_HOME is unset
	 */
	public static String getNpmExecutableOrNull() {
		String home = System.getenv(Settings.NODE_HOME);
		if (home == null) {
			home = Utility.getDIHelperProperty(Settings.NODE_HOME);
		}
		if (home == null || home.trim().isEmpty()) {
			return null;
		}
		home = home.trim();
		String npm;
		if (SystemUtils.IS_OS_WINDOWS) {
			npm = home + "/npm.cmd";
		} else {
			npm = home + "/bin/npm";
		}
		return npm.replace("\\", "/");
	}

	private static String readTail(File logFile) {
		try {
			String content = new String(Files.readAllBytes(logFile.toPath()), StandardCharsets.UTF_8);
			if (content.length() > NPM_OUTPUT_TAIL_CHARS) {
				content = content.substring(content.length() - NPM_OUTPUT_TAIL_CHARS);
			}
			return content;
		} catch (Exception e) {
			return "(unable to read " + logFile + ": " + e.getMessage() + ")";
		}
	}
}
