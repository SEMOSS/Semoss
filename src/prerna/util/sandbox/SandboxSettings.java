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
package prerna.util.sandbox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.OpenShellTransport;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * The one list of every setting that decides how user code is sandboxed, and the one place that
 * reads {@code SANDBOX_MODE}.
 *
 * <p>
 * All of these are {@code RDF_Map.prop} keys, read live through
 * {@link Utility#getDIHelperProperty(String)}. A container environment variable only reaches one
 * when a {@code set*.sh} script in semoss-artifacts writes it into the file. Each {@link Key}
 * carries its default, the sandbox it applies to and a one-line description, so the same list
 * documents the settings and prints the effective values (see {@link #logEffectiveOnce()}).
 * Documented in {@code docs/deployment/sandbox_configuration.md}.
 */
public final class SandboxSettings {

	private static final Logger classLogger = LogManager.getLogger(SandboxSettings.class);

	/**
	 * How user code is sandboxed once {@code CHROOT_ENABLE} is true.
	 *
	 * <p>
	 * Which workers are sandboxed at all depends on who supplies the {@code SymlinkHelper}:
	 * {@code User} passes one for user-authored python, while engine-owned workers (model engines,
	 * vector databases, guardrails, local function engines) and {@code Project} pass {@code null}.
	 */
	public enum Mode {
		/** No kernel jail: fake chroot with symlinks. The default. */
		FAKECHROOT,
		/** Namespace jail ({@code py/sandbox_launcher.py}) with a control-socket injector. */
		NAMESPACE,
		/** External sandbox (OpenShell) created ahead of the worker; no in-process jail. */
		OPENSHELL
	}

	/** Which sandbox a setting applies to. */
	public enum Scope {
		/** Every jailed worker. */
		COMMON,
		FAKECHROOT,
		NAMESPACE,
		OPENSHELL,
		/** The agent CLI sandbox (Claude Code, Copilot), separate from the user-code jail. */
		AGENT
	}

	/**
	 * One setting.
	 *
	 * @param name         the {@code RDF_Map.prop} key
	 * @param defaultValue the value used when the key is unset or blank; empty means none
	 * @param scope        the sandbox it applies to
	 * @param description  one line on what it does
	 */
	public record Key(String name, String defaultValue, Scope scope, String description) {

		/** The configured value, trimmed, or null when the key is unset or blank. */
		public String configured() {
			String v = Utility.getDIHelperProperty(name);
			return v == null || v.trim().isEmpty() ? null : v.trim();
		}

		/** The configured value, or the default. */
		public String value() {
			String v = configured();
			return v == null ? defaultValue : v;
		}

		/** The effective value as a boolean. */
		public boolean asBoolean() {
			return Boolean.parseBoolean(value());
		}

		/** The effective value as an int. */
		public int asInt() {
			return Integer.parseInt(value());
		}
	}

	private static final List<Key> ALL = new ArrayList<>();

	private static Key add(String name, String defaultValue, Scope scope, String description) {
		Key key = new Key(name, defaultValue, scope, description);
		ALL.add(key);
		return key;
	}

	// ------------------------------------------------------------------ common

	public static final Key CHROOT_ENABLE = add(Constants.CHROOT_ENABLE, "false", Scope.COMMON,
			"Master switch: jail user python, the agent bash tool and project-reactor file access");
	public static final Key SANDBOX_MODE = add(Constants.SANDBOX_MODE, Mode.FAKECHROOT.name(), Scope.COMMON,
			"FAKECHROOT, NAMESPACE (alias NSJAIL) or OPENSHELL; anything else, including OFF, means FAKECHROOT");
	public static final Key CHROOT_DIR = add(Constants.CHROOT_DIR, "", Scope.COMMON,
			"Folder holding each user's chroot folder and the shared template; required when CHROOT_ENABLE is true");
	public static final Key SANDBOX_PATH = add(Constants.SANDBOX_PATH, "", Scope.COMMON,
			"Executable directories visible inside the jail, separated by the OS path separator");
	public static final Key ULIMIT_PROCESS = add(Constants.ULIMIT_PROCESS, "", Scope.COMMON,
			"Virtual memory cap in KB (ulimit -v) for workers; not used by OPENSHELL, see OPENSHELL_MEMORY");
	public static final Key INSIGHT_CACHE_DIR = add(Constants.INSIGHT_CACHE_DIR, "", Scope.COMMON,
			"Insight scratch folder, linked into every chroot");
	public static final Key PY_SERVER_USER = add(Settings.PY_SERVER_USER, "", Scope.COMMON,
			"OS user the unjailed python worker runs as (sudo -u); ignored by the jails");

	// ------------------------------------------------------------------ fakechroot

	public static final Key CHROOT_SYMLINK_PATHS = add("CHROOT_SYMLINK_PATHS", "", Scope.FAKECHROOT,
			"Comma-separated absolute host paths linked into every chroot");

	// ------------------------------------------------------------------ namespace

	public static final Key SANDBOX_IO_DIR = add(Constants.SANDBOX_IO_DIR, "${java.io.tmpdir}/semoss-sandbox",
			Scope.NAMESPACE, "Short folder for jail, io and control sockets (AF_UNIX paths are limited to ~108 bytes)");

	// ------------------------------------------------------------------ openshell

	public static final Key OPENSHELL_CLI = add(OpenShellTransport.CLI_PROP, "openshell", Scope.OPENSHELL,
			"Path to the openshell CLI");
	public static final Key OPENSHELL_CLI_HOME = add(OpenShellTransport.CLI_HOME_PROP, "", Scope.OPENSHELL,
			"HOME for CLI children, where the gateway registration and mTLS bundle live; inherited when unset");
	public static final Key OPENSHELL_TEMPLATE = add(OpenShellTransport.TEMPLATE_PROP, "", Scope.OPENSHELL,
			"Workload template to create sandboxes from, instead of an image");
	public static final Key OPENSHELL_IMAGE = add(OpenShellTransport.IMAGE_PROP,
			"quay.io/semoss/semoss-sandbox:dev-3.14-cpu", Scope.OPENSHELL,
			"Image a new sandbox starts from; ignored when a template is set");
	public static final Key OPENSHELL_POLICY = add(OpenShellTransport.POLICY_PROP, "", Scope.OPENSHELL,
			"Policy template to render instead of the bundled sandbox-policy.yaml");
	public static final Key OPENSHELL_PYTHON_BINARY = add(OpenShellTransport.PYTHON_BINARY_PROP,
			"/usr/lib/python/cpython/bin/python3.14", Scope.OPENSHELL,
			"Interpreter path the policy's egress rules name");
	public static final Key OPENSHELL_EGRESS = add(OpenShellTransport.EGRESS_PROP,
			"pypi.org:443, files.pythonhosted.org:443", Scope.OPENSHELL,
			"Hosts sandboxed code may reach, host[:port] list; none for no egress");
	public static final Key OPENSHELL_CPU = add(OpenShellTransport.CPU_PROP, "2", Scope.OPENSHELL,
			"CPU cap per sandbox as a Kubernetes quantity (2, 500m); none for no cap");
	public static final Key OPENSHELL_MEMORY = add(OpenShellTransport.MEMORY_PROP, "4Gi", Scope.OPENSHELL,
			"Memory cap per sandbox (4Gi, 512Mi); none for no cap");
	public static final Key OPENSHELL_WORKER_PORT = add(OpenShellTransport.WORKER_PORT_PROP, "7100", Scope.OPENSHELL,
			"Port the worker listens on inside the sandbox");
	public static final Key OPENSHELL_LABEL = add(OpenShellTransport.LABEL_PROP, "app=semoss-executor",
			Scope.OPENSHELL, "Label applied to every executor sandbox");
	public static final Key OPENSHELL_READY_TIMEOUT_SECONDS = add(OpenShellTransport.READY_TIMEOUT_PROP, "180",
			Scope.OPENSHELL, "Cold-start budget in seconds");
	public static final Key OPENSHELL_KEEP_SANDBOX = add(OpenShellTransport.KEEP_SANDBOX_PROP, "false",
			Scope.OPENSHELL, "true leaves sandboxes in place on teardown, for debugging (also a -D property)");
	public static final Key OPENSHELL_IDLE_EXIT_MINUTES = add(OpenShellTransport.IDLE_EXIT_PROP, "180",
			Scope.OPENSHELL, "A user's worker exits after this long with no client; keep above the session timeout");
	public static final Key OPENSHELL_WORKER_MAX_CLIENTS = add(OpenShellTransport.MAX_CLIENTS_PROP, "16",
			Scope.OPENSHELL, "Connections one user's worker serves at once");

	// ------------------------------------------------------------------ agent CLI sandbox

	public static final Key AGENT_SANDBOX_ENABLE = add("AGENT_SANDBOX_ENABLE", "false", Scope.AGENT,
			"true wraps the Claude Code and Copilot CLIs in Landlock (Linux) or sandbox-exec (macOS)");
	public static final Key AGENT_SANDBOX_DEFAULT_READS = add("AGENT_SANDBOX_DEFAULT_READS", "", Scope.AGENT,
			"Comma-separated paths the agent CLI may read; empty uses the built-in OS list");
	public static final Key AGENT_SANDBOX_LOOPBACK_NETWORK = add("AGENT_SANDBOX_LOOPBACK_NETWORK", "true",
			Scope.AGENT, "Allow the agent CLI to reach loopback");
	public static final Key AGENT_SANDBOX_BLOCK_READS = add("AGENT_SANDBOX_BLOCK_READS", "", Scope.AGENT,
			"Comma-separated paths blocked on top of the built-in block list");

	private SandboxSettings() {
		// static utility
	}

	/** Every setting, in declaration order. */
	public static List<Key> all() {
		return Collections.unmodifiableList(ALL);
	}

	// ------------------------------------------------------------------ mode

	/**
	 * The jail mode. The single parser of {@code SANDBOX_MODE}: unset or blank is
	 * {@link Mode#FAKECHROOT}, {@code OPENSHELL} is {@link Mode#OPENSHELL}, {@code NAMESPACE} and the
	 * legacy {@code NSJAIL} are {@link Mode#NAMESPACE}, and anything else falls back to
	 * {@link Mode#FAKECHROOT} rather than silently gaining a jail.
	 *
	 * @return the mode; says nothing about whether jailing is on, see {@link #chrootEnabled()}
	 */
	public static Mode mode() {
		String mode = SANDBOX_MODE.configured();
		if (mode == null) {
			return Mode.FAKECHROOT;
		}
		if (Constants.SANDBOX_MODE_OPENSHELL.equalsIgnoreCase(mode)) {
			return Mode.OPENSHELL;
		}
		if ("NAMESPACE".equalsIgnoreCase(mode) || "NSJAIL".equalsIgnoreCase(mode)) {
			return Mode.NAMESPACE;
		}
		return Mode.FAKECHROOT;
	}

	/** Whether user python is jailed at all. */
	public static boolean chrootEnabled() {
		return CHROOT_ENABLE.asBoolean();
	}

	/** Whether the node worker runs in its own namespace jail: NAMESPACE mode, not on Windows. */
	public static boolean nodeNamespaceJail() {
		return !SystemUtils.IS_OS_WINDOWS && mode() == Mode.NAMESPACE;
	}

	/** Whether user-scoped OpenShell sandboxes apply: jailing is on and the mode is OPENSHELL. */
	public static boolean openShellEnabled() {
		return chrootEnabled() && mode() == Mode.OPENSHELL;
	}

	// ------------------------------------------------------------------ logging

	private static final AtomicBoolean LOGGED = new AtomicBoolean(false);

	/**
	 * Logs the effective sandbox settings once per JVM, at first use of a jail: the mode, and for
	 * each setting that applies to it whether the value is configured or the default. A mode value
	 * that is not a jail mode is called out, because it silently means FAKECHROOT.
	 */
	public static void logEffectiveOnce() {
		if (!LOGGED.compareAndSet(false, true)) {
			return;
		}
		Mode mode = mode();
		String raw = SANDBOX_MODE.configured();
		StringBuilder sb = new StringBuilder("Sandbox settings: CHROOT_ENABLE=").append(chrootEnabled())
				.append(", mode=").append(mode);
		if (raw != null && !raw.equalsIgnoreCase(mode.name()) && !"NSJAIL".equalsIgnoreCase(raw)) {
			classLogger.warn("SANDBOX_MODE={} is not a jail mode; using {}", raw, mode);
		}
		for (Key key : ALL) {
			if (key == CHROOT_ENABLE || key == SANDBOX_MODE) {
				continue;
			}
			if (key.scope() != Scope.COMMON && key.scope().name().equals(mode.name()) == false) {
				continue;
			}
			sb.append("\n  ").append(key.name()).append('=').append(key.value().isEmpty() ? "(unset)" : key.value())
					.append(key.configured() == null ? " (default)" : "");
		}
		classLogger.info(sb.toString());
	}
}
