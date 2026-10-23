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
package prerna.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Transport to a SEMOSS python worker running inside an OpenShell sandbox.
 *
 * <p>
 * OpenShell ({@code SANDBOX_MODE=OPENSHELL}) makes the worker the canonical main
 * process of its own sandbox rather than a child process of Tomcat. That changes
 * three things and nothing else:
 *
 * <ol>
 * <li><b>Where the worker runs.</b> A sandbox, created with
 * {@code openshell sandbox create} driven from an admitted workload template.</li>
 * <li><b>How bytes reach it.</b> A local port is forwarded into the sandbox's
 * loopback over the gateway ({@code openshell forward service}). The existing
 * {@code NativePySocketClient} is unchanged: it connects to
 * {@code 127.0.0.1:<forwardedPort>} exactly as it does for the non-sandbox path.</li>
 * <li><b>What "the connection" means.</b> The worker is single-connection
 * ({@code --max_count 1}) and it is the sandbox's main process, so a client
 * disconnect ends the worker <i>and</i> the sandbox. The connection must therefore
 * be held for the session's life, and re-established by re-creating the sandbox,
 * not merely by reconnecting a socket.</li>
 * </ol>
 *
 * <p>
 * <b>What this class deliberately does not do:</b> it does not choose the image,
 * the policy, the runtime class, or the egress profile. Those are deployment
 * properties admitted by the workload template, so that neither the user nor
 * model-generated code can widen its own boundary.
 */
public final class OpenShellTransport {

	private static final Logger classLogger = LogManager.getLogger(OpenShellTransport.class);

	/** Property naming the OpenShell CLI. */
	public static final String CLI_PROP = "OPENSHELL_CLI";
	/**
	 * Explicit {@code HOME} for the CLI child process.
	 *
	 * <p>
	 * <b>This is required, and it is the subtlest thing in this class.</b> The
	 * {@code openshell} CLI resolves both its gateway registration and its mTLS client
	 * bundle from <code>$HOME/.local/state/openshell/</code>. A caller with a different
	 * (or unset) {@code HOME} therefore talks to a different, or non-existent, gateway --
	 * and the failure is quiet: the sandbox is still created, but it is not the one the
	 * caller can reach, so the worker appears to die and the caller sees an unrelated
	 * null response several layers away.
	 *
	 * <p>
	 * Measured 2026-09-28: the same argv stays {@code Ready} when launched from a shell
	 * with a full environment, and dies when launched from a process with Tomcat's much
	 * thinner one (13 variables versus 124). With {@code HOME} unset the CLI reports
	 * "Or register one with: openshell gateway add".
	 *
	 * <p>
	 * Pinned explicitly rather than inherited, so behaviour does not depend on how the
	 * JVM happened to be started.
	 */
	public static final String CLI_HOME_PROP = "OPENSHELL_CLI_HOME";
	/** Property naming the sandbox workload template to create from. */
	public static final String TEMPLATE_PROP = "OPENSHELL_TEMPLATE";
	/** Property naming the sandbox image (used when no template is set). */
	public static final String IMAGE_PROP = "OPENSHELL_IMAGE";
	/** Property naming a filesystem-policy YAML applied at creation. */
	public static final String POLICY_PROP = "OPENSHELL_POLICY";
	/** Property naming the port the worker listens on *inside* the sandbox. */
	public static final String WORKER_PORT_PROP = "OPENSHELL_WORKER_PORT";
	/** Property naming the label value used to group executor sandboxes. */
	public static final String LABEL_PROP = "OPENSHELL_LABEL";
	/** How long to wait for the sandbox to reach Ready. */
	public static final String READY_TIMEOUT_PROP = "OPENSHELL_READY_TIMEOUT_SECONDS";

	private static final String DEFAULT_CLI = "openshell";
	private static final String DEFAULT_WORKER_PORT = "7100";
	private static final String DEFAULT_LABEL = "app=semoss-executor";
	private static final int DEFAULT_READY_TIMEOUT_S = 180;

	/** Where the worker writes its own log inside the sandbox. */
	public static final String WORKER_LOG_PATH = "/opt/semosshome/insight/worker.log";
	/** The worker's own line announcing that it is serving; the readiness signal. */
	public static final String WORKER_READY_MARKER = "waiting for request";

	/**
	 * When true, {@link #close()} stops the forward but leaves the sandbox in place.
	 *
	 * <p>
	 * Exists because the default teardown deletes the sandbox, which destroys the
	 * evidence needed to diagnose a startup failure: the worker's own output and the
	 * sandbox's final phase both vanish. Set with {@code -DOPENSHELL_KEEP_SANDBOX=true}
	 * or the {@link #KEEP_SANDBOX_PROP} property while investigating.
	 */
	public static final String KEEP_SANDBOX_PROP = "OPENSHELL_KEEP_SANDBOX";

	/** Minutes a user's worker may run with no client connected before it exits itself. */
	public static final String IDLE_EXIT_PROP = "OPENSHELL_IDLE_EXIT_MINUTES";
	/** How many clients (sessions, server instances) one user's worker serves at once. */
	public static final String MAX_CLIENTS_PROP = "OPENSHELL_WORKER_MAX_CLIENTS";
	private static final int DEFAULT_IDLE_EXIT_MINUTES = 180;
	private static final int DEFAULT_MAX_CLIENTS = 16;

	/** Label naming the user a sandbox belongs to (a digest, never the user id itself). */
	public static final String OWNER_LABEL = "semoss-owner";

	private final String sandboxName;
	private final String workerPort;
	private final boolean keepSandbox;
	/**
	 * Set for a user-scoped sandbox: the worker is persistent and the sandbox carries the
	 * owner label. Null for the session-scoped form, whose worker ends with its client.
	 */
	private final String owner;
	private int forwardedPort = -1;
	private Process forwardProcess;
	private StringBuilder forwardLog;

	private OpenShellTransport(String sandboxName, String workerPort) {
		this(sandboxName, workerPort, null);
	}

	private OpenShellTransport(String sandboxName, String workerPort, String owner) {
		this.sandboxName = sandboxName;
		this.workerPort = workerPort;
		this.owner = owner;
		String keep = System.getProperty(KEEP_SANDBOX_PROP);
		if (keep == null || keep.trim().isEmpty()) {
			keep = Utility.getDIHelperProperty(KEEP_SANDBOX_PROP);
		}
		this.keepSandbox = "true".equalsIgnoreCase(keep == null ? "" : keep.trim());
	}

	/**
	 * Maximum sandbox name length the gateway accepts.
	 *
	 * <p>
	 * Measured 2026-09-28: {@code openshell sandbox create} rejects longer names
	 * with <code>Client specified an invalid argument: name exceeds maximum length
	 * (25 &gt; 19)</code>. This is stricter than the DNS-1123 limit the name is
	 * otherwise modelled on, and it is not documented at the call site, so it is
	 * pinned here with its own regression check.
	 */
	public static final int MAX_SANDBOX_NAME_LENGTH = 19;

	/** Cap for one fetch-back archive; larger trees are pulled path by path or not at all. */
	private static final long MAX_FETCH_BYTES = 256L * 1024 * 1024;
	/** Most paths one change set may name; the worker overflows to a full capture before this. */
	private static final int MAX_CHANGED_PATHS = 2000;

	/**
	 * Relative paths, per transported root, that the sandbox is known to have held:
	 * everything in its last full capture plus every path written back since. Deletions
	 * are inferred only for these, so a host file the sandbox never received --
	 * gitignored, added on the host mid-session, missed by a partial upload -- is never
	 * mistaken for one the sandbox removed.
	 */
	private final java.util.Map<String, java.util.Set<String>> seenBySandbox = new java.util.concurrent.ConcurrentHashMap<>();
	/**
	 * Size and mtime of each host file as this transport last left it (uploaded or
	 * written back). A file that no longer matches was changed on the host since, and is
	 * quarantined before a sandbox copy replaces it.
	 */
	private final java.util.Map<String, String> hostStamps = new java.util.concurrent.ConcurrentHashMap<>();

	private static final String NAME_PREFIX = "pyw-";

	/**
	 * Deterministic sandbox name for a session key.
	 *
	 * <p>
	 * Kept in Java so the name is a pure function of the session key and needs no
	 * lookup: the gateway's own object store is the registry. The digest is
	 * truncated so the whole name fits {@link #MAX_SANDBOX_NAME_LENGTH}: with a
	 * 4-character prefix that leaves 15 hex characters (60 bits), which is still
	 * far beyond any realistic collision concern for concurrently active sessions.
	 */
	public static String sandboxNameFor(String sessionKey) {
		if (sessionKey == null || sessionKey.trim().isEmpty()) {
			throw new IllegalArgumentException("sessionKey is required to address an executor sandbox");
		}
		int digestChars = MAX_SANDBOX_NAME_LENGTH - NAME_PREFIX.length();
		String digest;
		try {
			java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
			byte[] hash = md.digest(sessionKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			StringBuilder sb = new StringBuilder();
			for (int i = 0; sb.length() < digestChars; i++) {
				sb.append(String.format("%02x", hash[i]));
			}
			digest = sb.substring(0, digestChars);
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
		String name = NAME_PREFIX + digest;
		if (name.length() > MAX_SANDBOX_NAME_LENGTH) {
			throw new IllegalStateException("sandbox name exceeds the gateway limit: " + name);
		}
		return name;
	}

	/**
	 * The exact argv {@link #start()} will run. Exposed so a hand-run command can be
	 * diffed against it byte for byte: every debugging round that failed here failed
	 * because the Java-side argv differed from the hand-run one in a way that was not
	 * visible by eye.
	 */
	public static String describeCreateCommand(String sessionKey) {
		try {
			return String.join(" ", createCommand(sandboxNameFor(sessionKey), null));
		} catch (Exception e) {
			return "(cannot build: " + e.getMessage() + ")";
		}
	}

	private static List<String> createCommand(String sandboxName, String owner) throws IOException {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		String template = Utility.getDIHelperProperty(TEMPLATE_PROP);
		String label = propOr(LABEL_PROP, DEFAULT_LABEL);
		String workerPort = propOr(WORKER_PORT_PROP, DEFAULT_WORKER_PORT);

		List<String> create = new ArrayList<>(Arrays.asList(cli, "sandbox", "create", "--name", sandboxName,
				"--keep", "--detach", "--label", label));
		if (owner != null) {
			create.add("--label");
			create.add(OWNER_LABEL + "=" + owner);
		}
		// A user's worker outlives any one client: it accepts the next session or server
		// instance, and exits by itself only after a long stretch with nobody connected,
		// so a sandbox every server lost track of still ends.
		String workerMode = owner == null ? " --timeout 30"
				: " --persist --max_count " + intPropOr(MAX_CLIENTS_PROP, DEFAULT_MAX_CLIENTS) + " --timeout "
						+ intPropOr(IDLE_EXIT_PROP, DEFAULT_IDLE_EXIT_MINUTES);
		if (template != null && !template.trim().isEmpty()) {
			create.add("--template");
			create.add(template.trim());
		} else {
			String image = Utility.getDIHelperProperty(IMAGE_PROP);
			if (image == null || image.trim().isEmpty()) {
				throw new IOException("no sandbox image: set " + IMAGE_PROP + " (or " + TEMPLATE_PROP
						+ "). The stock OpenShell image has no python3, so omitting this produces a sandbox"
						+ " that starts and then cannot run the worker.");
			}
			create.add("--from");
			create.add(image.trim());
		}
		String policy = Utility.getDIHelperProperty(POLICY_PROP);
		if (policy == null || policy.trim().isEmpty()) {
			throw new IOException("no sandbox policy: set " + POLICY_PROP
					+ ". Creating a sandbox without a filesystem policy leaves the workload unfenced, which"
					+ " is never what a deployment intends.");
		}
		create.add("--policy");
		create.add(policy.trim());
		// Give sandboxed code the same interpreter the worker uses. The runtime does not
		// apply the image's ENV PATH, so without this a `Py()` block that shells out to
		// `python3` gets the bare system interpreter while the worker runs from the venv.
		create.add("--env");
		create.add("PATH=/usr/lib/python/semossvenv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
		create.add("--");
		// The worker's stdout/stderr MUST go to a file, not to the supervisor's
		// stdout pipe. With the pipe it came up erratically -- sometimes serving,
		// sometimes never binding at all while the sandbox still read `Ready` -- and
		// slow (~5s). Redirecting to a file made it reliable, 3 runs out of 3, and
		// ready in ~0.66s. Suspected cause is output back-pressure on that pipe.
		// The file is also the readiness signal, read via `sandbox exec` so that no
		// connection is ever made to the single-connection worker.
		//
		// The interpreter is resolved explicitly: OpenShell builds the environment for
		// the processes it spawns, so an image's ENV PATH is not applied and a bare
		// `python3` means whatever the runtime's PATH points at -- which is how a venv
		// image ended up running the base interpreter with none of the dependencies.
		create.add("sh");
		create.add("-c");
		create.add("P=/usr/lib/python/semossvenv/bin/python3; [ -x \"$P\" ] || P=$(command -v python3);"
				+ " exec \"$P\" /opt/semosshome/py/gaas_tcp_socket_server.py" //
				+ " --port " + workerPort //
				+ " --start True" //
				+ " --py_folder /opt/semosshome/py" //
				+ " --insight_folder /opt/semosshome/insight" //
				+ workerMode //
				+ " > " + WORKER_LOG_PATH + " 2>&1");
		return create;
	}

	public static OpenShellTransport forSession(String sessionKey) {
		return new OpenShellTransport(sandboxNameFor(sessionKey), propOr(WORKER_PORT_PROP, DEFAULT_WORKER_PORT));
	}

	/**
	 * A user-scoped sandbox: a persistent worker, labelled with its owner digest, that any
	 * session or server instance of that user can attach to.
	 */
	public static OpenShellTransport forUser(String sandboxName, String owner) {
		if (owner == null || owner.trim().isEmpty()) {
			throw new IllegalArgumentException("owner is required for a user-scoped sandbox");
		}
		return new OpenShellTransport(sandboxName, propOr(WORKER_PORT_PROP, DEFAULT_WORKER_PORT), owner);
	}

	public String getSandboxName() {
		return sandboxName;
	}

	/**
	 * Connect to a sandbox that already runs a persistent worker: forward its port and
	 * nothing else. Only valid for a user-scoped sandbox; a session-scoped worker allows
	 * one client, and a second forward would queue behind it forever.
	 *
	 * @return the local port to connect to
	 */
	public int attach() throws IOException {
		if (owner == null) {
			throw new IllegalStateException("only a user-scoped sandbox can be attached to");
		}
		int readyTimeout = intPropOr(READY_TIMEOUT_PROP, DEFAULT_READY_TIMEOUT_S);
		openForward(readyTimeout);
		if (!awaitWorkerReady(Math.min(30, readyTimeout))) {
			detach();
			throw new IOException("worker inside sandbox " + sandboxName + " is not serving");
		}
		classLogger.info("Attached to executor sandbox {}; local port {}", sandboxName, this.forwardedPort);
		return this.forwardedPort;
	}

	/**
	 * Stop this instance's forward and leave the sandbox running. The user's worker
	 * survives for their other sessions and server instances; deleting it is the
	 * registry's decision, made when the user has no live session anywhere.
	 */
	public void detach() {
		if (forwardProcess != null) {
			forwardProcess.destroy();
			try {
				if (!forwardProcess.waitFor(5, TimeUnit.SECONDS)) {
					forwardProcess.destroyForcibly();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			forwardProcess = null;
		}
	}

	/** The sandbox's phase ({@code Ready}, {@code Completed}, ...), or null when it does not exist. */
	public static String phaseOf(String sandboxName) {
		try {
			String out = run(Arrays.asList(propOr(CLI_PROP, DEFAULT_CLI), "sandbox", "get", sandboxName, "-o",
					"json"), 30);
			com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(jsonBody(out)).getAsJsonObject();
			if (json.has("sandbox") && json.get("sandbox").isJsonObject()) {
				json = json.getAsJsonObject("sandbox");
			}
			return json.has("phase") ? json.get("phase").getAsString() : null;
		} catch (Exception e) {
			// "not found" exits non-zero; anything unparseable is treated the same way,
			// which only ever leads to creating a fresh sandbox.
			return null;
		}
	}

	/** Delete a sandbox by name; best effort. */
	public static void deleteSandbox(String sandboxName) {
		try {
			run(Arrays.asList(propOr(CLI_PROP, DEFAULT_CLI), "sandbox", "delete", sandboxName), 60);
			classLogger.info("Deleted executor sandbox {}", sandboxName);
		} catch (Exception e) {
			classLogger.warn("Failed to delete executor sandbox {}", sandboxName, e);
		}
	}

	/**
	 * Every executor sandbox on the gateway (selected by {@link #LABEL_PROP}), as name,
	 * phase, creation time ({@code yyyy-MM-dd HH:mm:ss}, UTC) and owner label (or null).
	 */
	public static List<String[]> listExecutorSandboxes() throws IOException, InterruptedException {
		String out = run(Arrays.asList(propOr(CLI_PROP, DEFAULT_CLI), "sandbox", "list", "--selector",
				propOr(LABEL_PROP, DEFAULT_LABEL), "-o", "json"), 60);
		List<String[]> rows = new ArrayList<>();
		com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(jsonBody(out)).getAsJsonObject();
		if (!json.has("sandboxes")) {
			return rows;
		}
		for (com.google.gson.JsonElement element : json.getAsJsonArray("sandboxes")) {
			com.google.gson.JsonObject box = element.getAsJsonObject();
			com.google.gson.JsonObject labels = box.has("labels") && box.get("labels").isJsonObject()
					? box.getAsJsonObject("labels")
					: new com.google.gson.JsonObject();
			rows.add(new String[] { box.get("name").getAsString(),
					box.has("phase") ? box.get("phase").getAsString() : "",
					box.has("created_at") ? box.get("created_at").getAsString() : "",
					labels.has(OWNER_LABEL) ? labels.get(OWNER_LABEL).getAsString() : null });
		}
		return rows;
	}

	/** The CLI may print notices before its JSON; keep from the first brace. */
	private static String jsonBody(String out) {
		int brace = out.indexOf('{');
		return brace < 0 ? "{}" : out.substring(brace);
	}

	/**
	 * Create the sandbox if it does not exist, then forward its worker port.
	 *
	 * <p>
	 * Create is idempotent from the caller's perspective: an "already exists" result
	 * is success, not an error. A sandbox that exists but has finished (phase
	 * {@code Completed}) is <b>not</b> reusable — the worker has exited and the
	 * next request must get a fresh sandbox, so callers should treat
	 * {@link #awaitReady()} failure as "delete and re-create".
	 *
	 * @return the local port to connect to
	 */
	public int start() throws IOException, InterruptedException {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		int readyTimeout = intPropOr(READY_TIMEOUT_PROP, DEFAULT_READY_TIMEOUT_S);
		// Single source of truth: the argv used here is exactly the one
		// describeCreateCommand() prints, so a hand-run diff is meaningful.
		List<String> create = createCommand(sandboxName, owner);

		classLogger.info("Starting executor sandbox {} (worker port {})", sandboxName, workerPort);
		String createOut = run(create, readyTimeout);
		classLogger.info("Sandbox create output for {}: {}", sandboxName, createOut);
		if (createOut.toLowerCase().contains("already exists")) {
			// A sandbox with this name exists, but this transport did not create it in
			// this call. That is not a reusable state: the worker inside allows one
			// connection, and whatever forward reached it first still holds that slot,
			// so a second forward leaves the real client queued and every request
			// blocks forever. Measured 2026-10-22: a Py() that never returned and a
			// logout thread stuck in clearInsightGlobals until the sandbox was deleted
			// by hand. Deleting and recreating is the only safe interpretation --
			// a sandbox this instance is not driving belongs to a dead session.
			classLogger.warn("Sandbox {} already exists; deleting and recreating it", sandboxName);
			run(Arrays.asList(cli, "sandbox", "delete", sandboxName), 60);
			createOut = run(create, readyTimeout);
			classLogger.info("Sandbox re-create output for {}: {}", sandboxName, createOut);
			if (createOut.toLowerCase().contains("already exists")) {
				throw new IOException("sandbox " + sandboxName + " still exists after delete: " + createOut);
			}
		}

		try {
			openForward(readyTimeout);
		} catch (IOException e) {
			close();
			throw e;
		}
		// Readiness must come from the worker's OWN log, and the worker must write
		// that log to a FILE, not to the supervisor's stdout pipe.
		//
		// Two measured facts (2026-09-28):
		//  1. With stdout on the supervisor's pipe the worker came up erratically --
		//     sometimes serving, sometimes never opening the port at all while the
		//     sandbox still read `Ready`. Redirecting its stdout to a file made it
		//     reliable, 3 runs out of 3, and fast (~0.66s instead of ~5s). Output
		//     back-pressure on that pipe is the suspected cause; the file removes it.
		//  2. The supervisor's `NET:OPEN 127.0.0.1:<port>/tcp` line is a *connection*
		//     event, not a bind event -- it also marks NET:CLOSE, and it fired only
		//     because a probe connected. Waiting for it is a mistake; an earlier
		//     version of this method did exactly that and stalled for the full
		//     timeout. The worker's own "waiting for request" line is the real signal.
		//
		// Never probe the forwarded port to test readiness: a session-scoped worker is
		// single-connection (--max_count 1), so a probe consumes its only slot and its
		// disconnect ends the worker.
		if (!awaitWorkerReady(readyTimeout)) {
			String detail = readAvailable(forwardProcess);
			close();
			throw new IOException("worker inside sandbox " + sandboxName + " did not report ready within "
					+ readyTimeout + "s (forward alive=" + (forwardProcess != null && forwardProcess.isAlive())
					+ ")" + (detail.isEmpty() ? "" : ": " + detail));
		}
		classLogger.info("Executor sandbox {} ready; worker listening, local port {}", sandboxName,
				this.forwardedPort);
		return this.forwardedPort;
	}

	/** Start {@code openshell forward service} for the worker port and wait until it binds. */
	private void openForward(int readyTimeout) throws IOException {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		this.forwardedPort = freeLocalPort();
		List<String> forward = Arrays.asList(cli, "forward", "service", "--target-port", workerPort, "--local",
				String.valueOf(this.forwardedPort), sandboxName);
		classLogger.info("Forwarding localhost:{} -> sandbox {}:{}", this.forwardedPort, sandboxName, workerPort);
		ProcessBuilder pb = new ProcessBuilder(forward);
		pb.environment().putAll(cliEnv());
		pb.redirectErrorStream(true);
		this.forwardProcess = pb.start();
		this.forwardLog = new StringBuilder();
		// A child whose pipe fills up blocks forever. The forward emits periodic
		// output, so it must be drained continuously or the port never binds.
		Thread drain = new Thread(() -> {
			try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(
					forwardProcess.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
				String line;
				while ((line = r.readLine()) != null) {
					synchronized (forwardLog) {
						forwardLog.append(line).append('\n');
					}
				}
			} catch (IOException e) {
				// process exited; the readiness check will report it
			}
		}, "openshell-forward-" + sandboxName);
		drain.setDaemon(true);
		drain.start();

		// The forward binds lazily: the CLI starts, then opens the local listener.
		// Returning before that produces a connection refused at the socket client
		// and surfaces as "Received a null PayloadStruct response" -- measured
		// 2026-09-28. Poll until the port actually accepts, or fail here with a
		// clear message rather than downstream with a confusing one.
		if (!awaitForwardReady(readyTimeout)) {
			String detail = readAvailable(forwardProcess);
			boolean alive = forwardProcess != null && forwardProcess.isAlive();
			classLogger.error("Forward for sandbox {} did not become ready on 127.0.0.1:{} (alive={}): {}",
					sandboxName, this.forwardedPort, alive, detail);
			detach();
			throw new IOException("port forward for sandbox " + sandboxName + " did not become ready on 127.0.0.1:"
					+ this.forwardedPort + " within " + readyTimeout + "s (process alive=" + alive + ")"
					+ (detail.isEmpty() ? "" : ": " + detail));
		}
	}

	/**
	 * Poll until the forward is up, <b>without ever connecting to the forwarded port</b>.
	 *
	 * <p>
	 * <b>Do not replace this with a TCP connect.</b> An earlier version did exactly
	 * that and it killed every sandbox it was meant to check: the forward proxies
	 * straight into the worker, the worker is single-connection ({@code --max_count 1}),
	 * so the probe consumed the worker's only slot and its disconnect triggered
	 * {@code stop_request()} -- ending the worker and taking the sandbox to
	 * {@code Completed}. The symptom was indistinguishable from "the worker never
	 * started", which is why it took so long to find. See the wiki's
	 * "never open a second connection to the worker's port" rule; this method exists to
	 * honour it.
	 *
	 * <p>
	 * Readiness is therefore taken from the CLI's own output: {@code forward service}
	 * prints a line containing {@code "Forwarding"} once the local listener is bound.
	 */
	private boolean awaitForwardReady(int timeoutSeconds) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
		while (System.nanoTime() < deadline) {
			if (forwardProcess == null || !forwardProcess.isAlive()) {
				// Died: record why before returning, or the reason is lost with the process.
				classLogger.error("Forward process exited early (exit={}): {}",
						forwardProcess == null ? "null" : forwardProcess.exitValue(), readAvailable(forwardProcess));
				return false;
			}
			synchronized (forwardLog) {
				if (forwardLog.indexOf("Forwarding") >= 0) {
					return true;
				}
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return false;
	}

	/**
	 * Copy a host file or directory into this sandbox, preserving its absolute path.
	 *
	 * <p>
	 * Used by {@link OpenShellTransporter} so that asset and project folders the worker refers to by
	 * absolute path exist inside the sandbox. Idempotency is the caller's job
	 * ({@code SymlinkHelper.activeTransports}); this method always transfers.
	 *
	 * @param absPath host path to copy
	 * @return true when the CLI reported success
	 */
	public boolean upload(String absPath) {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		// `sandbox upload <src> <dst>` copies src INTO dst, i.e. it appends basename(src).
		// Passing dst == src therefore nests: uploading /a/b/app_root to /a/b/app_root
		// produced /a/b/app_root/app_root/version/... (observed 2026-09-28, and the same
		// shape as the earlier X.py -> /path/X.py/X.py trap). The destination must be the
		// PARENT so the tree lands at the absolute path the worker expects.
		String destination = java.nio.file.Paths.get(absPath).getParent() == null ? "/"
				: java.nio.file.Paths.get(absPath).getParent().toString();
		classLogger.info("Sandbox upload {} -> {}:{}", absPath, sandboxName, destination);
		try {
			String out = run(Arrays.asList(cli, "sandbox", "upload", sandboxName, absPath, destination), 300);
			// `sandbox upload` prints a tick with its result; treat a non-zero exit as failure,
			// which run() already raises as IOException.
			classLogger.info("Sandbox upload result for {}: {}", absPath, out.trim());
			String normalized = Utility.normalizePath(absPath);
			if (normalized.contains("/" + Constants.PROJECT_FOLDER + "/")
					|| normalized.contains("/" + Constants.ROOM_FOLDER + "/")
					|| normalized.contains("/" + Constants.USER_FOLDER + "/")) {
				recordUploadedStamps(java.nio.file.Paths.get(normalized));
			}
			return true;
		} catch (Exception e) {
			classLogger.error("Sandbox upload failed for " + absPath + ": " + e.getClass().getSimpleName() + ": "
					+ e.getMessage(), e);
			return false;
		}
	}

	/**
	 * Poll until the worker reports that it is serving, <b>without ever connecting to
	 * the forwarded port</b>.
	 *
	 * <p>
	 * The signal is the worker's own log line {@code "waiting for request"}. It is read
	 * back with {@code sandbox exec}, so no socket is opened to the worker. Measured
	 * ready at +0.66s, against ~5s when the worker's stdout went to the supervisor's
	 * pipe instead of a file.
	 */
	private boolean awaitWorkerReady(int timeoutSeconds) {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
		while (System.nanoTime() < deadline) {
			try {
				List<String> cmd = Arrays.asList(cli, "sandbox", "exec", "-n", sandboxName, "--", "cat",
						WORKER_LOG_PATH);
				String out = run(cmd, Math.min(30, timeoutSeconds));
				if (out.contains(WORKER_READY_MARKER)) {
					return true;
				}
			} catch (Exception e) {
				// Log the message AND the exception: with just "{}" and no argument the
				// cause vanished and the failure read as a bare timeout.
				classLogger.error("Could not read the worker log while waiting for it to become ready: "
						+ e.getClass().getSimpleName() + ": " + e.getMessage(), e);
			}
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return false;
	}

	private String readAvailable(Process p) {
		if (forwardLog == null) {
			return "";
		}
		synchronized (forwardLog) {
			String s = forwardLog.toString().trim();
			return s.length() > 600 ? s.substring(s.length() - 600) : s;
		}
	}

	public int getForwardedPort() {
		return forwardedPort;
	}

	/**
	 * Copy one uploaded root back out of the sandbox onto the host.
	 *
	 * <p>
	 * This is the missing half of the data plane: {@link #upload(String)} moves a tree
	 * in, and without this nothing ever moves back, so a file an agent or a
	 * {@code Py()} call writes inside the sandbox is destroyed with it.
	 *
	 * <p>
	 * {@code openshell sandbox download} cannot be used directly: it is scoped to the
	 * sandbox workspace ({@code /opt/semosshome/py}) and refuses project paths with
	 * "outside the sandbox workspace". So the tree is streamed out through
	 * {@code sandbox exec -- tar -cf -} instead, which is byte-safe and therefore also
	 * correct for images and wheels.
	 *
	 * <p>
	 * A full capture also propagates deletions, but only for paths the sandbox is known
	 * to have held (see {@link #seenBySandbox}); absence alone proves nothing, because
	 * the upload skips gitignored files and the host can gain files mid-session.
	 *
	 * @param absPath a path previously passed to {@link #upload(String)}
	 * @return number of files written back or removed
	 * @throws IOException when the capture could not be taken or applied; the caller must
	 *                     treat the root as unsynced
	 */
	public int fetchBack(String absPath) throws IOException {
		if (absPath == null || absPath.trim().isEmpty()) {
			return 0;
		}
		String baseFolder = Utility.getBaseFolder();
		String normalized = Utility.normalizePath(absPath);
		if (baseFolder == null || !normalized.startsWith(Utility.normalizePath(baseFolder))) {
			classLogger.warn("Refusing fetch-back outside the SEMOSS home: {}", absPath);
			return 0;
		}
		java.io.File source = new java.io.File(normalized);
		String parent = source.getParent();
		String base = source.getName();
		if (parent == null) {
			return 0;
		}

		java.nio.file.Path temp = java.nio.file.Files.createTempDirectory("openshell-fetchback-");
		try {
			String cli = propOr(CLI_PROP, DEFAULT_CLI);
			// .git is captured so that a commit made in the sandbox shell survives, but only
			// git's data files are ever written back -- see isWritableGitPath. Regenerable
			// caches are skipped.
			java.nio.file.Path archive = temp.resolve("capture.tar");
			Capture capture = runToFile(Arrays.asList(cli, "sandbox", "exec", "-n", sandboxName, "--", "tar", "-C",
					parent, "--ignore-failed-read", "--exclude=node_modules", "--exclude=__pycache__", "--exclude=.venv",
					"-cf", "-", base), 120, archive);
			if (capture.bytes == 0) {
				return 0;
			}
			requireSafeArchive(archive);
			java.nio.file.Path tree = java.nio.file.Files.createDirectory(temp.resolve("tree"));
			extractArchive(archive, tree);
			// tar warns about anything it could not read (a mode-000 directory, say). What
			// it skipped is missing from the capture without having been deleted, so a
			// partial capture writes files back but infers no deletions.
			boolean complete = capture.warnings.isEmpty();
			if (!complete) {
				classLogger.warn("Capture of {} is partial; not propagating deletions this time: {}", absPath,
						capture.warnings);
			}
			int changed = reconcile(tree.resolve(base), java.nio.file.Paths.get(normalized), complete);
			classLogger.info("Full capture of {}: {} bytes, {} file(s) changed on the host{}", absPath, capture.bytes,
					changed, complete ? "" : " (partial)");
			return changed;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("fetch-back of " + absPath + " interrupted", e);
		} finally {
			deleteQuietly(temp);
		}
	}

	/**
	 * Fetch back only the named paths under a root that is already transported.
	 *
	 * <p>
	 * This is the narrow form of {@link #fetchBack(String)}: the sandbox worker
	 * reports which files a block actually wrote, so the host tars a handful of
	 * paths instead of the whole tree. Cost becomes proportional to the change
	 * rather than to the size of the project.
	 *
	 * @param absRoot        absolute path of the transported root inside the sandbox
	 * @param relativePaths  paths relative to that root, as reported by the watcher
	 * @return number of files written back
	 * @throws IOException when the paths could not be captured or applied; the change set
	 *                     is then lost and the caller must schedule a full capture
	 */
	public int fetchBackPaths(String absRoot, java.util.List<String> relativePaths) throws IOException {
		if (absRoot == null || relativePaths == null || relativePaths.isEmpty()) {
			return 0;
		}
		String baseFolder = Utility.getBaseFolder();
		String normalized = Utility.normalizePath(absRoot);
		if (baseFolder == null || !normalized.startsWith(Utility.normalizePath(baseFolder))) {
			classLogger.warn("Refusing path fetch-back outside the SEMOSS home: {}", absRoot);
			return 0;
		}
		java.util.List<String> safe = new java.util.ArrayList<>();
		for (String relative : relativePaths) {
			if (relative == null || relative.isEmpty()) {
				continue;
			}
			String cleaned = relative.replace('\\', '/');
			if (!isSafeRelative(cleaned)) {
				classLogger.warn("Refusing traversal in change set: {}", relative);
				continue;
			}
			safe.add(cleaned);
		}
		if (safe.isEmpty()) {
			return 0;
		}
		if (safe.size() > MAX_CHANGED_PATHS) {
			throw new IOException(safe.size() + " changed paths under " + absRoot + " exceed the " + MAX_CHANGED_PATHS
					+ " path limit");
		}

		java.nio.file.Path temp = java.nio.file.Files.createTempDirectory("openshell-fetchback-paths-");
		try {
			String cli = propOr(CLI_PROP, DEFAULT_CLI);
			java.util.List<String> command = new java.util.ArrayList<>(Arrays.asList(cli, "sandbox", "exec", "-n",
					sandboxName, "--", "tar", "-C", normalized, "--ignore-failed-read", "--exclude=node_modules",
					"--exclude=__pycache__", "--exclude=.venv", "-cf", "-", "--"));
			command.addAll(safe);
			java.nio.file.Path archive = temp.resolve("capture.tar");
			if (runToFile(command, 120, archive).bytes == 0) {
				return 0;
			}
			requireSafeArchive(archive);
			java.nio.file.Path tree = java.nio.file.Files.createDirectory(temp.resolve("tree"));
			extractArchive(archive, tree);
			return reconcile(tree, java.nio.file.Paths.get(normalized), false);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("path fetch-back from " + absRoot + " interrupted", e);
		} finally {
			deleteQuietly(temp);
		}
	}

	/**
	 * Write files from {@code incoming} onto {@code target}. Content is the authority: a
	 * differing sandbox file is always written. The host copy is quarantined first when
	 * it changed since this transport last left it, and the write is skipped if that copy
	 * cannot be made. {@code fullTree} means {@code incoming} is a complete capture of the
	 * root: paths the sandbox held before but no longer has are then propagated as
	 * deletions.
	 */
	private int reconcile(java.nio.file.Path incoming, java.nio.file.Path target, boolean fullTree)
			throws IOException {
		if (!java.nio.file.Files.isDirectory(incoming)) {
			return 0;
		}
		java.util.Set<String> seen = seenBySandbox.computeIfAbsent(target.toString(),
				key -> java.util.concurrent.ConcurrentHashMap.newKeySet());
		java.util.Set<String> previous = fullTree ? new java.util.HashSet<>(seen) : null;
		java.util.Set<String> captured = new java.util.HashSet<>();
		java.nio.file.Path store = quarantineStore();
		int[] copied = { 0 };
		try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(incoming)) {
			walk.filter(path -> java.nio.file.Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS))
					.forEach(path -> {
						String relative = incoming.relativize(path).toString().replace('\\', '/');
						captured.add(relative);
						if (!isWritableGitPath(relative)) {
							classLogger.debug("Not writing back git control file {}", relative);
							return;
						}
						java.nio.file.Path destination = containedHostPath(target, relative);
						if (destination == null) {
							classLogger.warn("Refusing write-back of {} under {}: path leaves the root or crosses a symlink",
									relative, target);
							return;
						}
						try {
							if (java.nio.file.Files.exists(destination, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
								if (java.nio.file.Files.mismatch(path, destination) == -1L) {
									recordStamp(destination);
									return;
								}
								if (hostChangedSinceSync(destination)
										&& quarantineCopy(destination, target, store) == null) {
									classLogger.warn("Write-back of {} skipped: the host copy changed and could not be preserved",
											destination);
									return;
								}
							}
							java.nio.file.Files.createDirectories(destination.getParent());
							// Re-check now that the parents exist: creating them must not have
							// walked through a link that appeared in between.
							if (containedHostPath(target, relative) == null) {
								classLogger.warn("Refusing write-back of {} under {}: path crosses a symlink", relative,
										target);
								return;
							}
							java.nio.file.Files.copy(path, destination,
									java.nio.file.StandardCopyOption.REPLACE_EXISTING,
									java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
							recordStamp(destination);
							copied[0]++;
						} catch (IOException e) {
							classLogger.warn("Could not write back {}", destination, e);
						}
					});
		}
		if (fullTree) {
			copied[0] += removeDeletedFromHost(target, previous, captured, store);
			// The capture is now the whole truth about what the sandbox holds.
			seen.clear();
			seen.addAll(captured);
		} else {
			seen.addAll(captured);
		}
		if (copied[0] > 0) {
			classLogger.info("Fetch-back wrote {} file(s) into {}", copied[0], target);
		}
		return copied[0];
	}

	/**
	 * A full capture carries survivors, not tombstones. A path the sandbox held before and
	 * the capture no longer contains was deleted inside the sandbox: quarantine the host
	 * copy, then remove it. Host files the sandbox never held are left alone.
	 */
	private int removeDeletedFromHost(java.nio.file.Path target, java.util.Set<String> previous,
			java.util.Set<String> captured, java.nio.file.Path store) {
		int moved = 0;
		for (String relative : previous) {
			if (captured.contains(relative) || !isWritableGitPath(relative)
					|| isExcludedFromCapture(java.nio.file.Paths.get(relative))) {
				continue;
			}
			java.nio.file.Path file = containedHostPath(target, relative);
			if (file == null || !java.nio.file.Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
				continue;
			}
			if (quarantineCopy(file, target, store) == null) {
				classLogger.warn("Delete suppressed for {} because it could not be quarantined", file);
				continue;
			}
			try {
				java.nio.file.Files.deleteIfExists(file);
				hostStamps.remove(file.toString());
				pruneEmptyParents(file, target);
				moved++;
			} catch (IOException e) {
				classLogger.warn("Could not remove {} after quarantining it", file, e);
			}
		}
		if (moved > 0) {
			classLogger.info("Fetch-back moved {} deleted file(s) to {}", moved, store);
		}
		return moved;
	}

	/**
	 * Remove a host file the sandbox reported as deleted, keeping a copy under the
	 * quarantine root. Ignored unless the sandbox is known to have held the path: a file
	 * created and removed inside one block must not take an unrelated host file with it.
	 *
	 * @param root     transported root the path belongs to
	 * @param relative path relative to that root
	 * @return true when the host file was removed
	 */
	public boolean deleteFromHost(String root, String relative) {
		java.nio.file.Path rootPath = java.nio.file.Paths.get(Utility.normalizePath(root));
		java.util.Set<String> seen = seenBySandbox.get(rootPath.toString());
		if (seen == null || !seen.contains(relative)) {
			classLogger.debug("Ignoring delete of {} under {}: the sandbox never held it", relative, root);
			return false;
		}
		if (!isWritableGitPath(relative)) {
			return false;
		}
		java.nio.file.Path file = containedHostPath(rootPath, relative);
		if (file == null) {
			classLogger.warn("Refusing delete of {} under {}: path leaves the root or crosses a symlink", relative, root);
			return false;
		}
		if (!java.nio.file.Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
			seen.remove(relative);
			return false;
		}
		if (quarantineCopy(file, rootPath, quarantineStore()) == null) {
			classLogger.warn("Delete suppressed for {} because it could not be quarantined", file);
			return false;
		}
		try {
			java.nio.file.Files.deleteIfExists(file);
			hostStamps.remove(file.toString());
			seen.remove(relative);
			pruneEmptyParents(file, rootPath);
			return true;
		} catch (IOException e) {
			classLogger.warn("Could not delete {} after quarantining it", file, e);
			return false;
		}
	}

	/**
	 * Remove directories that our own deletions emptied, walking up to (but never
	 * including) the transported root. Only empty directories are removed, so a folder
	 * someone deliberately created stays where it is.
	 */
	private static void pruneEmptyParents(java.nio.file.Path file, java.nio.file.Path root) {
		java.nio.file.Path parent = file.getParent();
		while (parent != null && parent.startsWith(root) && !parent.equals(root)) {
			try (java.util.stream.Stream<java.nio.file.Path> children = java.nio.file.Files.list(parent)) {
				if (children.findAny().isPresent()) {
					return;
				}
			} catch (IOException e) {
				return;
			}
			try {
				java.nio.file.Files.deleteIfExists(parent);
			} catch (IOException e) {
				return;
			}
			parent = parent.getParent();
		}
	}

	/**
	 * Directories the root capture never contains: {@code tar} excludes them, so a file
	 * missing from the archive under one of these names was never captured rather than
	 * deleted, and must not be treated as a tombstone.
	 */
	private static boolean isExcludedFromCapture(java.nio.file.Path relative) {
		for (java.nio.file.Path part : relative) {
			String name = part.toString();
			if ("node_modules".equals(name) || "__pycache__".equals(name) || ".venv".equals(name)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Inside a {@code .git} directory only git's data is written back: objects, refs,
	 * reflogs, HEAD and the index -- what a commit made in the sandbox produces.
	 * Everything else there (config, hooks, info, alternates) is code or configuration
	 * that host-side git runs or obeys, so a sandbox able to write it could execute
	 * commands on the server the next time the platform touches the repository. A
	 * {@code .git} <i>file</i> is refused too: it redirects the repository elsewhere.
	 */
	static boolean isWritableGitPath(String relative) {
		String[] parts = relative.split("/");
		for (int i = 0; i < parts.length; i++) {
			if (!".git".equals(parts[i])) {
				continue;
			}
			if (i == parts.length - 1) {
				return false;
			}
			String child = parts[i + 1];
			if (i + 2 == parts.length) {
				return GIT_DATA_FILES.contains(child);
			}
			if ("objects".equals(child)) {
				return !"info".equals(parts[i + 2]);
			}
			return "refs".equals(child) || "logs".equals(child);
		}
		return true;
	}

	private static final java.util.Set<String> GIT_DATA_FILES = java.util.Set.of("HEAD", "ORIG_HEAD", "FETCH_HEAD",
			"MERGE_HEAD", "index", "packed-refs", "COMMIT_EDITMSG");

	/** A relative path with no absolute prefix and no {@code ..} segment. */
	static boolean isSafeRelative(String relative) {
		if (relative == null || relative.isEmpty() || relative.startsWith("/") || relative.indexOf('\0') >= 0) {
			return false;
		}
		for (String part : relative.split("/")) {
			if ("..".equals(part)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Resolve {@code relative} under {@code root} for a host-side write or delete, or null
	 * when it would leave the root. No component between the root and the file may be a
	 * symlink: a link already in a writable project -- committed to its repository, say --
	 * would otherwise redirect the write, or the delete, into another user's space.
	 */
	static java.nio.file.Path containedHostPath(java.nio.file.Path root, String relative) {
		if (!isSafeRelative(relative)) {
			return null;
		}
		java.nio.file.Path candidate = root.resolve(relative).normalize();
		if (!candidate.startsWith(root) || candidate.equals(root)) {
			return null;
		}
		java.nio.file.Path current = root;
		for (java.nio.file.Path part : root.relativize(candidate)) {
			current = current.resolve(part);
			if (java.nio.file.Files.isSymbolicLink(current)) {
				return null;
			}
			if (!java.nio.file.Files.exists(current, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
				break;
			}
		}
		return candidate;
	}

	/** Remember a host file's state as this transport left it. */
	private void recordStamp(java.nio.file.Path file) {
		String stamp = stampOf(file);
		if (stamp != null) {
			hostStamps.put(file.toString(), stamp);
		}
	}

	/** True unless the host file is exactly as this transport last left it. */
	private boolean hostChangedSinceSync(java.nio.file.Path file) {
		String known = hostStamps.get(file.toString());
		return known == null || !known.equals(stampOf(file));
	}

	private static String stampOf(java.nio.file.Path file) {
		try {
			java.nio.file.attribute.BasicFileAttributes attributes = java.nio.file.Files.readAttributes(file,
					java.nio.file.attribute.BasicFileAttributes.class, java.nio.file.LinkOption.NOFOLLOW_LINKS);
			return attributes.size() + ":" + attributes.lastModifiedTime().toMillis();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Record the state of every file in a root just uploaded, so that write-back can tell
	 * a host file nobody touched (overwrite it) from one edited on the host since
	 * (quarantine it first).
	 */
	private void recordUploadedStamps(java.nio.file.Path root) {
		if (!java.nio.file.Files.isDirectory(root, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
			if (java.nio.file.Files.isRegularFile(root, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
				recordStamp(root);
			}
			return;
		}
		try {
			java.nio.file.Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<java.nio.file.Path>() {
				@Override
				public java.nio.file.FileVisitResult preVisitDirectory(java.nio.file.Path dir,
						java.nio.file.attribute.BasicFileAttributes attrs) {
					String name = dir.getFileName() == null ? "" : dir.getFileName().toString();
					return isExcludedFromCapture(java.nio.file.Paths.get(name))
							? java.nio.file.FileVisitResult.SKIP_SUBTREE
							: java.nio.file.FileVisitResult.CONTINUE;
				}

				@Override
				public java.nio.file.FileVisitResult visitFile(java.nio.file.Path file,
						java.nio.file.attribute.BasicFileAttributes attrs) {
					if (attrs.isRegularFile()) {
						hostStamps.put(file.toString(), attrs.size() + ":" + attrs.lastModifiedTime().toMillis());
					}
					return java.nio.file.FileVisitResult.CONTINUE;
				}

				@Override
				public java.nio.file.FileVisitResult visitFileFailed(java.nio.file.Path file, IOException e) {
					return java.nio.file.FileVisitResult.CONTINUE;
				}
			});
		} catch (IOException e) {
			classLogger.debug("Could not record upload state for {}", root, e);
		}
	}

	/** Timestamped quarantine root, outside every transported root so it is never pushed. */
	private static java.nio.file.Path quarantineStore() {
		String base = Utility.getBaseFolder();
		java.nio.file.Path store = java.nio.file.Paths
				.get(base == null ? System.getProperty("java.io.tmpdir") : base, "writeback-quarantine");
		String stamp = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date());
		return store.resolve(stamp);
	}

	/** Copy a host file aside before it is overwritten or removed. Returns the copy, or null. */
	private static java.nio.file.Path quarantineCopy(java.nio.file.Path file, java.nio.file.Path root,
			java.nio.file.Path store) {
		try {
			java.nio.file.Path relative = root.relativize(file);
			java.nio.file.Path destination = store.resolve(root.getFileName().toString())
					.resolve(relative.toString());
			java.nio.file.Files.createDirectories(destination.getParent());
			java.nio.file.Files.copy(file, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
					java.nio.file.StandardCopyOption.COPY_ATTRIBUTES, java.nio.file.LinkOption.NOFOLLOW_LINKS);
			classLogger.warn("Write-back preserved the host copy of {} at {}", file, destination);
			return destination;
		} catch (Exception e) {
			classLogger.warn("Could not preserve the host copy of {}", file, e);
			return null;
		}
	}

	/**
	 * Refuse an archive whose listing cannot be read, or that names an absolute path or
	 * a {@code ..} segment. Checked per segment, so a file called {@code v1..v2.diff} is
	 * fine.
	 */
	private static void requireSafeArchive(java.nio.file.Path archive) throws IOException, InterruptedException {
		String listing = runLocal(Arrays.asList("tar", "-tf", archive.toString()), 60);
		for (String line : listing.split("\\R")) {
			String entry = line.trim();
			if (entry.isEmpty()) {
				continue;
			}
			if (entry.endsWith("/")) {
				entry = entry.substring(0, entry.length() - 1);
			}
			if (entry.isEmpty() || !isSafeRelative(entry)) {
				throw new IOException("archive contains an absolute or traversing path: " + line);
			}
		}
	}

	/**
	 * Extract, then give the owner access to everything extracted: a directory the
	 * sandbox left at mode 000 would otherwise be unreadable to the reconcile walk and
	 * undeletable afterwards.
	 */
	private static void extractArchive(java.nio.file.Path archive, java.nio.file.Path destination)
			throws IOException, InterruptedException {
		runLocal(Arrays.asList("tar", "-xf", archive.toString(), "-C", destination.toString(), "--no-same-owner"),
				120);
		makeOwnerAccessible(destination);
	}

	private static void makeOwnerAccessible(java.nio.file.Path root) {
		try {
			runLocal(Arrays.asList("chmod", "-R", "u+rwX", root.toString()), 60);
		} catch (Exception e) {
			classLogger.debug("Could not normalise permissions under {}", root, e);
		}
	}

	private static void deleteQuietly(java.nio.file.Path root) {
		if (root == null || !java.nio.file.Files.exists(root)) {
			return;
		}
		makeOwnerAccessible(root);
		try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
			walk.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
				try {
					java.nio.file.Files.deleteIfExists(path);
				} catch (IOException ignored) {
					// best effort
				}
			});
		} catch (IOException ignored) {
			// best effort
		}
	}

	/** Run a local helper (tar, chmod) and return its output; a non-zero exit is an error. */
	private static String runLocal(List<String> cmd, int timeoutSeconds) throws IOException, InterruptedException {
		ProcessBuilder builder = new ProcessBuilder(cmd);
		builder.redirectErrorStream(true);
		builder.redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
		Process process = builder.start();
		java.io.ByteArrayOutputStream collected = new java.io.ByteArrayOutputStream();
		Thread reader = new Thread(() -> {
			try (java.io.InputStream stream = process.getInputStream()) {
				stream.transferTo(collected);
			} catch (IOException ignored) {
				// the exit code carries the outcome
			}
		}, "openshell-local-" + cmd.get(0));
		reader.setDaemon(true);
		reader.start();
		if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IOException("timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
		}
		reader.join(5000);
		String output = collected.toString(java.nio.charset.StandardCharsets.UTF_8);
		if (process.exitValue() != 0) {
			throw new IOException("command failed (" + process.exitValue() + "): " + String.join(" ", cmd) + "\n"
					+ output.trim());
		}
		return output;
	}

	/**
	 * Run a CLI command and stream its raw stdout into {@code out}, stopping it as soon as
	 * the output passes {@link #MAX_FETCH_BYTES}. The stream goes to disk rather than the
	 * heap: one session's capture must never be able to exhaust the memory of the JVM
	 * every other session shares.
	 *
	 * @return bytes written, and whatever the command wrote to stderr
	 */
	private Capture runToFile(List<String> cmd, int timeoutSeconds, java.nio.file.Path out)
			throws IOException, InterruptedException {
		ProcessBuilder builder = new ProcessBuilder(cmd);
		builder.environment().putAll(cliEnv());
		builder.redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
		Process process = builder.start();
		StringBuilder errors = new StringBuilder();
		Thread errorReader = new Thread(() -> {
			try (java.io.BufferedReader reader = new java.io.BufferedReader(
					new java.io.InputStreamReader(process.getErrorStream(), java.nio.charset.StandardCharsets.UTF_8))) {
				String line;
				while ((line = reader.readLine()) != null) {
					synchronized (errors) {
						if (errors.length() < 4096) {
							errors.append(line).append('\n');
						}
					}
				}
			} catch (IOException ignored) {
				// the exit code carries the outcome
			}
		}, "openshell-fetchback-errors");
		errorReader.setDaemon(true);
		errorReader.start();
		java.util.concurrent.atomic.AtomicLong written = new java.util.concurrent.atomic.AtomicLong();
		java.util.concurrent.atomic.AtomicBoolean overCap = new java.util.concurrent.atomic.AtomicBoolean();
		Thread reader = new Thread(() -> {
			byte[] buffer = new byte[64 * 1024];
			try (java.io.InputStream stream = process.getInputStream();
					java.io.OutputStream sink = java.nio.file.Files.newOutputStream(out)) {
				int read;
				while ((read = stream.read(buffer)) > 0) {
					if (written.addAndGet(read) > MAX_FETCH_BYTES) {
						overCap.set(true);
						process.destroyForcibly();
						return;
					}
					sink.write(buffer, 0, read);
				}
			} catch (IOException ignored) {
				// the exit code carries the outcome
			}
		}, "openshell-fetchback-reader");
		reader.setDaemon(true);
		reader.start();
		if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IOException("timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
		}
		reader.join(10_000);
		errorReader.join(2000);
		String warnings;
		synchronized (errors) {
			warnings = errors.toString().trim();
		}
		if (overCap.get()) {
			throw new IOException("capture passed the " + MAX_FETCH_BYTES + " byte cap: " + String.join(" ", cmd));
		}
		if (process.exitValue() != 0) {
			throw new IOException("command failed (" + process.exitValue() + "): " + String.join(" ", cmd)
					+ (warnings.isEmpty() ? "" : "\n" + warnings));
		}
		return new Capture(written.get(), warnings);
	}

	/** Size of a streamed capture and what the command reported on stderr. */
	private static final class Capture {
		private final long bytes;
		private final String warnings;

		Capture(long bytes, String warnings) {
			this.bytes = bytes;
			this.warnings = warnings;
		}
	}

	/**
	 * Stop forwarding and delete the sandbox.
	 *
	 * <p>
	 * This is the whole teardown: the gateway delete is authoritative and handles
	 * process, cgroup and network cleanup. There is no "graceful worker shutdown"
	 * verb to call first, because the worker cannot outlive its sandbox.
	 */
	public void close() {
		detach();
		if (keepSandbox) {
			classLogger.warn("KEEP_SANDBOX is set; leaving sandbox {} in place for inspection", sandboxName);
			return;
		}
		try {
			run(Arrays.asList(propOr(CLI_PROP, DEFAULT_CLI), "sandbox", "delete", sandboxName), 60);
		} catch (Exception e) {
			classLogger.warn("Failed to delete executor sandbox {}", sandboxName, e);
		}
	}

	// ------------------------------------------------------------------ helpers

	/**
	 * Environment for CLI children: inherit the caller's, but pin {@code HOME} so the
	 * CLI always finds the same gateway registration and client certificate bundle.
	 */
	private static java.util.Map<String, String> cliEnv() {
		java.util.Map<String, String> env = new java.util.HashMap<>(System.getenv());
		String home = System.getProperty(CLI_HOME_PROP);
		if (home == null || home.trim().isEmpty()) {
			home = Utility.getDIHelperProperty(CLI_HOME_PROP);
		}
		if (home != null && !home.trim().isEmpty()) {
			env.put("HOME", home.trim());
		}
		return env;
	}

	private static String propOr(String key, String fallback) {
		String v = Utility.getDIHelperProperty(key);
		return (v == null || v.trim().isEmpty()) ? fallback : v.trim();
	}

	private static int intPropOr(String key, int fallback) {
		String v = Utility.getDIHelperProperty(key);
		if (v == null || v.trim().isEmpty()) {
			return fallback;
		}
		try {
			return Integer.parseInt(v.trim());
		} catch (NumberFormatException e) {
			return fallback;
		}
	}

	/**
	 * Ask the OS for a free port rather than picking one: the forward binds
	 * host-local, and two sessions must not collide.
	 */
	private static int freeLocalPort() throws IOException {
		try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
			return s.getLocalPort();
		}
	}

	private static String run(List<String> cmd, int timeoutSeconds) throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.environment().putAll(cliEnv());
		pb.redirectErrorStream(true);
		// Close the child's stdin immediately. Without this it inherits a pipe that
		// stays open for the JVM's lifetime and any subcommand that reads stdin --
		// `openshell sandbox exec` does -- blocks until the timeout instead of
		// running. Measured 2026-09-28: the same `sandbox exec ... cat` returned
		// instantly from a shell (stdin at EOF) and timed out after 30s under
		// Tomcat, which is what made the readiness poll fail and the pixel stall for
		// the full 180s. A shell would not have shown this.
		pb.redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
		Process p = pb.start();

		// Read on a separate thread. A blocking readLine() here hangs the CALLER --
		// and because close() also calls run(), a wedged CLI child used to deadlock the
		// failure path itself: the request thread sat in readLine() inside close() with
		// no timeout, and the sandbox was never cleaned up. Measured 2026-09-28.
		StringBuilder out = new StringBuilder();
		Thread reader = new Thread(() -> {
			try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(
					p.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
				String line;
				while ((line = r.readLine()) != null) {
					synchronized (out) {
						out.append(line).append('\n');
					}
				}
			} catch (IOException ignored) {
				// stream closed with the process; the exit code carries the outcome
			}
		}, "openshell-cli-" + cmd.get(0));
		reader.setDaemon(true);
		reader.start();

		if (!p.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
			p.destroyForcibly();
			reader.interrupt();
			throw new IOException("timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
		}
		// Give the reader a moment to drain what the process wrote before it exited.
		reader.join(2000);
		if (p.exitValue() != 0) {
			// A sandbox that already exists is the normal re-entry case, not a failure.
			String collected;
			synchronized (out) {
				collected = out.toString();
			}
			String lower = collected.toLowerCase();
			if (!lower.contains("already exists")) {
				throw new IOException("command failed (" + p.exitValue() + "): " + String.join(" ", cmd) + "\n"
						+ collected.trim());
			}
		}
		synchronized (out) {
			return out.toString().trim();
		}
	}
}
