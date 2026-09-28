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

	private final String sandboxName;
	private final String workerPort;
	private final boolean keepSandbox;
	private int forwardedPort = -1;
	private Process forwardProcess;
	private StringBuilder forwardLog;

	private OpenShellTransport(String sandboxName, String workerPort) {
		this.sandboxName = sandboxName;
		this.workerPort = workerPort;
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
			return String.join(" ", createCommand(sandboxNameFor(sessionKey)));
		} catch (Exception e) {
			return "(cannot build: " + e.getMessage() + ")";
		}
	}

	private static List<String> createCommand(String sandboxName) throws IOException {
		String cli = propOr(CLI_PROP, DEFAULT_CLI);
		String template = Utility.getDIHelperProperty(TEMPLATE_PROP);
		String label = propOr(LABEL_PROP, DEFAULT_LABEL);
		String workerPort = propOr(WORKER_PORT_PROP, DEFAULT_WORKER_PORT);

		List<String> create = new ArrayList<>(Arrays.asList(cli, "sandbox", "create", "--name", sandboxName,
				"--keep", "--detach", "--label", label));
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
		create.add("--");
		// The worker's stdout/stderr MUST go to a file, not to the supervisor's
		// stdout pipe. With the pipe it came up erratically -- sometimes serving,
		// sometimes never binding at all while the sandbox still read `Ready` -- and
		// slow (~5s). Redirecting to a file made it reliable, 3 runs out of 3, and
		// ready in ~0.66s. Suspected cause is output back-pressure on that pipe.
		// The file is also the readiness signal, read via `sandbox exec` so that no
		// connection is ever made to the single-connection worker.
		create.add("sh");
		create.add("-c");
		create.add("exec python3 /opt/semosshome/py/gaas_tcp_socket_server.py" //
				+ " --port " + workerPort //
				+ " --start True" //
				+ " --py_folder /opt/semosshome/py" //
				+ " --insight_folder /opt/semosshome/insight" //
				+ " --timeout 30" //
				+ " > " + WORKER_LOG_PATH + " 2>&1");
		return create;
	}

	public static OpenShellTransport forSession(String sessionKey) {
		String workerPort = Utility.getDIHelperProperty(WORKER_PORT_PROP);
		if (workerPort == null || workerPort.trim().isEmpty()) {
			workerPort = DEFAULT_WORKER_PORT;
		}
		return new OpenShellTransport(sandboxNameFor(sessionKey), workerPort.trim());
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
		List<String> create = createCommand(sandboxName);

		classLogger.info("Starting executor sandbox {} (worker port {})", sandboxName, workerPort);
		String createOut = run(create, readyTimeout);
		classLogger.info("Sandbox create output for {}: {}", sandboxName, createOut);

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
			close();
			throw new IOException("port forward for sandbox " + sandboxName + " did not become ready on 127.0.0.1:"
					+ this.forwardedPort + " within " + readyTimeout + "s (process alive=" + alive + ")"
					+ (detail.isEmpty() ? "" : ": " + detail));
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
		// Never probe the forwarded port to test readiness: the worker is
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
	 * Stop forwarding and delete the sandbox.
	 *
	 * <p>
	 * This is the whole teardown: the gateway delete is authoritative and handles
	 * process, cgroup and network cleanup. There is no "graceful worker shutdown"
	 * verb to call first, because the worker cannot outlive its sandbox.
	 */
	public void close() {
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
