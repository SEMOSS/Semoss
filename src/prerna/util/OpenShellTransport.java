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

	private final String sandboxName;
	private final String workerPort;
	private int forwardedPort = -1;
	private Process forwardProcess;
	private StringBuilder forwardLog;

	private OpenShellTransport(String sandboxName, String workerPort) {
		this.sandboxName = sandboxName;
		this.workerPort = workerPort;
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
		String template = Utility.getDIHelperProperty(TEMPLATE_PROP);
		String label = propOr(LABEL_PROP, DEFAULT_LABEL);
		int readyTimeout = intPropOr(READY_TIMEOUT_PROP, DEFAULT_READY_TIMEOUT_S);

		List<String> create = new ArrayList<>(Arrays.asList(cli, "sandbox", "create", "--name", sandboxName,
				"--keep", "--detach", "--label", label));
		if (template != null && !template.trim().isEmpty()) {
			create.add("--template");
			create.add(template.trim());
		} else {
			// An explicit image is REQUIRED, not optional. The stock OpenShell base
			// image carries no python3, and if --from is omitted the sandbox is still
			// created and reaches Ready -- only the workload fails to spawn, which
			// surfaces to the caller as an unrelated null response. Fail loudly here
			// instead of silently provisioning an image that cannot run the worker.
			String image = Utility.getDIHelperProperty(IMAGE_PROP);
			if (image == null || image.trim().isEmpty()) {
				throw new IOException("no sandbox image: set " + IMAGE_PROP + " (or " + TEMPLATE_PROP
						+ "). The stock OpenShell image has no python3, so omitting this produces a sandbox"
						+ " that starts and then cannot run the worker.");
			}
			create.add("--from");
			create.add(image.trim());
		}
		// The filesystem/egress policy is applied at creation: Landlock settings are
		// locked at startup and cannot be widened later, which is the point. Omitting
		// it silently means NO filesystem policy, so require it explicitly too.
		String policy = Utility.getDIHelperProperty(POLICY_PROP);
		if (policy == null || policy.trim().isEmpty()) {
			throw new IOException("no sandbox policy: set " + POLICY_PROP
					+ ". Creating a sandbox without a filesystem policy leaves the workload unfenced, which"
					+ " is never what a deployment intends.");
		}
		create.add("--policy");
		create.add(policy.trim());
		// The worker is the sandbox's canonical main process. It must be started with
		// the insight folder it will write {insightFolder}/log.txt into.
		create.add("--");
		create.add("python3");
		create.add("/opt/semosshome/py/gaas_tcp_socket_server.py");
		create.add("--port");
		create.add(workerPort);
		create.add("--start");
		create.add("True");
		create.add("--py_folder");
		create.add("/opt/semosshome/py");
		create.add("--insight_folder");
		create.add("/opt/semosshome/insight");

		classLogger.info("Starting executor sandbox {} (worker port {})", sandboxName, workerPort);
		String createOut = run(create, readyTimeout);
		classLogger.info("Sandbox create output for {}: {}", sandboxName, createOut);

		this.forwardedPort = freeLocalPort();
		List<String> forward = Arrays.asList(cli, "forward", "service", "--target-port", workerPort, "--local",
				String.valueOf(this.forwardedPort), sandboxName);
		classLogger.info("Forwarding localhost:{} -> sandbox {}:{}", this.forwardedPort, sandboxName, workerPort);
		ProcessBuilder pb = new ProcessBuilder(forward);
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
		if (!awaitForwardReady(this.forwardedPort, readyTimeout)) {
			String detail = readAvailable(forwardProcess);
			close();
			throw new IOException("port forward for sandbox " + sandboxName + " did not become ready on 127.0.0.1:"
					+ this.forwardedPort + " within " + readyTimeout + "s" + (detail.isEmpty() ? "" : ": " + detail));
		}
		classLogger.info("Executor sandbox {} ready; worker reachable on 127.0.0.1:{}", sandboxName,
				this.forwardedPort);
		return this.forwardedPort;
	}

	/**
	 * Poll the forwarded port until something accepts a connection.
	 *
	 * <p>
	 * A TCP connect is the right probe here and deliberately not a protocol exchange:
	 * the worker is single-connection ({@code --max_count 1}), so sending anything to
	 * test readiness would consume its only slot and, on disconnect, end the worker.
	 */
	private static boolean awaitForwardReady(int port, int timeoutSeconds) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
		while (System.nanoTime() < deadline) {
			try (java.net.Socket probe = new java.net.Socket()) {
				probe.connect(new java.net.InetSocketAddress("127.0.0.1", port), 1000);
				return true;
			} catch (IOException notYet) {
				try {
					Thread.sleep(250);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
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
		try {
			run(Arrays.asList(propOr(CLI_PROP, DEFAULT_CLI), "sandbox", "delete", sandboxName), 60);
		} catch (Exception e) {
			classLogger.warn("Failed to delete executor sandbox {}", sandboxName, e);
		}
	}

	// ------------------------------------------------------------------ helpers

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
		pb.redirectErrorStream(true);
		Process p = pb.start();
		StringBuilder out = new StringBuilder();
		try (java.io.BufferedReader r = new java.io.BufferedReader(
				new java.io.InputStreamReader(p.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
			String line;
			while ((line = r.readLine()) != null) {
				out.append(line).append('\n');
			}
		}
		if (!p.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
			p.destroyForcibly();
			throw new IOException("timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
		}
		if (p.exitValue() != 0) {
			// A sandbox that already exists is the normal re-entry case, not a failure.
			String lower = out.toString().toLowerCase();
			if (!lower.contains("already exists")) {
				throw new IOException("command failed (" + p.exitValue() + "): " + String.join(" ", cmd) + "\n"
						+ out.toString().trim());
			}
		}
		return out.toString().trim();
	}
}
