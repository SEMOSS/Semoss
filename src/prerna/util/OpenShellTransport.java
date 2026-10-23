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

	/** Cap for one fetch-back archive; larger trees are pulled path by path or not at all. */
	private static final long MAX_FETCH_BYTES = 256L * 1024 * 1024;

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
		// Give sandboxed code the same interpreter the worker uses. The runtime does not
		// apply the image's ENV PATH, so without this a `Py()` block that shells out to
		// `python3` gets the bare system interpreter while the worker runs from the venv.
		create.add("--env");
		create.add("PATH=/opt/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
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
		create.add("P=/opt/venv/bin/python3; [ -x \"$P\" ] || P=$(command -v python3);"
				+ " exec \"$P\" /opt/semosshome/py/gaas_tcp_socket_server.py" //
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
	 * Reconciliation is per file and deliberately conservative: a file is written back
	 * only when its content differs <b>and</b> the sandbox copy is newer than the host
	 * copy. That keeps a stale or half-written sandbox from clobbering a host edit made
	 * after the upload.
	 *
	 * @param absPath a path previously passed to {@link #upload(String)}
	 * @return number of files written back
	 */
	public int fetchBack(String absPath) {
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

		java.nio.file.Path temp = null;
		try {
			String cli = propOr(CLI_PROP, DEFAULT_CLI);
			// .git is deliberately NOT excluded: a commit made in the sandbox shell is
			// work the user expects to keep, and git objects are content-addressed so
			// copying them back is safe. Only regenerable caches are skipped.
			// Known risk, not yet guarded: a host-side commit and a sandbox-side commit
			// made concurrently can leave refs diverged; the revision/conflict check that
			// would catch it is still to come.
			byte[] archive = runBytes(Arrays.asList(cli, "sandbox", "exec", "-n", sandboxName, "--", "tar", "-C", parent,
					"--exclude=node_modules", "--exclude=__pycache__", "--exclude=.venv",
					"-cf", "-", base), 120);
			if (archive == null || archive.length == 0) {
				return 0;
			}
			if (archive.length > MAX_FETCH_BYTES) {
				classLogger.warn("Fetch-back of {} is {} bytes, over the {} byte cap; skipped", absPath, archive.length,
						MAX_FETCH_BYTES);
				return 0;
			}
			if (!archiveEntriesAreSafe(archive)) {
				classLogger.warn("Fetch-back of {} contained an absolute or traversing path; skipped", absPath);
				return 0;
			}
			temp = java.nio.file.Files.createTempDirectory("openshell-fetchback-");
			extractArchive(archive, temp);
			return reconcile(temp.resolve(base), java.nio.file.Paths.get(normalized), true);
		} catch (Exception e) {
			classLogger.warn("Fetch-back failed for {}", absPath, e);
			return 0;
		} finally {
			if (temp != null) {
				deleteQuietly(temp);
			}
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
	 */
	public int fetchBackPaths(String absRoot, java.util.List<String> relativePaths) {
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
			if (cleaned.startsWith("/") || cleaned.contains("../") || cleaned.equals("..")) {
				classLogger.warn("Refusing traversal in change set: {}", relative);
				continue;
			}
			safe.add(cleaned);
			if (safe.size() >= 2000) {
				break;
			}
		}
		if (safe.isEmpty()) {
			return 0;
		}

		java.nio.file.Path temp = null;
		try {
			String cli = propOr(CLI_PROP, DEFAULT_CLI);
			java.util.List<String> command = new java.util.ArrayList<>(Arrays.asList(cli, "sandbox", "exec", "-n",
					sandboxName, "--", "tar", "-C", normalized, "--ignore-failed-read", "--exclude=node_modules",
					"--exclude=__pycache__", "--exclude=.venv", "-cf", "-"));
			command.addAll(safe);
			byte[] archive = runBytes(command, 120);
			if (archive == null || archive.length == 0) {
				return 0;
			}
			if (archive.length > MAX_FETCH_BYTES) {
				classLogger.warn("Path fetch-back from {} is {} bytes, over the cap; skipped", absRoot, archive.length);
				return 0;
			}
			if (!archiveEntriesAreSafe(archive)) {
				classLogger.warn("Path fetch-back from {} contained an absolute or traversing path; skipped", absRoot);
				return 0;
			}
			temp = java.nio.file.Files.createTempDirectory("openshell-fetchback-paths-");
			extractArchive(archive, temp);
			return reconcile(temp, java.nio.file.Paths.get(normalized), false);
		} catch (Exception e) {
			classLogger.warn("Path fetch-back failed for {}", absRoot, e);
			return 0;
		} finally {
			if (temp != null) {
				deleteQuietly(temp);
			}
		}
	}

	/**
	 * Write files from {@code incoming} onto {@code target}. Content is the authority:
	 * a differing sandbox file is always written, and mtime only decides whether the
	 * host copy must be preserved first. When {@code fullTree} is set the target is
	 * also scanned for files the sandbox no longer has, so a full fallback can
	 * propagate deletions instead of silently resurrecting them.
	 */
	private int reconcile(java.nio.file.Path incoming, java.nio.file.Path target, boolean fullTree)
			throws java.io.IOException {
		if (!java.nio.file.Files.exists(incoming)) {
			return 0;
		}
		java.nio.file.Path store = quarantineStore();
		int[] copied = { 0 };
		try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(incoming)) {
			walk.filter(java.nio.file.Files::isRegularFile).filter(path -> !java.nio.file.Files.isSymbolicLink(path))
					.forEach(path -> {
						java.nio.file.Path relative = incoming.relativize(path);
						java.nio.file.Path destination = target.resolve(relative.toString());
						try {
							if (java.nio.file.Files.exists(destination)) {
								if (sameContent(path, destination)) {
									return;
								}
								// Different bytes. A host copy newer than the sandbox copy may be a
								// concurrent edit, so keep it rather than silently overwriting it.
								if (java.nio.file.Files.getLastModifiedTime(destination)
										.compareTo(java.nio.file.Files.getLastModifiedTime(path)) > 0) {
									quarantineCopy(destination, target, store);
								}
							}
							java.nio.file.Path folder = destination.getParent();
							if (folder != null) {
								java.nio.file.Files.createDirectories(folder);
							}
							java.nio.file.Files.copy(path, destination,
									java.nio.file.StandardCopyOption.REPLACE_EXISTING,
									java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
							copied[0]++;
						} catch (java.io.IOException e) {
							classLogger.warn("Could not write back {}", destination, e);
						}
					});
		}
		if (fullTree) {
			copied[0] += quarantineStale(target, incoming, store);
		}
		if (copied[0] > 0) {
			classLogger.info("Fetch-back wrote {} file(s) into {}", copied[0], target);
		}
		return copied[0];
	}

	/**
	 * A full fetch carries survivors, not tombstones. Any host file under the root that
	 * the sandbox capture no longer contains was deleted inside the sandbox; move it to
	 * quarantine rather than leaving it to reappear in the project.
	 */
	private static int quarantineStale(java.nio.file.Path target, java.nio.file.Path incoming,
			java.nio.file.Path store) {
		int[] moved = { 0 };
		try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(target)) {
			walk.filter(java.nio.file.Files::isRegularFile).filter(path -> !java.nio.file.Files.isSymbolicLink(path))
					.filter(path -> !isExcludedFromCapture(target.relativize(path)))
					.forEach(path -> {
						java.nio.file.Path relative = target.relativize(path);
						if (java.nio.file.Files.exists(incoming.resolve(relative.toString()))) {
							return;
						}
						java.nio.file.Path copy = quarantineCopy(path, target, store);
						if (copy != null) {
							try {
								java.nio.file.Files.deleteIfExists(path);
								pruneEmptyParents(path, target);
								moved[0]++;
							} catch (java.io.IOException e) {
								classLogger.warn("Could not remove {} after quarantining it", path, e);
							}
						}
					});
		} catch (java.io.IOException e) {
			classLogger.warn("Could not scan {} for deletions", target, e);
		}
		if (moved[0] > 0) {
			classLogger.info("Fetch-back moved {} deleted file(s) to {}", moved[0], store);
		}
		return moved[0];
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
			} catch (java.io.IOException e) {
				return;
			}
			try {
				java.nio.file.Files.deleteIfExists(parent);
			} catch (java.io.IOException e) {
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
					java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
			classLogger.warn("Write-back preserved the host copy of {} at {}", file, destination);
			return destination;
		} catch (Exception e) {
			classLogger.warn("Could not preserve the host copy of {}", file, e);
			return null;
		}
	}

	/**
	 * Remove a host file the sandbox deleted, keeping a copy under the quarantine root.
	 *
	 * @param path absolute host path reported as deleted
	 * @param root transported root the path belongs to
	 */
	public static void quarantineAndDelete(String path, String root) {
		java.nio.file.Path file = java.nio.file.Paths.get(path);
		if (!java.nio.file.Files.exists(file)) {
			return;
		}
		if (quarantineCopy(file, java.nio.file.Paths.get(Utility.normalizePath(root)), quarantineStore()) == null) {
			classLogger.warn("Delete suppressed for {} because it could not be quarantined", path);
			return;
		}
		try {
			java.nio.file.Files.deleteIfExists(file);
		} catch (java.io.IOException e) {
			classLogger.warn("Could not delete {} after quarantining it", path, e);
		}
	}

	private static boolean sameContent(java.nio.file.Path left, java.nio.file.Path right) throws java.io.IOException {
		if (java.nio.file.Files.size(left) != java.nio.file.Files.size(right)) {
			return false;
		}
		return java.util.Arrays.equals(java.nio.file.Files.readAllBytes(left), java.nio.file.Files.readAllBytes(right));
	}

	private static boolean archiveEntriesAreSafe(byte[] archive) {
		try {
			Process process = new ProcessBuilder("tar", "-tf", "-")
					.redirectErrorStream(true).start();
			Thread writer = new Thread(() -> {
				try (java.io.OutputStream stdin = process.getOutputStream()) {
					stdin.write(archive);
				} catch (java.io.IOException ignored) {
					// process may exit early; the exit code carries the outcome
				}
			});
			writer.start();
			String listing = new String(process.getInputStream().readAllBytes(),
					java.nio.charset.StandardCharsets.UTF_8);
			writer.join(5000);
			process.waitFor(20, TimeUnit.SECONDS);
			for (String line : listing.split("\\R")) {
				String entry = line.trim();
				if (entry.isEmpty()) {
					continue;
				}
				if (entry.startsWith("/") || entry.contains("..")) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static void extractArchive(byte[] archive, java.nio.file.Path destination) throws Exception {
		Process process = new ProcessBuilder("tar", "-xf", "-", "-C", destination.toString())
				.redirectErrorStream(true).start();
		Thread writer = new Thread(() -> {
			try (java.io.OutputStream stdin = process.getOutputStream()) {
				stdin.write(archive);
			} catch (java.io.IOException ignored) {
				// see above
			}
		});
		writer.start();
		String output = new String(process.getInputStream().readAllBytes(),
				java.nio.charset.StandardCharsets.UTF_8);
		writer.join(10_000);
		if (!process.waitFor(60, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new java.io.IOException("tar extraction timed out");
		}
		if (process.exitValue() != 0) {
			throw new java.io.IOException("tar extraction failed: " + output.trim());
		}
	}

	private static void deleteQuietly(java.nio.file.Path root) {
		try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
			walk.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
				try {
					java.nio.file.Files.deleteIfExists(path);
				} catch (java.io.IOException ignored) {
					// best effort
				}
			});
		} catch (java.io.IOException ignored) {
			// best effort
		}
	}

	/**
	 * Run a CLI command and return its raw stdout. {@link #run(List, int)} decodes to
	 * String and would corrupt any binary payload, which is exactly what a tar stream is.
	 */
	private byte[] runBytes(List<String> cmd, int timeoutSeconds) throws IOException, InterruptedException {
		ProcessBuilder builder = new ProcessBuilder(cmd);
		builder.environment().putAll(cliEnv());
		builder.redirectErrorStream(false);
		builder.redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
		Process process = builder.start();
		java.io.ByteArrayOutputStream collected = new java.io.ByteArrayOutputStream();
		Thread reader = new Thread(() -> {
			try (java.io.InputStream stream = process.getInputStream()) {
				stream.transferTo(collected);
			} catch (java.io.IOException ignored) {
				// the exit code carries the outcome
			}
		}, "openshell-fetchback-reader");
		reader.setDaemon(true);
		reader.start();
		Thread errors = new Thread(() -> {
			try (java.io.InputStream stream = process.getErrorStream()) {
				stream.transferTo(java.io.OutputStream.nullOutputStream());
			} catch (java.io.IOException ignored) {
				// discarded
			}
		}, "openshell-fetchback-errors");
		errors.setDaemon(true);
		errors.start();
		if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			throw new IOException("timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
		}
		reader.join(5000);
		if (process.exitValue() != 0) {
			throw new IOException("command failed (" + process.exitValue() + "): " + String.join(" ", cmd));
		}
		return collected.toByteArray();
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
