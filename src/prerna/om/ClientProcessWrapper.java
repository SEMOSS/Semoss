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
package prerna.om;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.google.gson.GsonBuilder;

import prerna.tcp.client.NativePySocketClient;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PortAllocator;
import prerna.util.SandboxInjector;
import prerna.util.Settings;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

public class ClientProcessWrapper {

	private static final Logger classLogger = LogManager.getLogger(ClientProcessWrapper.class);
	private static final Logger pyLogger = LogManager.getLogger(Constants.PY_LOGGER_NAME);

	// After spawning a python server process, poll briefly to catch an immediate
	// crash (import / syntax errors surface within a few ms) without blocking long
	// on the common healthy case. ~150ms total (5 x 30ms).
	private static final int PROCESS_CRASH_POLL_ATTEMPTS = 5;
	private static final int PROCESS_CRASH_POLL_INTERVAL_MS = 30;

	// After the socket client thread is started, the create thread waits for the
	// client to report ready (connected + handshake done). It re-checks every
	// INTERVAL_MS (woken early via notify when the client flips ready/killAll) and
	// gives up after TIMEOUT_MS so a python process that never comes up surfaces a
	// clear error instead of blocking the caller forever.
	private static final long SOCKET_CLIENT_READY_WAIT_INTERVAL_MS = 1_000L;
	private static final long SOCKET_CLIENT_READY_WAIT_TIMEOUT_MS = 60_000L;

	private final Object lockCreate = new Object();
	private final Object lockDestroy = new Object();

	private SocketClient socketClient;
	private Process process;
	private String prefix;
	private int port;
	private String venvPath;
	private String serverDirectory;

	private String udsPath;
	private String controlSocketPath;
	// Namespace sandbox folders to remove on shutdown(true). The io-dir is
	// jail-visible for worker.sock; the control-dir is host-only for control.sock.
	private String sandboxIoDir;
	private String sandboxJailDir;
	private String sandboxControlDir;

	private boolean nativePyServer;
	private SymlinkHelper chrootSymlinkHelper;
	private String classPath;
	private boolean debug;
	private String timeout;
	private String loggerLevel;

	private Map<String, String> threadLoggerCtx;

	/**
	 * Convenience overload of
	 * {@link #createProcessAndClient(boolean, SymlinkHelper, int, String, String, String, boolean, String, String, Map)}
	 * that uses an empty thread-logger context.
	 *
	 * @param nativePyServer      true to launch the native Python TCP server
	 *                            (gaas_tcp_socket_server.py); false to launch the
	 *                            Java {@code SocketServer}
	 * @param chrootSymlinkHelper chroot/sandbox helper for isolated per-user
	 *                            processes; null for a non-isolated process
	 * @param port                port to connect on; negative to auto-allocate a
	 *                            free port
	 * @param venvPath            path to the Python venv executable, or null to use
	 *                            the configured base Python
	 * @param serverDirectory     working/scratch directory for the server process
	 * @param classPath           classpath override for the Java server, or null
	 *                            for the default
	 * @param debug               true to attach to an already-running server on the
	 *                            given port instead of spawning one
	 * @param timeout             idle timeout in minutes (null/"-1" for none)
	 * @param loggerLevel         log level for the spawned process (e.g. INFO)
	 * @throws Exception if the process cannot be started or the socket client never
	 *                   becomes ready
	 */
	public void createProcessAndClient(boolean nativePyServer, SymlinkHelper chrootSymlinkHelper, int port,
			String venvPath, String serverDirectory, String classPath, boolean debug, String timeout,
			String loggerLevel) throws Exception {
		this.createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath,
				debug, timeout, loggerLevel, new HashMap<>());
	}

	/**
	 * Spawn the analytics server process (native Python or Java) and connect a
	 * socket client to it, blocking until the client reports ready or a timeout is
	 * hit. Selects the launch path based on {@code nativePyServer} and the
	 * chroot/sandbox mode of {@code chrootSymlinkHelper}. When {@code debug} is
	 * true and a positive port is supplied, it attaches to an already-running
	 * server instead of spawning one.
	 *
	 * @param nativePyServer      true to launch the native Python TCP server
	 *                            (gaas_tcp_socket_server.py); false to launch the
	 *                            Java {@code SocketServer}
	 * @param chrootSymlinkHelper chroot/sandbox helper for isolated per-user
	 *                            processes; null for a non-isolated process
	 * @param port                port to connect on; negative to auto-allocate a
	 *                            free port
	 * @param venvPath            path to the Python venv executable, or null to use
	 *                            the configured base Python
	 * @param serverDirectory     working/scratch directory for the server process
	 * @param classPath           classpath override for the Java server, or null
	 *                            for the default
	 * @param debug               true to attach to an already-running server on the
	 *                            given port instead of spawning one
	 * @param timeout             idle timeout in minutes (null/"-1" for none)
	 * @param loggerLevel         log level for the spawned process (e.g. INFO)
	 * @param threadLoggerCtx     log4j MDC context to propagate onto the socket
	 *                            client thread
	 * @throws Exception if the process cannot be started or the socket client never
	 *                   becomes ready
	 */
	public void createProcessAndClient(boolean nativePyServer, SymlinkHelper chrootSymlinkHelper, int port,
			String venvPath, String serverDirectory, String classPath, boolean debug, String timeout,
			String loggerLevel, Map<String, String> threadLoggerCtx) throws Exception {
		synchronized (lockCreate) {
			this.nativePyServer = nativePyServer;
			this.chrootSymlinkHelper = chrootSymlinkHelper;
			this.classPath = classPath;
			this.port = calculatePort(port);
			this.venvPath = venvPath;
			this.serverDirectory = serverDirectory;
			this.debug = debug;
			this.loggerLevel = loggerLevel;
			this.timeout = timeout;
			if (this.timeout == null) {
				this.timeout = "-1";
			}
			boolean serverRunning = debug && port > 0;
			if (!serverRunning) {
				if (nativePyServer) {
					if (this.chrootSymlinkHelper != null && this.chrootSymlinkHelper.isInjectMode()) {
						String insightCache = Utility.getDIHelperProperty(prerna.util.Constants.INSIGHT_CACHE_DIR);
						Path serverDirectoryPath = Files.createTempDirectory(Paths.get(insightCache), "a");
						this.serverDirectory = serverDirectoryPath.toString();
						ClientProcessWrapper.writeLogConfigurationFile(this.serverDirectory);

						// Use the user's chroot folder name as the source identity; the
						// sandbox launcher shortens it to keep AF_UNIX socket paths valid.
						String ioDirName = Paths.get(this.chrootSymlinkHelper.getUserChrootFolder()).getFileName()
								.toString();

						Object[] ret = ClientProcessWrapper.startTCPServerNativePySandbox(this.serverDirectory,
								this.port + "", this.timeout, this.loggerLevel, ioDirName);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
						this.udsPath = (String) ret[2];
						this.controlSocketPath = (String) ret[3];
						this.sandboxIoDir = (String) ret[4];
						this.sandboxJailDir = (String) ret[5];
						this.sandboxControlDir = (String) ret[6];

						SandboxInjector injector = new SandboxInjector(this.controlSocketPath);
						if (!injector.awaitReady(SOCKET_CLIENT_READY_WAIT_TIMEOUT_MS)) {
							throw new IllegalStateException("Timed out waiting for namespace sandbox control socket");
						}
						this.chrootSymlinkHelper.setInjector(injector);
					} else if (this.chrootSymlinkHelper != null) {
						// for a user process - this will be something like /opt/user_id_randomid/
						Path chrootPath = Paths.get(this.chrootSymlinkHelper.getUserChrootFolder());
						// we will be creating a fake semoss home in the chrooted directory
						// so grabbing the current base folder to mock the same pattern
						String baseFolderPath = Utility.getBaseFolder();
						// this is the fake semoss home in the chroot
						Path chrootBaseFolderPath = Paths.get(chrootPath + baseFolderPath);
						// create a temp folder where we will start the process for the server
						Path serverDirectoryPath = Files.createTempDirectory(chrootBaseFolderPath, "a");
						this.serverDirectory = serverDirectoryPath.toString();
						// we need to have a relative path to replace the log4j file as it is started
						// in the chroot world and not the base OS world
						// .. technically since i'm hard coding above the base folder from rdf_map,
						// could replace but w/e
						String relative = chrootPath.relativize(serverDirectoryPath).toString();
						if (!relative.startsWith("/")) {
							relative = "/" + relative;
						}
						ClientProcessWrapper.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);

						Object[] ret = ClientProcessWrapper.startTCPServerNativePyChroot(
								this.chrootSymlinkHelper.getUserChrootFolder(), relative, this.port + "", this.timeout,
								this.loggerLevel);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
					} else {
						// write the log4j file in the server directory
						ClientProcessWrapper.writeLogConfigurationFile(this.serverDirectory);

						Object[] ret = ClientProcessWrapper.startTCPServerNativePy(this.serverDirectory, this.port + "",
								this.venvPath, this.timeout, this.loggerLevel);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
					}
				} else {
					if (chrootSymlinkHelper != null) {
						// for a user process - this will be something like /opt/user_id_randomid/
						Path chrootPath = Paths.get(chrootSymlinkHelper.getUserChrootFolder());
						// we will be creating a fake semoss home in the chrooted directory
						// so grabbing the current base folder to mock the same pattern
						String baseFolderPath = Utility.getBaseFolder();
						// this is the fake semoss home in the chroot
						Path chrootBaseFolderPath = Paths.get(chrootPath + baseFolderPath);
						// create a temp folder where we will start the process for the server
						Path serverDirectoryPath = Files.createTempDirectory(chrootBaseFolderPath, "a");
						this.serverDirectory = serverDirectoryPath.toString();
						// we need to have a relative path to replace the log4j file as it is started
						// in the chroot world and not the base OS world
						// .. technically since i'm hard coding above the base folder from rdf_map,
						// could replace but w/e
						String relative = chrootPath.relativize(serverDirectoryPath).toString();
						if (!relative.startsWith("/")) {
							relative = "/" + relative;
						}
						ClientProcessWrapper.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);

						this.process = ClientProcessWrapper.startTCPServerChroot(classPath,
								this.chrootSymlinkHelper.getUserChrootFolder(), relative, this.port + "");
					} else {
						// write the log4j file in the server directory
						ClientProcessWrapper.writeLogConfigurationFile(this.serverDirectory);
						this.process = ClientProcessWrapper.startTCPServer(classPath, this.serverDirectory,
								this.port + "");
					}
				}
			}

			try {
				if (this.nativePyServer) {
					this.socketClient = new NativePySocketClient(threadLoggerCtx);
				} else {
					this.socketClient = new SocketClient(threadLoggerCtx);
				}
				this.socketClient.setCpw(this);
				if (this.udsPath != null) {
					this.socketClient.connectUds(this.udsPath);
				} else {
					this.socketClient.connect("127.0.0.1", this.port, false);
				}
				Thread t = new Thread(socketClient);
				t.start();
				long waitDeadline = System.currentTimeMillis() + SOCKET_CLIENT_READY_WAIT_TIMEOUT_MS;
				synchronized (socketClient) {
					while (!socketClient.isReady() && !socketClient.isKillAll()) {
						long remainingWaitMillis = waitDeadline - System.currentTimeMillis();
						if (remainingWaitMillis <= 0) {
							throw new IllegalArgumentException(
									"Timed out waiting for isolated analytics engine socket client to become ready");
						}
						try {
							socketClient.wait(Math.min(SOCKET_CLIENT_READY_WAIT_INTERVAL_MS, remainingWaitMillis));
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							classLogger.error("Interrupted while waiting for socket client readiness", e);
							throw new IllegalStateException("Interrupted while waiting for socket client readiness", e);
						}
					}
				}
				if (socketClient.isKillAll()) {
					throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
				}
				classLogger.info("Setting the socket client ");
			} catch (Exception e) {
				if (debug) {
					throw new IllegalArgumentException("Could not connect to process - note force port is on " + port
							+ " and your server might not be started");
				}
				classLogger.error("Failed to initialize socket client for isolated analytics engine on port {}",
						this.port, e);
				throw e;
			}
		}
	}

	/**
	 * Stop the server (best-effort, bounded to 15s) and destroy the process. The
	 * server stop runs on a virtual thread so a hung process cannot block the
	 * caller indefinitely. If the port is still held by the OS afterwards it is
	 * reset to -1 so the next start auto-allocates a fresh one.
	 *
	 * @param cleanUpFolder true to also recursively delete the server scratch
	 *                      directory and any namespace-sandbox folders
	 */
	public void shutdown(boolean cleanUpFolder) {
		synchronized (lockDestroy) {
			if (this.socketClient != null && this.socketClient.isConnected()) {
				try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

					Callable<Boolean> callableTask = () -> {
						boolean result = false;
						if (cleanUpFolder) {
							this.socketClient.stopServer();
							classLogger.info("Sucessfully stopped the process");
							// remove the insight scratch dir plus namespace sandbox dirs
							// (no-ops when those are null)
							result = deleteFolderWithRetries(this.serverDirectory)
									& deleteFolderWithRetries(this.sandboxIoDir)
									& deleteFolderWithRetries(this.sandboxJailDir)
									& deleteFolderWithRetries(this.sandboxControlDir);
						} else {
							this.socketClient.stopServer();
							classLogger.info("Sucessfully stopped the process");
							result = true;
						}
						return result;
					};

					Future<Boolean> future = executor.submit(callableTask);
					try {
						// dont have the user wait forever...
						Boolean result = future.get(15, TimeUnit.SECONDS);
						if (!result) {
							classLogger.warn("Failed to shutdown the process");
						}
					} catch (TimeoutException e) {
						classLogger.warn("Task did not finish within the timeout");
						future.cancel(true);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						classLogger.error("Interrupted while waiting for socket client shutdown task to finish", e);
					} catch (ExecutionException e) {
						classLogger.error("Socket client shutdown task failed", e);
					} finally {
						// reset the venv path
						this.venvPath = null;
					}
				}
			}
			// you know what, always try this...
			if (this.process != null) {
				try {
					this.process.destroy();
				} catch (Exception e) {
					classLogger.error("Failed to destroy isolated analytics process for port {}", this.port, e);
				}
			}
		}
		if (this.port > 0) {
			if (!PortAllocator.isPortAvailable(this.port)) {
				classLogger.warn("Port is still in use by OS {}", this.port);
				classLogger.warn("Setting port to -1 for new assignment");
				this.port = -1;
			}
		}
	}

	/**
	 * Best-effort recursive delete of a folder, retrying a few times to ride out
	 * transient locks while the process finishes exiting.
	 *
	 * @param folder absolute path to remove; null/empty is treated as success
	 * @return true if the folder is gone (or never existed)
	 */
	private boolean deleteFolderWithRetries(String folder) {
		if (folder == null || folder.trim().isEmpty()) {
			return true;
		}
		File dir = new File(folder);
		int attempt = 0;
		while (attempt < 3) {
			try {
				if (dir.exists()) {
					FileUtils.deleteDirectory(dir);
					classLogger.info("Successfully cleaned up the directory {}", folder);
				} else {
					classLogger.info("Directory does not exist {}", folder);
				}
				return true;
			} catch (Exception ignored) {
				attempt++;
				classLogger.info("Failed attempt #{} to delete the folder {}", attempt, folder);
				try {
					Thread.sleep(attempt * 1000L);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					classLogger.error("Interrupted while waiting between cleanup retries for {}", folder, e1);
					return false;
				}
			}
		}
		return false;
	}

	/**
	 * Re-run {@link #createProcessAndClient} with the same settings used on the
	 * last start, reusing the existing venv path. Used to revive a dropped
	 * connection.
	 *
	 * @throws Exception if the process cannot be restarted or the socket client
	 *                   never becomes ready
	 */
	public void reconnect() throws Exception {
		createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath, debug,
				timeout, loggerLevel, threadLoggerCtx);
	}

	/**
	 * Re-run {@link #createProcessAndClient} with the same settings as the last
	 * start, but resolving the Python executable from the given venv engine.
	 *
	 * @param venvEngineId id of the venv engine whose Python executable to use, or
	 *                     null to use the configured base Python
	 * @throws Exception if the process cannot be restarted or the socket client
	 *                   never becomes ready
	 */
	public void reconnect(String venvEngineId) throws Exception {
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath, debug,
				timeout, loggerLevel, threadLoggerCtx);
	}

	/**
	 * Resolve the port to use: a negative value auto-allocates the next free port,
	 * otherwise the supplied port is returned unchanged.
	 *
	 * @param port requested port, or negative to auto-allocate
	 * @return the resolved port
	 */
	private int calculatePort(int port) {
		if (port < 0) {
			port = PortAllocator.getInstance().getNextAvailablePort();
		}

		return port;
	}

	/**
	 * @return the socket client connected to the server process, or null if not yet
	 *         created
	 */
	public SocketClient getSocketClient() {
		return socketClient;
	}

	/**
	 * @param socketClient the socket client to associate with this wrapper
	 */
	public void setSocketClient(SocketClient socketClient) {
		this.socketClient = socketClient;
	}

	/**
	 * @return the random process prefix (e.g. {@code p_aBcDe}) identifying the
	 *         spawned server
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * @param prefix the process prefix identifying the spawned server
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * @return the spawned OS process, or null if none was started (e.g. debug
	 *         attach mode)
	 */
	public Process getProcess() {
		return process;
	}

	/**
	 * @param process the spawned OS process
	 */
	public void setProcess(Process process) {
		this.process = process;
	}

	/**
	 * @return the port the server is connected on
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port the server is connected on
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the working/scratch directory for the server process
	 */
	public String getServerDirectory() {
		return serverDirectory;
	}

	/**
	 * @param serverDirectory the working/scratch directory for the server process
	 */
	public void setServerDirectory(String serverDirectory) {
		this.serverDirectory = serverDirectory;
	}

	/**
	 * Spawn the Java {@code SocketServer} worker process (via a generated starter
	 * script), applying the configured memory and ulimit settings.
	 *
	 * @param cp            classpath to use, or null for the built-in default jar
	 *                      list
	 * @param insightFolder working directory for the process
	 * @param port          port to bind, or null to omit the port argument
	 * @return the spawned process, or null if it failed to start
	 */
	public static Process startTCPServer(String cp, String insightFolder, String port) {
		Process thisProcess = null;
		if (cp == null) {
			cp = "fst-2.56.jar;jep-3.9.0.jar;log4j-1.2.17.jar;commons-io-2.4.jar;objenesis-2.5.1.jar;jackson-core-2.9.5.jar;javassist-3.20.0-GA.jar;netty-all-4.1.47.Final.jar;classes";
		}
		String specificPath = Utility.getCP(cp, insightFolder);
		try {
			String java = System.getenv(Constants.JAVA_HOME);
			if (java == null) {
				java = Utility.getDIHelperProperty(Constants.JAVA_HOME);
			}
			java = java.trim();
			if (!java.endsWith("bin")) {
				// seems like for graal
				java = java + "/bin/java";
			} else {
				java = java + "/java";
			}
			// account for spaces in the path to java
			if (java.contains(" ")) {
				java = "\"" + java + "\"";
			}
			// change the \\
			java = java.replace("\\", "/");

			String tcpWorker = Utility.getDIHelperProperty(Constants.TCP_WORKER);
			if (tcpWorker == null || (tcpWorker = tcpWorker.trim()).isEmpty()) {
				tcpWorker = prerna.tcp.SocketServer.class.getName();
			}
			String[] commands = null;
			if (port == null) {
				commands = new String[6];
			} else {
				commands = new String[7];
				commands[6] = port;
			}
			String finalDir = insightFolder.replace("\\", "/");
			commands[0] = java;

			// compose for memory
			String xms = Utility.getDIHelperProperty("Xms");
			String xmx = Utility.getDIHelperProperty("Xmx");
			String memory = "";
			if (xms != null && xmx != null) {
				memory = "-Xms" + xms + " -Xmx" + xmx;
			}
			commands[1] = memory + " -cp";
			commands[2] = specificPath;
			commands[3] = tcpWorker;
			commands[4] = finalDir;
			commands[5] = DIHelper.getInstance().getRDFMapFileLocation();

			classLogger.debug("Trying to create file in .. {}", finalDir);
			File file = new File(finalDir + "/init");
			file.createNewFile();
			classLogger.debug("Python start commands ... ");
			classLogger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS
					&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
				String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " + ulimit + " && " + sb.toString() + "\"" };
			}

			classLogger.info("Starting user process with ::: {}", Arrays.toString(commands));
			String[] starterFile = writeStarterFile(commands, finalDir);
			ProcessBuilder pb = new ProcessBuilder(starterFile);
			pb.redirectError();
			Process p = pb.start();
			try {
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error("Interrupted while waiting for the java server process to start", ie);
			}
			classLogger.info("Finished waiting for user process");
			thisProcess = p;
		} catch (IOException ioe) {
			classLogger.error("Failed to start the java server process", ioe);
		}

		return thisProcess;
	}

	/**
	 * Chroot variant of {@link #startTCPServer(String, String, String)}: spawns the
	 * Java {@code SocketServer} worker inside the given chroot directory via a
	 * generated starter script.
	 *
	 * @param cp            classpath to use, or null for the built-in default jar
	 *                      list
	 * @param chrootDir     the chroot root the process is launched inside
	 * @param insightFolder working directory for the process (relative to the
	 *                      chroot)
	 * @param port          port to bind, or null to omit the port argument
	 * @return the spawned process, or null if it failed to start
	 */
	public static Process startTCPServerChroot(String cp, String chrootDir, String insightFolder, String port) {
		Process thisProcess = null;
		if (cp == null) {
			cp = "fst-2.56.jar;jep-3.9.0.jar;log4j-1.2.17.jar;commons-io-2.4.jar;objenesis-2.5.1.jar;jackson-core-2.9.5.jar;javassist-3.20.0-GA.jar;netty-all-4.1.47.Final.jar;classes";
		}
		String specificPath = Utility.getCP(cp, insightFolder);
		try {
			String java = System.getenv(Constants.JAVA_HOME);
			if (java == null) {
				java = Utility.getDIHelperProperty(Constants.JAVA_HOME);
			}
			java = java.trim();
			if (!java.endsWith("bin")) {
				// seems like for graal
				java = java + "/bin/java";
			} else {
				java = java + "/java";
			}
			// account for spaces in the path to java
			if (java.contains(" ")) {
				java = "\"" + java + "\"";
			}
			// change the \\
			java = java.replace("\\", "/");

			String tcpWorker = Utility.getDIHelperProperty(Constants.TCP_WORKER);
			if (tcpWorker == null || (tcpWorker = tcpWorker.trim()).isEmpty()) {
				tcpWorker = prerna.tcp.SocketServer.class.getName();
			}
			String[] commands = null;
			if (port == null) {
				commands = new String[6];
			} else {
				commands = new String[7];
				commands[6] = port;
			}
			String finalDir = insightFolder.replace("\\", "/");
			commands[0] = java;
			// compose for memory
			String xms = Utility.getDIHelperProperty("Xms");
			String xmx = Utility.getDIHelperProperty("Xmx");

			String memory = "";
			if (xms != null && xmx != null) {
				memory = "-Xms" + xms + " -Xmx" + xmx;
			}

			commands[1] = memory + " -cp";
			commands[2] = specificPath;
			commands[3] = tcpWorker;
			commands[4] = finalDir;
			commands[5] = DIHelper.getInstance().getRDFMapFileLocation();

			classLogger.debug("Trying to create file in .. {}", finalDir);
			File file = new File(chrootDir + finalDir + "/init");
			file.createNewFile();
			classLogger.debug("Python start commands ... ");
			classLogger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS
					&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
				String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " + ulimit + " && " + sb.toString() + "\"" };
			}

			classLogger.info("Starting user process with ::: {}", Arrays.toString(commands));
			String[] starterFile = writeStarterFile(commands, chrootDir, finalDir);
			ProcessBuilder pb = new ProcessBuilder(starterFile);
			pb.redirectError();
			Process p = pb.start();

			try {
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error("Interrupted while waiting for the chrooted java server process to start", ie);
			}
			classLogger.info("Finished waiting for user process");
			thisProcess = p;
		} catch (IOException ioe) {
			classLogger.error("Failed to start the chrooted java server process", ioe);
		}

		return thisProcess;
	}

	/**
	 * Spawn the native Python TCP server (gaas_tcp_socket_server.py), optionally
	 * under {@code sudo -u <PY_SERVER_USER>} and/or a memory ulimit. After
	 * starting, it polls briefly for an immediate crash and, if the process exited,
	 * parses the console output for a Python traceback to surface as the failure.
	 *
	 * @param insightFolder working directory for the process
	 * @param port          port to bind
	 * @param py            path to the Python executable, or null/empty to resolve
	 *                      the configured base Python
	 * @param timeout       idle timeout in minutes ("-1" for none)
	 * @param loggerLevel   log level for the spawned process (e.g. INFO)
	 * @return a two-element array: { the spawned {@link Process} (or null on
	 *         failure), the process prefix string }
	 */
	private static Thread startPyGobbler(InputStream stream, boolean isError, List<String> capturedLines) {
		Thread t = new Thread(() -> {
			try (BufferedReader r = new BufferedReader(new InputStreamReader(stream))) {
				String line;
				while ((line = r.readLine()) != null) {
					if (capturedLines != null) {
						capturedLines.add(line);
					}
					if (isError) {
						pyLogger.error(line);
					} else {
						pyLogger.info(line);
					}
				}
			} catch (IOException e) {
				// stream closed when process exits
			}
		});
		t.setDaemon(true);
		t.start();
		return t;
	}

	public static Object[] startTCPServerNativePy(String insightFolder, String port, String py, String timeout,
			String loggerLevel) {
		String prefix = "";
		Process thisProcess = null;
		String finalDir = insightFolder.replace("\\", "/");

		try {
			// only try to find the base python if one was not passed in
			if (py == null || py.isEmpty()) {
				py = getPythonExecutable();
			} else {
				classLogger.info("The python executable being used is: \"{}\"", py);
			}
			String pyBase = Utility.getBaseFolder().replace("\\", "/") + "/" + Constants.PY_BASE_FOLDER;
			String gaasServer = pyBase + "/gaas_tcp_socket_server.py";

			prefix = Utility.getRandomString(5);
			prefix = "p_" + prefix;

			String outputFile = finalDir + "/console.txt";

			String pythonUser = Utility.getDIHelperProperty(Settings.PY_SERVER_USER);
			String[] baseCommand = new String[] { py, gaasServer, "--port", port, "--max_count", "1", "--py_folder",
					pyBase, "--insight_folder", finalDir, "--prefix", prefix, "--timeout", timeout, "--logger_level",
					loggerLevel };

			String[] commands;
			if (pythonUser != null && !pythonUser.trim().isEmpty()) {
				commands = new String[baseCommand.length + 3];
				commands[0] = "sudo";
				commands[1] = "-u";
				commands[2] = pythonUser;
				System.arraycopy(baseCommand, 0, commands, 3, baseCommand.length);

				File pythonProcessFolder = new File(finalDir);
				if (pythonProcessFolder.exists() && pythonProcessFolder.isDirectory()) {
					pythonProcessFolder.setReadable(true, false);
					pythonProcessFolder.setWritable(true, false);
					pythonProcessFolder.setExecutable(true, false);
				}
			} else {
				commands = baseCommand;
			}

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS
					&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
				String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " + ulimit + " && " + sb.toString() + "\"" };
			}

			classLogger.info("Starting user/engine process with ::: {}", Arrays.toString(commands));
			ProcessBuilder pb = new ProcessBuilder(commands);
			boolean pyLogCapture = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.PY_LOG_CAPTURE_ENABLED));
			List<String> capturedLines = null;
			Thread stdoutGobbler = null;
			Thread stderrGobbler = null;
			if (!pyLogCapture) {
				ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(new File(outputFile));
				pb.redirectError(redirector);
				pb.redirectOutput(redirector);
			}
			Process p = pb.start();
			if (pyLogCapture) {
				capturedLines = new CopyOnWriteArrayList<>();
				stdoutGobbler = startPyGobbler(p.getInputStream(), false, capturedLines);
				stderrGobbler = startPyGobbler(p.getErrorStream(), true, capturedLines);
			}
			// Poll at short intervals for an immediate crash (import / syntax errors
			// surface within a few ms). A healthy process keeps running, so cap the
			// total wait small and return early the moment it exits; the socket
			// client's connect loop checks liveness for any later crash, so we do not
			// need to block long here.
			for (int i = 0; i < PROCESS_CRASH_POLL_ATTEMPTS && p.isAlive(); i++) {
				try {
					if (p.waitFor(PROCESS_CRASH_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
						break;
					}
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					classLogger.error("Interrupted while polling for an early python process crash", ie);
					break;
				}
			}
			classLogger.info("Finished waiting for user/engine process");
			if (!p.isAlive()) {
				StringBuilder errorMsg = new StringBuilder();
				if (pyLogCapture) {
					try {
						if (stdoutGobbler != null) {
							stdoutGobbler.join(200);
						}
						if (stderrGobbler != null) {
							stderrGobbler.join(200);
						}
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
					boolean inTraceback = false;
					for (String line : capturedLines) {
						if (line.startsWith("Traceback")) {
							inTraceback = true;
						}
						if (inTraceback) {
							errorMsg.append(line).append("\n");
						}
					}
				} else {
					// if it crashed here, then the outputFile will contain the error. Read file and
					// send error back
					try (FileReader fr = new FileReader(outputFile); BufferedReader br = new BufferedReader(fr)) {
						String line;
						while ((line = br.readLine()) != null) {
							if (line.startsWith("Traceback")) {
								errorMsg.append(line).append("\n");
								while ((line = br.readLine()) != null) {
									errorMsg.append(line).append("\n");
								}
							}
						}
					}
				}
				if (!errorMsg.toString().isEmpty()) {
					throw new IllegalStateException(errorMsg.toString());
				}
			}
			thisProcess = p;
		} catch (IOException ioe) {
			classLogger.error("Failed to start the native python server process", ioe);
		}

		return new Object[] { thisProcess, prefix };
	}

	/**
	 * Chroot variant of
	 * {@link #startTCPServerNativePy(String, String, String, String, String)}:
	 * spawns the native Python TCP server inside a chroot via
	 * {@code fakechroot fakeroot chroot}. {@code chrootDir} is usually something
	 * like {@code /opt/kunal__abc123123}, under which lives the full (fake) OS.
	 *
	 * @param chrootDir     the chroot root the process is launched inside
	 * @param insightFolder working directory for the process (relative to the
	 *                      chroot)
	 * @param port          port to bind
	 * @param timeout       idle timeout in minutes ("-1" for none)
	 * @param loggerLevel   log level for the spawned process (e.g. INFO)
	 * @return a two-element array: { the spawned {@link Process} (or null on
	 *         failure), the process prefix string }
	 */
	public static Object[] startTCPServerNativePyChroot(String chrootDir, String insightFolder, String port,
			String timeout, String loggerLevel) {
		String prefix = "";
		Process thisProcess = null;
		String finalDir = insightFolder.replace("\\", "/");

		try {
			String py = getPythonExecutable();
			String pyBase = Utility.getBaseFolder().replace("\\", "/") + "/" + Constants.PY_BASE_FOLDER;
			String gaasServer = pyBase + "/gaas_tcp_socket_server.py";

			prefix = Utility.getRandomString(5);
			prefix = "p_" + prefix;

			String outputFile = chrootDir + finalDir + "/console.txt";

			String[] commands = new String[] { "fakechroot", "fakeroot", "chroot", "--userspec=1001:1001", "/", "env",
					"-i", py, gaasServer, "--port", port, "--max_count", "1", "--py_folder", pyBase, "--insight_folder",
					finalDir, "--prefix", prefix, "--timeout", timeout, "--logger_level", loggerLevel,
					"--userChrootFolder", chrootDir };

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS
					&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
				String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " + ulimit + " && " + sb.toString() + "\"" };
			}

			classLogger.info("Starting user process with ::: {}", Arrays.toString(commands));
			ProcessBuilder pb = new ProcessBuilder(commands);
			ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(new File(outputFile));
			pb.redirectError(redirector);
			pb.redirectOutput(redirector);
			Process p = pb.start();
			try {
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error("Interrupted while waiting for the chrooted native python process to start", ie);
			}
			classLogger.info("Finished waiting for user process");
			thisProcess = p;
		} catch (IOException ioe) {
			classLogger.error("Failed to start the chrooted native python process", ioe);
		}

		return new Object[] { thisProcess, prefix };
	}

	/**
	 * Start the per-user Python worker inside the unprivileged Linux namespace
	 * sandbox (SANDBOX_MODE=NAMESPACE; legacy NSJAIL configs are also accepted) via
	 * py/sandbox_launcher.py. Additive alternative to
	 * {@link #startTCPServerNativePyChroot}.
	 *
	 * @param insightFolder working directory for the worker process
	 * @param port          port passed to the worker (it listens on a Unix domain
	 *                      socket, not TCP, inside the sandbox)
	 * @param timeout       idle timeout in minutes ("-1" for none)
	 * @param loggerLevel   log level for the spawned process (e.g. INFO)
	 * @param ioDirName     name for this worker's io-dir / jail folder (e.g.
	 *                      userid_sessionId so it is identifiable on disk); a
	 *                      random name is generated when null/empty
	 * @return { Process, prefix, udsPath, controlSocketPath, ioDir, jailDir,
	 *         controlDir }
	 */
	public static Object[] startTCPServerNativePySandbox(String insightFolder, String port, String timeout,
			String loggerLevel, String ioDirName) {
		String prefix = "";
		Process thisProcess = null;
		String udsPath = null;
		String controlSocketPath = null;
		String ioDir = null;
		String jailDir = null;
		String controlDir = null;
		String finalDir = insightFolder.replace("\\", "/");

		try {
			String py = getPythonExecutable();
			String baseFolder = Utility.getBaseFolder().replace("\\", "/");
			String pyBase = baseFolder + "/" + Constants.PY_BASE_FOLDER;
			String launcher = pyBase + "/sandbox_launcher.py";

			prefix = "p_" + Utility.getRandomString(5);

			String ioRoot = Utility.getDIHelperProperty(Constants.SANDBOX_IO_DIR);
			if (Strings.isNullOrEmpty(ioRoot)) {
				ioRoot = System.getProperty("java.io.tmpdir") + "/semoss-sandbox";
			}
			// AF_UNIX socket paths are limited by sockaddr_un.sun_path (commonly
			// 108 bytes), so keep runtime folder/socket names short even when the
			// user chroot folder name is long and human-readable.
			String sourceName = Strings.isNullOrEmpty(ioDirName) ? prefix
					: ioDirName.replaceAll("[^a-zA-Z0-9._-]", "_");
			String folderName = prefix + "_" + Integer.toUnsignedString(sourceName.hashCode(), 36);
			ioDir = ioRoot + "/" + folderName;
			jailDir = ioRoot + "/" + folderName + "_j";
			controlDir = ioRoot + "/" + folderName + "_c";
			new File(Utility.normalizePath(ioDir)).mkdirs();
			new File(Utility.normalizePath(jailDir)).mkdirs();
			new File(Utility.normalizePath(controlDir)).mkdirs();
			udsPath = ioDir + "/w.sock";
			controlSocketPath = controlDir + "/c.sock";

			// the SEMOSS home is the inject-root: projects, user assets, and
			// insight folders all live under it and are injected on demand
			String injectRoot = baseFolder;

			String outputFile = ioDir + "/console.txt";

			java.util.List<String> commands = new java.util.ArrayList<>();
			commands.add(py);
			commands.add(launcher);
			commands.add("--py-folder");
			commands.add(pyBase);
			commands.add("--insight-folder");
			commands.add(finalDir);
			commands.add("--io-dir");
			commands.add(ioDir);
			commands.add("--control-socket");
			commands.add(controlSocketPath);
			commands.add("--inject-root");
			commands.add(injectRoot);
			commands.add("--jail-root");
			commands.add(jailDir);
			commands.add("--");
			commands.add("--port");
			commands.add(port);
			commands.add("--max_count");
			commands.add("1");
			commands.add("--py_folder");
			commands.add(pyBase);
			commands.add("--insight_folder");
			commands.add(finalDir);
			commands.add("--prefix");
			commands.add(prefix);
			commands.add("--timeout");
			commands.add(timeout);
			commands.add("--logger_level");
			commands.add(loggerLevel);
			commands.add("--uds-path");
			commands.add(udsPath);

			String[] commandArray = commands.toArray(new String[0]);

			if (!SystemUtils.IS_OS_WINDOWS
					&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
				String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commandArray) {
					sb.append(str).append(" ");
				}
				commandArray = new String[] { "/bin/bash", "-c",
						"\"ulimit -v " + ulimit + " && " + sb.toString().trim() + "\"" };
			}

			classLogger.info("Starting namespace-sandboxed user process with ::: {}", Arrays.toString(commandArray));
			ProcessBuilder pb = new ProcessBuilder(commandArray);
			boolean pyLogCapture = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.PY_LOG_CAPTURE_ENABLED));
			File consoleFile = new File(outputFile);
			List<String> capturedLines = null;
			Thread stdoutGobbler = null;
			Thread stderrGobbler = null;
			if (!pyLogCapture) {
				ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(consoleFile);
				pb.redirectError(redirector);
				pb.redirectOutput(redirector);
			}
			Process p = pb.start();
			if (pyLogCapture) {
				capturedLines = new CopyOnWriteArrayList<>();
				stdoutGobbler = startPyGobbler(p.getInputStream(), false, capturedLines);
				stderrGobbler = startPyGobbler(p.getErrorStream(), true, capturedLines);
			}
			try {
				if (p.waitFor(500, TimeUnit.MILLISECONDS)) {
					String startupLog;
					if (pyLogCapture) {
						try {
							if (stdoutGobbler != null) {
								stdoutGobbler.join(200);
							}
							if (stderrGobbler != null) {
								stderrGobbler.join(200);
							}
						} catch (InterruptedException ie2) {
							Thread.currentThread().interrupt();
						}
						startupLog = String.join("\n", capturedLines);
					} else {
						startupLog = readSandboxStartupLog(consoleFile);
					}
					throw new IllegalStateException(
							"Namespace sandbox exited during startup with code " + p.exitValue() + ". " + startupLog);
				}
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted while waiting for namespace sandbox startup", ie);
			}
			thisProcess = p;
		} catch (IOException ioe) {
			throw new IllegalStateException("Failed to start namespace sandbox process", ioe);
		}

		return new Object[] { thisProcess, prefix, udsPath, controlSocketPath, ioDir, jailDir, controlDir };
	}

	/**
	 * Read the tail of a namespace-sandbox console log to enrich a startup-failure
	 * error message. Best-effort: returns a human-readable placeholder when the
	 * file is missing, empty, or unreadable, and truncates to the last ~4000 chars.
	 *
	 * @param consoleFile the sandbox console log file
	 * @return the (possibly truncated) log contents or an explanatory message
	 */
	private static String readSandboxStartupLog(File consoleFile) {
		if (consoleFile == null || !consoleFile.isFile()) {
			return "Sandbox console log was not created.";
		}
		try {
			String contents = Files.readString(consoleFile.toPath(), StandardCharsets.UTF_8).trim();
			if (contents.isEmpty()) {
				return "Sandbox console log is empty: " + consoleFile.getAbsolutePath();
			}
			int maxChars = 4_000;
			if (contents.length() > maxChars) {
				contents = contents.substring(contents.length() - maxChars);
			}
			return "Sandbox console log (" + consoleFile.getAbsolutePath() + "):\n" + contents;
		} catch (IOException ioe) {
			return "Could not read sandbox console log " + consoleFile.getAbsolutePath() + ": " + ioe.getMessage();
		}
	}

	/**
	 * Resolve the base Python executable path, checking in order: the
	 * {@code PYTHONHOME} env var, {@code PYTHONHOME} in the RDF_Map, the
	 * {@code PY_HOME} env var, then {@code PY_HOME} in the RDF_Map. The OS-specific
	 * binary ({@code /bin/python3} or {@code python.exe}) is appended.
	 *
	 * @return the absolute path to the Python executable
	 * @throws NullPointerException if no Python home is configured
	 */
	private static String getPythonExecutable() {
		String py = System.getenv(Settings.PYTHONHOME);
		if (py == null) {
			py = Utility.getDIHelperProperty(Settings.PYTHONHOME);
		}
		if (py == null) {
			py = System.getenv(Settings.PY_HOME);
		}
		if (py == null) {
			py = Utility.getDIHelperProperty(Settings.PY_HOME);
		}
		if (py == null) {
			throw new NullPointerException("Must define python home");
		}
		py = py.trim();
		if (SystemUtils.IS_OS_WINDOWS) {
			py = py + "/python.exe";
		} else {
			py = py + "/bin/python3";
		}
		py = py.replace("\\", "/");
		classLogger.info("The python executable being used is: \"{}\"", py);
		return py;
	}

	/**
	 * Write the given command tokens to a {@code starter.sh}/{@code starter.bat}
	 * script in {@code dir} (chmod 777 on unix) and return the command array used
	 * to invoke it. When {@code ENABLE_BINDFS} is set on unix the returned command
	 * wraps the script in {@code fakechroot fakeroot chroot}.
	 *
	 * @param commands the process command tokens to write into the starter script
	 * @param dir      the directory to write the starter script into
	 * @return the command array that launches the starter script
	 */
	public static String[] writeStarterFile(String[] commands, String dir) {
		// check if the os is unix and if so make it .sh
		String osName = System.getProperty("os.name").toLowerCase();

		String starter = "";
		String[] commandsStarter = null;

		if (osName.indexOf("win") >= 0) {
			commandsStarter = new String[1];
			commandsStarter[0] = dir + "/starter.bat";
			starter = dir + "/starter.bat";
		}
		if (osName.indexOf("win") < 0) {
			commandsStarter = new String[2];
			commandsStarter[0] = "/bin/bash";
			starter = dir + "/starter.sh";
			commandsStarter[1] = starter;
		}
		try {
			File starterFile = new File(starter);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (int cmdIndex = 0; cmdIndex < commands.length; cmdIndex++) {
				baos.write(commands[cmdIndex].getBytes());
				baos.write("  ".getBytes());
			}
			FileUtils.writeByteArrayToFile(starterFile, baos.toByteArray());

			// chmod in case.. who knows
			if (osName.indexOf("win") < 0) {
				ProcessBuilder p = new ProcessBuilder("/bin/chmod", "777", starter);
				p.start();
			}
		} catch (FileNotFoundException fnfe) {
			classLogger.error("Failed to write the python server starter file", fnfe);
		} catch (IOException ioe) {
			classLogger.error("Failed to write the python server starter file", ioe);
		}

		if (Boolean.parseBoolean(Utility.getDIHelperProperty("ENABLE_BINDFS")) && osName.indexOf("win") < 0) {
			commandsStarter = new String[5];
			starter = dir + "/starter.sh";
			commandsStarter[0] = "fakechroot";
			commandsStarter[1] = "fakeroot";
			commandsStarter[2] = "chroot";
			commandsStarter[3] = "/bin/bash";
			commandsStarter[4] = starter;
		}

		return commandsStarter;
	}

	/**
	 * Chroot variant of {@link #writeStarterFile(String[], String)}: writes the
	 * starter script and, when {@code CHROOT_ENABLE} is set on unix, returns a
	 * command array that runs it under
	 * {@code fakechroot fakeroot chroot <chrootDir>}.
	 *
	 * @param commands  the process command tokens to write into the starter script
	 * @param chrootDir the chroot root the script is launched inside
	 * @param dir       the directory (relative to the chroot) to write the starter
	 *                  script into
	 * @return the command array that launches the starter script
	 */
	public static String[] writeStarterFile(String[] commands, String chrootDir, String dir) {
		// check if the os is unix and if so make it .sh
		String osName = System.getProperty("os.name").toLowerCase();

		String starter = "";
		String[] commandsStarter = null;

		if (osName.indexOf("win") >= 0) {
			commandsStarter = new String[1];
			commandsStarter[0] = dir + "/starter.bat";
			starter = dir + "/starter.bat";
		}
		if (osName.indexOf("win") < 0) {
			commandsStarter = new String[2];
			commandsStarter[0] = "/bin/bash";
			starter = dir + "/starter.sh";
			commandsStarter[1] = starter;
		}
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE)) && osName.indexOf("win") < 0) {
			commandsStarter = new String[6];
			starter = dir + "/starter.sh";
			commandsStarter[0] = "fakechroot";
			commandsStarter[1] = "fakeroot";
			commandsStarter[2] = "chroot";
			commandsStarter[3] = chrootDir;
			commandsStarter[4] = "/bin/bash";
			commandsStarter[5] = starter;

			starter = chrootDir + dir + "/starter.sh";

		}

		try {
			File starterFile = new File(starter);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (int cmdIndex = 0; cmdIndex < commands.length; cmdIndex++) {
				baos.write(commands[cmdIndex].getBytes());
				baos.write("  ".getBytes());
			}
			FileUtils.writeByteArrayToFile(starterFile, baos.toByteArray());

			// chmod in case.. who knows
			if (osName.indexOf("win") < 0) {
				ProcessBuilder p = new ProcessBuilder("/bin/chmod", "777", starter);
				p.start();
			}
		} catch (FileNotFoundException fnfe) {
			classLogger.error("Failed to write the python server starter file", fnfe);
		} catch (IOException ioe) {
			classLogger.error("Failed to write the python server starter file", ioe);
		}

		return commandsStarter;
	}

	/**
	 * Write the {@code log4j2.properties} file for the socket server by reading the
	 * template at {@code <BASE_FOLDER>/py/log-config/log4j.properties} and pointing
	 * its {@code FILE_LOCATION} at {@code dir/output.log}.
	 *
	 * @param dir the server directory to write {@code log4j2.properties} into and
	 *            where the server log should be written
	 */
	public static void writeLogConfigurationFile(String dir) {
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile, StandardCharsets.UTF_8);
			// property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", dir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig, StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Failed to write the log4j2.properties file for the socket server", e);
		}
	}

	/**
	 * Chroot variant of {@link #writeLogConfigurationFile(String)}: the
	 * {@code log4j2.properties} file is written to {@code dir} but its
	 * {@code FILE_LOCATION} points at {@code paramDir/output.log} (the path as seen
	 * from inside the chroot, which differs from the host path).
	 *
	 * @param dir      the host-side directory to write {@code log4j2.properties}
	 *                 into
	 * @param paramDir the chroot-relative directory the server log should be
	 *                 written to
	 */
	public static void writeLogConfigurationFile(String dir, String paramDir) {
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = Utility.getDIHelperProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile, StandardCharsets.UTF_8);
			// property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", paramDir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig, StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Failed to write the log4j2.properties file for the socket server", e);
		}
	}
}
