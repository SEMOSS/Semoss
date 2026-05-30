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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.tcp.client.NativePySocketClient;
import prerna.tcp.client.SocketClient;
import prerna.util.PortAllocator;
import prerna.util.SandboxInjector;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

public class ClientProcessWrapper {

	private static final Logger classLogger = LogManager.getLogger(ClientProcessWrapper.class);

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
	// NSJAIL sandbox folders to remove on shutdown(true): the io-dir holds the
	// worker/control sockets + console.txt, the jail-dir holds the throwaway root
	private String sandboxIoDir;
	private String sandboxJailDir;

	private boolean nativePyServer;
	private SymlinkHelper chrootSymlinkHelper;
	private String classPath;
	private boolean debug;
	private String timeout;
	private String loggerLevel;

	private Map<String, String> threadLoggerCtx;

	/**
	 * 
	 * @param nativePyServer
	 * @param chrootSymlinkHelper
	 * @param port
	 * @param venvPath
	 * @param serverDirectory
	 * @param classPath
	 * @param debug
	 * @param timeout
	 * @param loggerLevel
	 * @throws Exception
	 */
	public void createProcessAndClient(boolean nativePyServer, SymlinkHelper chrootSymlinkHelper, int port,
			String venvPath, String serverDirectory, String classPath, boolean debug, String timeout,
			String loggerLevel) throws Exception {
		this.createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath,
				debug, timeout, loggerLevel, new HashMap<>());
	}

	/**
	 * 
	 * @param nativePyServer
	 * @param chrootSymlinkHelper
	 * @param port
	 * @param venvPath
	 * @param serverDirectory
	 * @param classPath
	 * @param debug
	 * @param timeout
	 * @param loggerLevel
	 * @param threadLoggerCtx
	 * @throws Exception
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
						Utility.writeLogConfigurationFile(this.serverDirectory);

						// name the sandbox folders after the user's chroot folder
						// (userid_sessionId) so they are identifiable on disk
						String ioDirName = Paths.get(this.chrootSymlinkHelper.getUserChrootFolder()).getFileName()
								.toString();

						Object[] ret = Utility.startTCPServerNativePySandbox(this.serverDirectory, this.port + "",
								this.timeout, this.loggerLevel, ioDirName);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
						this.udsPath = (String) ret[2];
						this.controlSocketPath = (String) ret[3];
						this.sandboxIoDir = (String) ret[4];
						this.sandboxJailDir = (String) ret[5];

						this.chrootSymlinkHelper.setInjector(new SandboxInjector(this.controlSocketPath));
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
						Utility.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);

						Object[] ret = Utility.startTCPServerNativePyChroot(
								this.chrootSymlinkHelper.getUserChrootFolder(), relative, this.port + "", this.timeout,
								this.loggerLevel);
						this.process = (Process) ret[0];
						this.prefix = (String) ret[1];
					} else {
						// write the log4j file in the server directory
						Utility.writeLogConfigurationFile(this.serverDirectory);

						Object[] ret = Utility.startTCPServerNativePy(this.serverDirectory, this.port + "",
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
						Utility.writeLogConfigurationFile(chrootBaseFolderPath.toString(), relative);

						this.process = Utility.startTCPServerChroot(classPath,
								this.chrootSymlinkHelper.getUserChrootFolder(), relative, this.port + "");
					} else {
						// write the log4j file in the server directory
						Utility.writeLogConfigurationFile(this.serverDirectory);
						this.process = Utility.startTCPServer(classPath, this.serverDirectory, this.port + "");
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
	 * 
	 */
	public void shutdown(boolean cleanUpFolder) {
		synchronized (lockDestroy) {
			if (this.socketClient != null && this.socketClient.isConnected()) {
				ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

				Callable<Boolean> callableTask = () -> {
					boolean result = false;
					if (cleanUpFolder) {
						this.socketClient.stopServer();
						classLogger.info("Sucessfully stopped the process");
						// remove the insight scratch dir plus, for NSJAIL, the sandbox
						// io-dir and jail-dir (no-ops when those are null)
						result = deleteFolderWithRetries(this.serverDirectory)
								& deleteFolderWithRetries(this.sandboxIoDir)
								& deleteFolderWithRetries(this.sandboxJailDir);
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
					executor.shutdown();

					// reset the venv path
					this.venvPath = null;
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
	 *
	 * @throws Exception
	 */
	public void reconnect() throws Exception {
		createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath, debug,
				timeout, loggerLevel, threadLoggerCtx);
	}

	/**
	 * 
	 * @param venvEngineId
	 * @throws Exception
	 */
	public void reconnect(String venvEngineId) throws Exception {
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		createProcessAndClient(nativePyServer, chrootSymlinkHelper, port, venvPath, serverDirectory, classPath, debug,
				timeout, loggerLevel, threadLoggerCtx);
	}

	/**
	 * 
	 * @param port
	 * @return
	 */
	private int calculatePort(int port) {
		if (port < 0) {
			port = PortAllocator.getInstance().getNextAvailablePort();
		}

		return port;
	}

	/**
	 * 
	 * @return
	 */
	public SocketClient getSocketClient() {
		return socketClient;
	}

	/**
	 * 
	 * @param socketClient
	 */
	public void setSocketClient(SocketClient socketClient) {
		this.socketClient = socketClient;
	}

	/**
	 * 
	 * @return
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * 
	 * @param prefix
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * 
	 * @return
	 */
	public Process getProcess() {
		return process;
	}

	/**
	 * 
	 * @param process
	 */
	public void setProcess(Process process) {
		this.process = process;
	}

	/**
	 * 
	 * @return
	 */
	public int getPort() {
		return port;
	}

	/**
	 * 
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * 
	 * @return
	 */
	public String getServerDirectory() {
		return serverDirectory;
	}

	/**
	 * 
	 * @param serverDirectory
	 */
	public void setServerDirectory(String serverDirectory) {
		this.serverDirectory = serverDirectory;
	}

}
