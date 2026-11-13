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
package prerna.auth;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.javatuples.Pair;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;

import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RRemoteRserve;
import prerna.engine.impl.r.RUserConnectionDedicated;
import prerna.om.ClientProcessWrapper;
import prerna.om.LocalUserStore;
import prerna.project.api.IProject;
import prerna.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.reactor.mgmt.MgmtUtil;
import prerna.reactor.playwright.PlaywrightSession;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

public class User implements Serializable {

	private static Logger classLogger = LogManager.getLogger(User.class);

	private static final String DIR_SEPARATOR = "/";

	// main object storing the users access tokens
	private Map<AuthProvider, AccessToken> accessTokens = new ConcurrentHashMap<>();
	private List<AuthProvider> loggedInProfiles = Collections.synchronizedList(new ArrayList<>());
	// storing the timezone the user is in
	private ZoneId zoneId;

	// store model conversation rooms
	private Map<String, Room> roomHash = new ConcurrentHashMap<>();

	// store the users insights
	private transient Map<String, List<String>> openInsights = null;

	// need to have an access token store
	private transient IRUserConnection rcon;
	private transient RRemoteRserve rconRemote;

	// python related stuff
	private transient ClientProcessWrapper pythonCPW = new ClientProcessWrapper();

	// node.js agent execution environment - lazily spawned, agent-tool only
	private transient ClientProcessWrapper nodeCPW = new ClientProcessWrapper();

	private transient ClientProcessWrapper javaCPW = new ClientProcessWrapper();

	private String chrootPath = null;
	private transient volatile SymlinkHelper symlinkHelper = null;

	// playwright
	private transient volatile Map<String, PlaywrightSession> playwrightSessionStore = null;
	private transient volatile BrowserContext sharedPlaywrightContext;

	private Map<AuthProvider, String> assetProjectMap = new ConcurrentHashMap<>();
	private AuthProvider primaryLogin;

	// in-memory mutex for user-scoped lazy init/mutations (asset project creation,
	// symlink setup, temporal key generation, playwright context/session store
	// setup)
	private transient ReentrantLock userLock = new ReentrantLock();

	// need to move everything here
	// since on reconnect we need to redo serialization.
	private Map<String, Boolean> insightSerializedMap = new ConcurrentHashMap<>();

	// this is a unique identifier for this user instance
	private String userEpoch = null;

	private transient volatile String[] cachedTemporalAccessSecretKey = null;

	public User() {
		// transient objects should be defined in the constructor
		// since if this is serialized we dont want these values to be null
		this.openInsights = new HashMap<>();
		// set it in the mgmt utils
		addUserMemory();
		this.userEpoch = UUID.randomUUID().toString();
		this.playwrightSessionStore = new ConcurrentHashMap<>();
	}

	/**
	 * Set the access token for a given provider
	 * 
	 * @param value
	 */
	public void setAccessToken(AccessToken value) {
		value = ReadOnlyAccessToken.unmodifiableToken(value);
		AuthProvider name = value.getProvider();
		if (!loggedInProfiles.contains(name)) {
			loggedInProfiles.add(name);
		}
		accessTokens.put(name, value);
	}

	/**
	 * Set the access token for a given provider We do not register in the logged in
	 * profiles but can still grab from reactors that utilize them
	 * 
	 * @param value
	 */
	public void setGlobalAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}

	/**
	 * Get the requested access token
	 * 
	 * @param name
	 * @return
	 */
	public AccessToken getAccessToken(AuthProvider name) {
		return accessTokens.get(name);
	}

	/**
	 * Drop the access token for a given provider
	 * 
	 * @param name The name of the provider
	 * @return boolean if the provider was dropped
	 */
	public boolean dropAccessToken(String name) {
		// remove from token map
		AuthProvider tokenKey = AuthProvider.valueOf(name);
		AccessToken token = accessTokens.remove(tokenKey);
		// remove from profiles list
		loggedInProfiles.remove(tokenKey);

		// return false if the token actually wasn't found
		return token != null;
	}

	/**
	 * Drop the access token for a given provider
	 * 
	 * @param tokenKey The name of the provider
	 * @return boolean if the provider was dropped
	 */
	public boolean dropAccessToken(AuthProvider tokenKey) {
		// remove from token map
		AccessToken token = accessTokens.remove(tokenKey);
		// remove from profiles list
		loggedInProfiles.remove(tokenKey);
		// return false if the token actually wasn't found
		if (token == null) {
			return false;
		}

		// recalculate the primary login
		if (this.primaryLogin == tokenKey && !this.loggedInProfiles.isEmpty()) {
			this.primaryLogin = this.loggedInProfiles.get(0);
		}
		return true;
	}

	/**
	 * Get the list of logged in profiles
	 * 
	 * @return
	 */
	public List<AuthProvider> getLogins() {
		return loggedInProfiles;
	}

	public boolean isLoggedIn() {
		return !this.loggedInProfiles.isEmpty();
	}

	/**
	 * Returns whether another user is the same person as this one: the same object,
	 * or a user who shares a login with the same id and login type.
	 *
	 * @param other the user to compare with, or null
	 * @return true when both are the same person
	 */
	public boolean isSamePerson(User other) {
		if (other == this) {
			return true;
		}
		if (other == null) {
			return false;
		}
		for (AuthProvider login : getLogins()) {
			AccessToken mine = getAccessToken(login);
			AccessToken theirs = other.getAccessToken(login);
			if (mine != null && theirs != null && mine.getId() != null && mine.getId().equals(theirs.getId())) {
				return true;
			}
		}
		return false;
	}

	////////////////////////////////////////////////////////////////////////

	public AuthProvider getPrimaryLogin() {
		if (this.primaryLogin == null && isLoggedIn()) {
			this.primaryLogin = this.loggedInProfiles.get(0);
		}
		return this.primaryLogin;
	}

	public AccessToken getPrimaryLoginToken() {
		if (this.primaryLogin == null && isLoggedIn()) {
			this.primaryLogin = this.loggedInProfiles.get(0);
		}
		return accessTokens.get(primaryLogin);
	}

	public void setPrimaryLogin(AuthProvider primaryLogin) {
		this.primaryLogin = primaryLogin;
	}

	public String getAssetProjectId(AuthProvider token) {
		if (token == null) {
			return null;
		}

		String cachedProjectId = this.assetProjectMap.get(token);
		if (cachedProjectId != null) {
			return cachedProjectId;
		}

		String projectId = UserAssetUtils.getUserAssetProject(this, token);

		if (projectId != null) {
			this.assetProjectMap.put(token, projectId);
		} else {
			ReentrantLock lock = getUserLock();
			lock.lock();
			try {
				projectId = UserAssetUtils.getUserAssetProject(this, token);
				if (projectId == null) {
					projectId = UserAssetUtils.createUserAssetProject(this, token);
				}
			} catch (Exception e) {
				classLogger.error("Failed to load or create user asset project for token {}", token, e);
			} finally {
				lock.unlock();
			}

			if (projectId != null) {
				this.assetProjectMap.put(token, projectId);
			}
		}

		if (projectId == null) {
			return null;
		}

		// TODO actually sync the pull, not sure pull it
		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pullUserAsset(projectId, false);
		}

		return projectId;
	}

	/**
	 * Convenience wrapper that returns the user's asset project as an IProject,
	 * resolved via the primary login token.
	 */
	public IProject getAssetProject() {
		return getAssetProject(getPrimaryLogin());
	}

	/**
	 * Convenience wrapper that returns the user's asset project as an IProject for
	 * the given auth provider token.
	 */
	public IProject getAssetProject(AuthProvider token) {
		String projectId = getAssetProjectId(token);
		if (projectId == null) {
			return null;
		}
		return Utility.getUserAssetProject(projectId);
	}

	public Map<AuthProvider, String> getAssetEngineMap() {
		return this.assetProjectMap;
	}

	public String getUserEpoch() {
		return userEpoch;
	}

	////////////////////////////////////////////////////////////////////////

	public IRUserConnection getRcon() {
		return rcon;
	}

	public void setRcon(IRUserConnection rcon) {
		this.rcon = rcon;
	}

	public RRemoteRserve getRconRemote() {
		return rconRemote;
	}

	public void setRconRemote(RRemoteRserve rconRemote) {
		this.rconRemote = rconRemote;
	}

	/**
	 * Store the open insight
	 * 
	 * @param engineId
	 * @param rdbmsId
	 * @param insightId
	 */
	public void addOpenInsight(String engineId, String rdbmsId, String insightId) {
		if (this.openInsights == null) {
			this.openInsights = new HashMap<>();
		}
		String id = getUid(engineId, rdbmsId);
		List<String> openInstances = null;
		if (this.openInsights.containsKey(id)) {
			openInstances = this.openInsights.get(id);
		} else {
			openInstances = new Vector<>();
			this.openInsights.put(id, openInstances);
		}
		openInstances.add(insightId);
	}

	/**
	 * Remove open insight
	 * 
	 * @param engineId
	 * @param rdbmsId
	 * @param insightId
	 */
	public void removeOpenInsight(String engineId, String rdbmsId, String insightId) {
		if (this.openInsights == null) {
			return;
		}
		String id = getUid(engineId, rdbmsId);
		List<String> openInstances = null;
		if (this.openInsights.containsKey(id)) {
			openInstances = this.openInsights.get(id);
			openInstances.remove(insightId);
		}
	}

	/**
	 * Grab the open insight ids for a specific saved insight
	 * 
	 * @param engineId
	 * @param rdbmsId
	 * @return
	 */
	public List<String> getOpenInsightInstances(String engineId, String rdbmsId) {
		String id = getUid(engineId, rdbmsId);
		return this.openInsights.get(id);
	}

	private String getUid(String engineId, String rdbmsId) {
		return engineId + "__" + rdbmsId;
	}

	/**
	 * Get the user open insights
	 * 
	 * @return
	 */
	public Map<String, List<String>> getOpenInsights() {
		return openInsights;
	}

	/**
	 * 
	 * @param zoneId
	 */
	public void setZoneId(ZoneId zoneId) {
		this.zoneId = zoneId;
	}

	/**
	 * 
	 * @return
	 */
	public ZoneId getZoneId() {
		return zoneId;
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, Room> getRoomHash() {
		return roomHash;
	}

	/**
	 * Returns the user-level lock, lazily reinitializing after deserialization when
	 * transient fields are null.
	 */
	private ReentrantLock getUserLock() {
		ReentrantLock lock = this.userLock;
		if (lock != null) {
			return lock;
		}
		synchronized (this) {
			lock = this.userLock;
			if (lock == null) {
				lock = new ReentrantLock();
				this.userLock = lock;
			}
		}
		return lock;
	}

	/**
	 * 
	 * @return
	 */
	public ClientProcessWrapper getPythonClientProcessWrapper() {
		return this.pythonCPW;
	}

	/**
	 * 
	 * @param create
	 * @return
	 */
	public SocketClient getPythonSocketClient(boolean create) {
		return getPythonSocketClient(create, -1, null);
	}

	/**
	 * 
	 * @param create
	 * @param venvEngineId
	 * @return
	 */
	public SocketClient getPythonSocketClient(boolean create, String venvEngineId) {
		return getPythonSocketClient(create, -1, venvEngineId);
	}

	/**
	 * 
	 * @param create
	 * @param port
	 * @return
	 */
	public SocketClient getPythonSocketClient(boolean create, int port, String venvEngineId) {
		if (!create) {
			if (this.pythonCPW == null) {
				return null;
			}
			return this.pythonCPW.getSocketClient();
		}
		if (this.pythonCPW == null) {
			this.pythonCPW = new ClientProcessWrapper();
		}
		SocketClient client = this.pythonCPW
				.getOrStartSocketClient(() -> startPythonSocketServerAndClient(-1, venvEngineId));
		client.setUser(this);
		return client;
	}

	/**
	 * 
	 * @return
	 */
	public SymlinkHelper getUserSymlinkHelper() {
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			if (symlinkHelper != null) {
				return symlinkHelper;
			}

			ReentrantLock lock = getUserLock();
			lock.lock();
			try {
				if (symlinkHelper == null) {
					String uniqueUserName = getSingleLogginName(this) + "-" + UUID.randomUUID().toString();
					String chrootDir = Utility.getDIHelperProperty(Constants.CHROOT_DIR);
					chrootPath = chrootDir + DIR_SEPARATOR + uniqueUserName;
					symlinkHelper = new SymlinkHelper(chrootPath);

					// symlink the user asset folder into the chroot on boot
					try {
						symlinkHelper.symlinkUserAsset(this);
					} catch (Exception e) {
						classLogger.warn("Unable to symlink user asset folder into chroot", e);
					}
				}
			} finally {
				lock.unlock();
			}
			return symlinkHelper;
		}

		throw new IllegalArgumentException("Chroot is not enabled on this instance");
	}

	/**
	 * Starts this user's native Python server process and connects its socket
	 * client. The process runs inside the user's chroot when chroot is enabled.
	 *
	 * @param port         the port to connect on; negative to auto-allocate, or to
	 *                     use FORCE_PORT when it is set
	 * @param venvEngineId the virtual environment engine whose Python executable to
	 *                     use, or null for the base Python
	 */
	public void startPythonSocketServerAndClient(int port, String venvEngineId) {
		if (this.pythonCPW == null) {
			this.pythonCPW = new ClientProcessWrapper();
		}
		if (this.pythonCPW.getSocketClient() == null || !this.pythonCPW.getSocketClient().isConnected()) {
			boolean debug = false;
			if (port < 0) {
				String forcePort = Utility.getDIHelperProperty(Settings.FORCE_PORT);
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger.warn("User {} has an invalid FORCE_PORT value", User.getSingleLogginName(this));
					}
				}
			}

			String loggerLevel = Utility.getDIHelperProperty(Settings.LOGGER_LEVEL);
			if (loggerLevel == null || (loggerLevel = loggerLevel.trim()).isEmpty()) {
				loggerLevel = "WARNING";
			}

			String serverDirectory = Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR);
			Path serverDirectoryPath = null;

			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				// unique user is just for testing so when i ls on R, I can see it is me and not
				// someone else
				this.symlinkHelper = getUserSymlinkHelper();

				// we do not define the Server Directory here - because it will dynamically
				// generate in the chroot location
				try {
					this.pythonCPW.createProcessAndClient(true, this.symlinkHelper, port, null, null, debug, "-1",
							loggerLevel, ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start chrooted python process for user {}",
							User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to user server");
				}
			} else {
				try {
					serverDirectoryPath = getWorkerDirectory(this.pythonCPW, serverDirectory, "a");
				} catch (IOException e) {
					classLogger.error("Failed to create temp directory for non-chroot python process under {}",
							serverDirectory, e);
					throw new IllegalArgumentException("Could not create directory to launch project process");
				}

				classLogger.info("Starting Non-chroot TCP Server for User = {}", User.getSingleLogginName(this));
				try {
					String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable()
							: this.pythonCPW.getVenvPath();
					this.pythonCPW.createProcessAndClient(true, null, port, venvPath, serverDirectoryPath.toString(),
							debug, "-1", loggerLevel, ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start non-chroot python process for user {}",
							User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to user server");
				}
			}
		}
	}

	/**
	 *
	 * @return
	 */
	public ClientProcessWrapper getNodeClientProcessWrapper() {
		return this.nodeCPW;
	}

	/**
	 * Get the socket client for this user's node.js agent worker, optionally
	 * starting the worker if it is not running. Unlike the python process, the node
	 * worker is only ever reached through agent harness tools - there is no
	 * user-facing reactor or endpoint for it.
	 *
	 * @param create true to lazily start/revive the worker
	 * @return the socket client, or null when create is false and none exists
	 */
	public SocketClient getNodeSocketClient(boolean create) {
		if (!create) {
			if (this.nodeCPW == null) {
				return null;
			}
			return this.nodeCPW.getSocketClient();
		}
		if (this.nodeCPW == null) {
			this.nodeCPW = new ClientProcessWrapper();
		}
		SocketClient client = this.nodeCPW.getOrStartSocketClient(() -> startNodeSocketServerAndClient(-1));
		client.setUser(this);
		return client;
	}

	/**
	 * Start the per-user node.js agent worker and its socket client. The worker
	 * self-exits after an idle period (NODE_IDLE_TIMEOUT minutes, default 30) since
	 * no user session depends on it directly.
	 *
	 * @param port the port to connect on; negative to auto-allocate
	 */
	public void startNodeSocketServerAndClient(int port) {
		if (this.nodeCPW == null) {
			this.nodeCPW = new ClientProcessWrapper();
		}
		if (this.nodeCPW.getSocketClient() == null || !this.nodeCPW.getSocketClient().isConnected()) {
			String loggerLevel = Utility.getDIHelperProperty(Settings.LOGGER_LEVEL);
			if (loggerLevel == null || (loggerLevel = loggerLevel.trim()).isEmpty()) {
				loggerLevel = "WARNING";
			}

			String idleTimeout = Utility.getDIHelperProperty(Settings.NODE_IDLE_TIMEOUT);
			if (idleTimeout == null || (idleTimeout = idleTimeout.trim()).isEmpty()) {
				idleTimeout = "30";
			}

			String serverDirectory = Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR);
			Path serverDirectoryPath = null;
			try {
				serverDirectoryPath = getWorkerDirectory(this.nodeCPW, serverDirectory, "n");
			} catch (IOException e) {
				classLogger.error("Failed to create temp directory for the node process under {}", serverDirectory, e);
				throw new IllegalArgumentException("Could not create directory to launch the node process");
			}

			classLogger.info("Starting node agent worker for User = {}", User.getSingleLogginName(this));
			try {
				this.nodeCPW.createNodeProcessAndClient(port, serverDirectoryPath.toString(), false, idleTimeout,
						loggerLevel, ThreadContext.getImmutableContext());
			} catch (Exception e) {
				classLogger.error("Failed to start node process for user {}", User.getSingleLogginName(this), e);
				throw new IllegalArgumentException("Unable to connect to the node execution engine");
			}
		}
	}

	/**
	 * Returns the wrapper for this user's Java worker process, which runs R over
	 * TCP and the custom reactors set to execute on the socket.
	 *
	 * @return the Java worker process wrapper
	 */
	public ClientProcessWrapper getJavaClientProcessWrapper() {
		return this.javaCPW;
	}

	/**
	 * Returns the socket client for this user's Java worker process, which runs R
	 * over TCP and the custom reactors set to execute on the socket.
	 *
	 * @param create true to start the worker, or reconnect to it, when it is not
	 *               connected
	 * @return the socket client, or null when create is false and no worker exists
	 */
	public SocketClient getJavaSocketClient(boolean create) {
		if (!create) {
			if (this.javaCPW == null) {
				return null;
			}
			return this.javaCPW.getSocketClient();
		}
		if (this.javaCPW == null) {
			this.javaCPW = new ClientProcessWrapper();
		}
		SocketClient client = this.javaCPW.getOrStartSocketClient(() -> startJavaSocketServerAndClient(-1));
		client.setUser(this);

		this.insightSerializedMap.clear();

		return client;
	}

	/**
	 * Starts this user's Java worker process and connects its socket client. The
	 * worker runs inside the user's chroot when chroot is enabled.
	 *
	 * @param port the port to connect on; negative to auto-allocate, or to attach
	 *             to JAVA_FORCE_PORT when it is set
	 */
	public void startJavaSocketServerAndClient(int port) {
		if (this.javaCPW == null) {
			this.javaCPW = new ClientProcessWrapper();
		}
		if (this.javaCPW.getSocketClient() == null || !this.javaCPW.getSocketClient().isConnected()) {
			boolean debug = false;
			if (port < 0) {
				String forcePort = Utility.getDIHelperProperty(Settings.JAVA_FORCE_PORT);
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						classLogger.warn("User {} has an invalid JAVA_FORCE_PORT value",
								User.getSingleLogginName(this));
					}
				}
			}

			String loggerLevel = Utility.getDIHelperProperty(Settings.LOGGER_LEVEL);
			if (loggerLevel == null || (loggerLevel = loggerLevel.trim()).isEmpty()) {
				loggerLevel = "WARNING";
			}

			if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
				this.symlinkHelper = getUserSymlinkHelper();
				try {
					this.javaCPW.createProcessAndClient(false, this.symlinkHelper, port, null, null, debug, "-1",
							loggerLevel, ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start chrooted Java worker for user {}",
							User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to the Java worker");
				}
			} else {
				String serverDirectory = Utility.getDIHelperProperty(Constants.INSIGHT_CACHE_DIR);
				Path serverDirectoryPath = null;
				try {
					serverDirectoryPath = getWorkerDirectory(this.javaCPW, serverDirectory, "j");
				} catch (IOException e) {
					classLogger.error("Failed to create temp directory for the Java worker under {}", serverDirectory,
							e);
					throw new IllegalArgumentException("Could not create directory to launch the Java worker");
				}

				classLogger.info("Starting Java worker for User = {}", User.getSingleLogginName(this));
				try {
					this.javaCPW.createProcessAndClient(false, null, port, null, serverDirectoryPath.toString(), debug,
							"-1", loggerLevel, ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start Java worker for user {}", User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to the Java worker");
				}
			}
		}
	}

	/**
	 * Returns the folder a worker runs in: the folder its wrapper already uses when
	 * that still exists, so a restarted worker keeps its folder, or a new folder
	 * under the insight cache.
	 *
	 * @param cpw          the worker's process wrapper
	 * @param cacheDir     the insight cache folder
	 * @param folderPrefix the prefix for a new folder's name
	 * @return the folder
	 * @throws IOException if a new folder cannot be created
	 */
	private static Path getWorkerDirectory(ClientProcessWrapper cpw, String cacheDir, String folderPrefix)
			throws IOException {
		String existing = cpw.getServerDirectory();
		if (existing != null && Files.isDirectory(Paths.get(existing))) {
			return Paths.get(existing);
		}
		return Files.createTempDirectory(Paths.get(cacheDir), folderPrefix);
	}

	/**
	 * Returns the process that runs this user's R: the Java worker when R runs over
	 * TCP, or the user's dedicated Rserve.
	 *
	 * @return the process, or null when R does not run in a process of its own for
	 *         this user
	 */
	public Process getrProcess() {
		if (RJavaTranslatorFactory.runsInJavaWorker()) {
			return this.javaCPW == null ? null : this.javaCPW.getProcess();
		}
		return this.rcon instanceof RUserConnectionDedicated dedicated ? dedicated.getProcess() : null;
	}

	/**
	 * Returns the port of the process that runs this user's R: the Java worker when
	 * R runs over TCP, or the user's dedicated Rserve.
	 *
	 * @return the port, or -1 when R does not run in a process of its own for this
	 *         user
	 */
	public int getrPort() {
		if (RJavaTranslatorFactory.runsInJavaWorker()) {
			return this.javaCPW == null || this.javaCPW.getSocketClient() == null ? -1 : this.javaCPW.getPort();
		}
		return this.rcon instanceof RUserConnectionDedicated dedicated ? dedicated.getPort() : -1;
	}

	/**
	 * Returns the port of this user's python process.
	 *
	 * @return the port, or -1 when the python process has not started
	 */
	public int getPyPort() {
		return this.pythonCPW == null || this.pythonCPW.getSocketClient() == null ? -1 : this.pythonCPW.getPort();
	}

	private void addUserMemory() {
		long memoryInGigs = 0;
		// check if the user has memory
		boolean checkMem = Boolean.parseBoolean(Utility.getDIHelperProperty(Settings.CHECK_MEM) + "");
		if (checkMem) {
			long freeMem = MgmtUtil.getFreeMemory();
			String memProfileSettings = Utility.getDIHelperProperty(Settings.MEM_PROFILE_SETTINGS);
			if (memProfileSettings.equalsIgnoreCase(Settings.CONSTANT_MEM)) {
				String memLimitSettings = Utility.getDIHelperProperty(Settings.USER_MEM_LIMIT);
				memoryInGigs = Integer.parseInt(memLimitSettings);
			}

			MgmtUtil.addMemory4User(memoryInGigs);
		}
	}

	public void removeUserMemory() {
		long memoryInGigs = 0;
		// check if the user has memory
		boolean checkMem = Boolean.parseBoolean(Utility.getDIHelperProperty(Settings.CHECK_MEM) + "");
		if (checkMem) {
			String memProfileSettings = Utility.getDIHelperProperty(Settings.MEM_PROFILE_SETTINGS);
			if (memProfileSettings.equalsIgnoreCase(Settings.CONSTANT_MEM)) {
				String memLimitSettings = Utility.getDIHelperProperty(Settings.USER_MEM_LIMIT);
				memoryInGigs = Integer.parseInt(memLimitSettings);
			}

			MgmtUtil.removeMemory4User(memoryInGigs);
		}
	}

	public String[] getUserCredential(AuthProvider prov) {
		// just need some specific one the user is using
		if (prov != null && accessTokens.containsKey(prov)) {
			String[] creds = getUserEmail(accessTokens.get(prov));
			if (creds[1] != null) {
				return creds;
			}
		}

		Iterator<AuthProvider> accessKeysItr = accessTokens.keySet().iterator();
		while (accessKeysItr.hasNext()) {
			AuthProvider provider = accessKeysItr.next();
			AccessToken tok = accessTokens.get(provider);
			String[] creds = getUserEmail(tok);
			if (creds[1] != null) {
				return creds;
			}
		}

		return new String[] { "anonymous", "anonymous@not_logged_in.com" };
	}

	public String getCachedTemporalAccessKey() {
		if (this.cachedTemporalAccessSecretKey != null) {
			return this.cachedTemporalAccessSecretKey[0];
		}
		return null;
	}

	public String[] createCachedTemporalAccessSecretKey() {
		AccessToken loginToken = this.getPrimaryLoginToken();
		if (loginToken == null) {
			throw new NullPointerException("User does not have a primary login token");
		}

		if (this.cachedTemporalAccessSecretKey != null) {
			return this.cachedTemporalAccessSecretKey;
		}

		if (this.cachedTemporalAccessSecretKey == null) {
			ReentrantLock lock = getUserLock();
			lock.lock();
			try {
				if (this.cachedTemporalAccessSecretKey == null) {
					String accessKey = UUID.randomUUID().toString();
					String secretKey = UUID.randomUUID().toString();
					this.cachedTemporalAccessSecretKey = new String[] { accessKey, secretKey };
					LocalUserStore.getInstance().store(accessKey,
							new Object[] { secretKey, loginToken.getId(), loginToken.getProvider() });
					classLogger.info("Generated temporal access/secret key for user");
				}
			} finally {
				lock.unlock();
			}
		}

		return this.cachedTemporalAccessSecretKey;
	}

	/**
	 * Records whether the insight has been sent to this user's Java worker.
	 *
	 * @param insightId the insight id
	 * @param serialize true when the worker has the insight
	 */
	public void setInsightSerialization(String insightId, Boolean serialize) {
		if (insightId == null) {
			return;
		}
		if (serialize == null) {
			insightSerializedMap.remove(insightId);
		} else {
			insightSerializedMap.put(insightId, serialize);
		}
	}

	/**
	 * Returns whether the insight has been sent to this user's Java worker.
	 *
	 * @param insightId the insight id
	 * @return true when the worker has the insight
	 */
	public Boolean getInsightSerialization(String insightId) {
		return insightId != null && Boolean.TRUE.equals(insightSerializedMap.get(insightId));
	}

	private String[] getUserEmail(AccessToken token) {
		String[] userEmail = new String[2];
		userEmail[0] = token.getUsername();
		userEmail[1] = token.getEmail();

		return userEmail;
	}

	public PlaywrightSession getPlaywrightSession(String sessionId) {
		PlaywrightSession thisSession = getPlaywrightSessionStore().get(sessionId);
		if (thisSession == null) {
			throw new IllegalArgumentException("Invalid/Expired playwright session: " + sessionId);
		}
		return thisSession;
	}

	public void setPlaywrightSession(String sessionId, PlaywrightSession playwrightSession) {
		getPlaywrightSessionStore().put(sessionId, playwrightSession);
	}

	public void removePlaywrightSession(String sessionId) {
		PlaywrightSession thisSession = getPlaywrightSessionStore().remove(sessionId);
		if (thisSession != null) {
			try {
				thisSession.close();
			} catch (Exception e) {
				classLogger.error("Error occurred closing the playwright session {}", sessionId, e);
			}
		}
	}

	public Set<String> getPlaywrightSessionIds() {
		return getPlaywrightSessionStore().keySet();
	}

	private Map<String, PlaywrightSession> getPlaywrightSessionStore() {
		if (this.playwrightSessionStore != null) {
			return this.playwrightSessionStore;
		}

		if (this.playwrightSessionStore == null) {
			ReentrantLock lock = getUserLock();
			lock.lock();
			try {
				if (this.playwrightSessionStore == null) {
					this.playwrightSessionStore = new ConcurrentHashMap<>();
				}
			} finally {
				lock.unlock();
			}
		}
		return this.playwrightSessionStore;
	}

	public BrowserContext getSharedPlaywrightContext() {
		return sharedPlaywrightContext;
	}

	public void setSharedPlaywrightContext(BrowserContext context) {
		this.sharedPlaywrightContext = context;
	}

	/**
	 * Thread-safe get-or-create to avoid multiple contexts for the same user
	 * 
	 * @param browser
	 * @param options
	 * @return
	 */
	public BrowserContext getOrCreateSharedPlaywrightContext(Browser browser, Browser.NewContextOptions options) {
		BrowserContext ctx = sharedPlaywrightContext;
		if (ctx != null) {
			return ctx;
		}

		if (ctx == null) {
			ReentrantLock lock = getUserLock();
			lock.lock();
			try {
				ctx = sharedPlaywrightContext;
				if (ctx == null) {
					ctx = browser.newContext(options);
					ctx.setDefaultTimeout(60_000);
					ctx.setDefaultNavigationTimeout(60_000);
					sharedPlaywrightContext = ctx;
				}
			} finally {
				lock.unlock();
			}
		}
		return ctx;
	}

	/**
	 * Call this on logout/reset to close context and clear storage for this user
	 */
	public void closeAndClearSharedPlaywrightContext() {
		BrowserContext ctx = sharedPlaywrightContext;
		sharedPlaywrightContext = null;
		if (ctx != null) {
			try {
				ctx.close();
			} catch (Exception ignored) {
			}
		}
	}

	/*
	 * Static utility methods
	 */

	public static Map<String, String> getLoginNames(User semossUser) {
		Map<String, String> retMap = new HashMap<>();
		if (semossUser == null) {
			return retMap;
		}
		for (AuthProvider p : semossUser.loggedInProfiles) {
			String name = semossUser.getAccessToken(p).getName();
			if (name == null) {
				// need to send something
				name = "";
			}
			retMap.put(p.toString().toUpperCase(), name);
		}

		return retMap;
	}

	public static Map<String, Map<String, Object>> getLoginDetails(User semossUser) {
		Map<String, Map<String, Object>> retMap = new HashMap<>();
		if (semossUser == null) {
			return retMap;
		}
		for (AuthProvider p : semossUser.loggedInProfiles) {
			AccessToken token = semossUser.getAccessToken(p);
			String id = token.getId();
			String name = token.getName();
			if (name == null) {
				// need to send something
				name = "";
			}

			Map<String, Object> innerMap = new HashMap<>();
			innerMap.put("id", id);
			innerMap.put("name", name);
			Map<String, String> sans = token.getSAN();
			if (sans != null && sans.size() > 0) {
				innerMap.put("san", sans);
			}
			retMap.put(p.toString(), innerMap);
		}

		return retMap;
	}

	/**
	 * Returns the id of the user's primary login, or an empty string when there is
	 * no user or no login left, as for a user who has just logged out of their last
	 * login.
	 *
	 * @param semossUser the user, or null
	 * @return the primary login's id, or an empty string
	 */
	public static String getSingleLogginName(User semossUser) {
		if (semossUser == null) {
			return "";
		}

		AuthProvider primaryLogin = semossUser.getPrimaryLogin();
		AccessToken token = primaryLogin == null ? null : semossUser.accessTokens.get(primaryLogin);
		return token == null ? "" : token.getId();
	}

	public static List<Pair<String, String>> getUserIdAndType(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		List<Pair<String, String>> creds = new ArrayList<>();
		if (user.getLogins() != null) {
			for (AuthProvider login : user.getLogins()) {
				String userid = user.getAccessToken(login).getId();
				Pair<String, String> added = Pair.with(userid, login.name());
				creds.add(added);
			}
		}

		if (creds.size() == 0) {
			throw new IllegalArgumentException("User needs to be logged in.");
		}

		return creds;
	}

	public static Pair<String, String> getPrimaryUserIdAndTypePair(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		AuthProvider login = user.getPrimaryLogin();
		if (login == null) {
			throw new IllegalArgumentException("User must have primary login");
		}
		String userid = user.getAccessToken(login).getId();
		return Pair.with(userid, login.getLabel());
	}

}
