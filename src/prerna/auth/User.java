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
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.javatuples.Pair;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RRemoteRserve;
import prerna.om.ClientProcessWrapper;
import prerna.om.CopyObject;
import prerna.om.LocalUserStore;
import prerna.project.api.IProject;
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
	private Map<String, Object> roomHash = new ConcurrentHashMap<>();

	// store the users insights
	private transient Map<String, List<String>> openInsights = null;

	// need to have an access token store
	private transient IRUserConnection rcon;
	private transient RRemoteRserve rconRemote;

	// python related stuff
	private transient ClientProcessWrapper pythonCPW = new ClientProcessWrapper();
	private transient Process pyProcess = null;

	// r
	private transient ClientProcessWrapper rCPW = new ClientProcessWrapper();
	private transient Process rProcess = null;

	private String chrootPath = null;
	private transient volatile SymlinkHelper symlinkHelper = null;

	// playwright
	private transient volatile Map<String, PlaywrightSession> playwrightSession = null;
	private transient volatile BrowserContext sharedPlaywrightContext;

	private Map<AuthProvider, String> assetProjectMap = new HashMap<>();
	private AuthProvider primaryLogin;

	private transient Object assetSyncObject = null;
	private transient Object workspaceSyncObject = null;

	public transient CopyObject cp = null;

	private int rPort = -1;
	private int pyPort = -1;

	// need to move everything here
	// since on reconnect we need to redo serialization.
	private Map<String, Boolean> insightSerializedMap = new HashMap<String, Boolean>();

	// this is a unique identifier for this user instance
	private String userEpoch = null;

	private boolean anonymous;
	private String anonymousId;

	private transient volatile String[] cachedTemporalAccessSecretKey = null;

	public User() {
		// transient objects should be defined in the constructor
		// since if this is serialized we dont want these values to be null
		this.openInsights = new HashMap<>();
		this.assetSyncObject = new Object();
		this.workspaceSyncObject = new Object();
		// set it in the mgmt utils
		addUserMemory();
		this.userEpoch = UUID.randomUUID().toString();
		this.playwrightSession = new ConcurrentHashMap<>();
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
		setAnonymous(false);
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

	public void setAnonymous(boolean anonymous) {
		this.anonymous = anonymous;
	}

	public boolean isAnonymous() {
		return this.anonymous;
	}

	public void setAnonymousId(String anonymousId) {
		this.anonymousId = anonymousId;
	}

	public String getAnonymousId() {
		return this.anonymousId;
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
		if (this.assetProjectMap.get(token) != null) {
			return this.assetProjectMap.get(token);
		}
		String projectId = UserAssetUtils.getUserAssetProject(this, token);

		if (projectId != null) {
			this.assetProjectMap.put(token, projectId);
		} else {
			try {
				synchronized (assetSyncObject) {
					projectId = UserAssetUtils.getUserAssetProject(this, token);
					if (projectId == null) {
						projectId = UserAssetUtils.createUserAssetProject(this, token);
					}
				}
			} catch (Exception e) {
				classLogger.error("Failed to load or create user asset project for token {}", token, e);
			}

			this.assetProjectMap.put(token, projectId);
		}

		// TODO actually sync the pull, not sure pull it
		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pullUserAsset(projectId, false);
		}

		return this.assetProjectMap.get(token);
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

	public void ctrlC(String source, String showSource) {
		this.cp = new CopyObject();
		cp.source = source;
		cp.showSource = showSource;
	}

	public CopyObject getCtrlC() {
		return cp;
	}

	public void ctrlX(String source, String showSource) {
		this.cp = new CopyObject();
		cp.source = source;
		cp.showSource = showSource;
		cp.delete = true;
	}

	public void escapeCopy() {
		this.cp = null;
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
	public Map<String, Object> getRoomHash() {
		return roomHash;
	}

	/////////////////////////////////////////////////////

	/*
	 * Static utility methods
	 */

	public static Map<String, String> getLoginNames(User semossUser) {
		Map<String, String> retMap = new HashMap<>();
		if (semossUser == null) {
			return retMap;
		}
		if (semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled()
				&& semossUser.isAnonymous()) {
			retMap.put("ANONYMOUS", "Sign In");
		} else {
			for (AuthProvider p : semossUser.loggedInProfiles) {
				String name = semossUser.getAccessToken(p).getName();
				if (name == null) {
					// need to send something
					name = "";
				}
				retMap.put(p.toString().toUpperCase(), name);
			}
		}

		return retMap;
	}

	public static Map<String, Map<String, Object>> getLoginDetails(User semossUser) {
		Map<String, Map<String, Object>> retMap = new HashMap<>();
		if (semossUser == null) {
			return retMap;
		}
		if (semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled()
				&& semossUser.isAnonymous()) {
			Map<String, Object> innerMap = new HashMap<>();
			innerMap.put("id", semossUser.getAnonymousId());
			innerMap.put("name", "Sign In");
			retMap.put("ANONYMOUS", innerMap);
		} else {
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
		}

		return retMap;
	}

	public static String getSingleLogginName(User semossUser) {
		if (semossUser == null) {
			return "";
		}

		if (semossUser.loggedInProfiles.isEmpty() && AbstractSecurityUtils.anonymousUsersEnabled()
				&& semossUser.isAnonymous()) {
			return "ANONYMOUS " + semossUser.anonymousId;
		}

		AccessToken token = semossUser.accessTokens.get(semossUser.getPrimaryLogin());
		return token.getId();
	}

	public static List<Pair<String, String>> getUserIdAndType(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		if (user.isAnonymous()) {
			throw new IllegalArgumentException("User cannot be anonymous.");
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

		if (user.isAnonymous()) {
			throw new IllegalArgumentException("User cannot be anonymous.");
		}

		AuthProvider login = user.getPrimaryLogin();
		if (login == null) {
			throw new IllegalArgumentException("User must have primary login");
		}
		String userid = user.getAccessToken(login).getId();
		return Pair.with(userid, login.getLabel());
	}

	@Deprecated
	public static List<Pair<String, String>> getPrimaryUserIdAndType(User user) {
		if (user == null) {
			throw new IllegalArgumentException("User cannot be null.");
		}

		if (user.isAnonymous()) {
			throw new IllegalArgumentException("User cannot be anonymous.");
		}

		List<Pair<String, String>> creds = new ArrayList<>();

		AuthProvider login = user.getPrimaryLogin();
		if (login == null) {
			throw new IllegalArgumentException("User must have primary login");
		}
		String userid = user.getAccessToken(login).getId();
		creds.add(Pair.with(userid, login.getLabel()));

		if (creds.size() == 0) {
			throw new IllegalArgumentException("User needs to be logged in.");
		}

		return creds;
	}

	/////////////////////////////////////////////////////

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
		if (this.pythonCPW == null || this.pythonCPW.getSocketClient() == null) {
			startPythonSocketServerAndClient(-1, venvEngineId);
			this.pythonCPW.getSocketClient().setUser(this);
		} else if (!this.pythonCPW.getSocketClient().isConnected()) {
			this.pythonCPW.shutdown(false);
			try {
				this.pythonCPW.reconnect();
			} catch (Exception e) {
				classLogger.error("Failed to reconnect to user python process", e);
				throw new IllegalArgumentException("Failed to connect to your isolated analytics engine");
			}
		}

		// invalidate the serialization map
		this.insightSerializedMap.clear();

		return this.pythonCPW.getSocketClient();
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

			synchronized (this) {
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
			}
			return symlinkHelper;
		}

		throw new IllegalArgumentException("Chroot is not enabled on this instance");
	}

	/**
	 * 
	 * @param port
	 * @param venvEngineId
	 */
	public void startPythonSocketServerAndClient(int port, String venvEngineId) {
		if (this.pythonCPW == null) {
			this.pythonCPW = new ClientProcessWrapper();
		}
		if (this.pythonCPW.getSocketClient() == null || !this.pythonCPW.getSocketClient().isConnected()) {
			boolean nativePyServer = false;
			// defined in rdf map
			String nativePyServerStr = Utility.getDIHelperProperty(Settings.NATIVE_PY_SERVER);
			if (nativePyServerStr != null && !(nativePyServerStr = nativePyServerStr.trim()).isEmpty()) {
				nativePyServer = Boolean.parseBoolean(nativePyServerStr);
			}

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

			String customClassPath = Utility.getDIHelperProperty("TCP_WORKER_CP");
			if (customClassPath == null) {
				classLogger.info("No custom class path set");
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
					// TODO update once venv with chroot is enabled
					this.pythonCPW.createProcessAndClient(nativePyServer, this.symlinkHelper, port, null, null,
							customClassPath, debug, "-1", loggerLevel, ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start chrooted python process for user {}",
							User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to user server");
				}
			} else {
				try {
					serverDirectoryPath = Files.createTempDirectory(Paths.get(serverDirectory), "a");
				} catch (IOException e) {
					classLogger.error("Failed to create temp directory for non-chroot python process under {}",
							serverDirectory, e);
					throw new IllegalArgumentException("Could not create directory to launch project process");
				}

				classLogger.info("Starting Non-chroot TCP Server for User = {}", User.getSingleLogginName(this));
				try {
					String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable()
							: null;
					this.pythonCPW.createProcessAndClient(nativePyServer, null, port, venvPath,
							serverDirectoryPath.toString(), customClassPath, debug, "-1", loggerLevel,
							ThreadContext.getImmutableContext());
				} catch (Exception e) {
					classLogger.error("Failed to start non-chroot python process for user {}",
							User.getSingleLogginName(this), e);
					throw new IllegalArgumentException("Unable to connect to user server");
				}
			}
		}
	}

	public Process getrProcess() {
		return rProcess;
	}

	public void setrProcess(Process rProcess) {
		this.rProcess = rProcess;
	}

	public Process getPyProcess() {
		return pyProcess;
	}

	public void setPyProcess(Process pyProcess) {
		this.pyProcess = pyProcess;
	}

	public int getrPort() {
		return rPort;
	}

	public void setrPort(int rPport) {
		this.rPort = rPport;
	}

	public int getPyPort() {
		return pyPort;
	}

	public void setPyPort(int pyPport) {
		this.pyPort = pyPport;
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
			long freeMem = MgmtUtil.getFreeMemory();
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
			synchronized (this) {
				if (this.cachedTemporalAccessSecretKey == null) {
					String accessKey = UUID.randomUUID().toString();
					String secretKey = UUID.randomUUID().toString();
					this.cachedTemporalAccessSecretKey = new String[] { accessKey, secretKey };
					LocalUserStore.getInstance().store(accessKey,
							new Object[] { secretKey, loginToken.getId(), loginToken.getProvider() });
					classLogger.info("Generated temporal access/secret key for user");
				}
			}
		}

		return this.cachedTemporalAccessSecretKey;
	}

	public void setInsightSerialization(String insightId, Boolean serialize) {
		insightSerializedMap.put(insightId, serialize);
	}

	public Boolean getInsightSerialization(String insightId) {
		return insightSerializedMap.containsKey(insightId) && insightSerializedMap.get(insightId);
	}

	private String[] getUserEmail(AccessToken token) {
		String[] userEmail = new String[2];
		userEmail[0] = token.getUsername();
		userEmail[1] = token.getEmail();

		return userEmail;
	}

	public PlaywrightSession getPlaywrightSession(String id) {
		PlaywrightSession session = getPlaywrightSessionStore().get(id);
		if (session == null) {
			throw new IllegalArgumentException("Invalid/Expired playwright session: " + id);
		}
		return session;
	}

	public void setPlaywrightSession(String id, PlaywrightSession s) {
		getPlaywrightSessionStore().put(id, s);
	}

	public void removePlaywrightSession(String id) {
		getPlaywrightSessionStore().remove(id);
	}

	private Map<String, PlaywrightSession> getPlaywrightSessionStore() {
		if (this.playwrightSession != null) {
			return this.playwrightSession;
		}

		if (this.playwrightSession == null) {
			synchronized (this) {
				if (this.playwrightSession == null) {
					this.playwrightSession = new ConcurrentHashMap<>();
				}
			}
		}
		return this.playwrightSession;
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
			synchronized (this) {
				ctx = sharedPlaywrightContext;
				if (ctx == null) {
					ctx = browser.newContext(options);
					ctx.setDefaultTimeout(60_000);
					ctx.setDefaultNavigationTimeout(60_000);
					sharedPlaywrightContext = ctx;
				}
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

}
